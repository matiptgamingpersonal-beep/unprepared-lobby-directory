"""Bounded WebSocket transport for ENet datagrams; admission stays inside ENet.

WSS terminates at the trusted hosting edge. No packets, addresses or room codes
are logged. This is an ephemeral relay, not a lobby registry or account service.
"""
import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from http import HTTPStatus
import ipaddress
import json
import logging
import os
import re
import secrets
import signal
import struct
import time

from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Response

MAX_PACKET = 65539
MAX_HELLO = 512
HANDSHAKE_TIMEOUT = 8.0
SEND_TIMEOUT = 5.0
MAX_QUEUE_PACKETS = 64
MAX_QUEUE_BYTES = 1024 * 1024
MAX_GLOBAL_QUEUE_BYTES = 32 * 1024 * 1024
LOGGER = logging.getLogger("unprepared.relay.private")
LOGGER.disabled = True


class RelayError(Exception):
    pass


class Bucket:
    def __init__(self, rate, burst, now=None):
        self.rate, self.burst = rate, burst
        self.tokens = float(burst)
        self.updated = time.monotonic() if now is None else now

    def consume(self, amount, now=None):
        now = time.monotonic() if now is None else now
        self.tokens = min(self.burst, self.tokens + max(0, now - self.updated) * self.rate)
        self.updated = now
        if amount > self.tokens:
            return False
        self.tokens -= amount
        return True


async def _close(websocket, code, reason):
    with suppress(ConnectionClosed, OSError, asyncio.TimeoutError):
        await asyncio.wait_for(websocket.close(code=code, reason=reason), SEND_TIMEOUT + 1)


class Endpoint:
    """One FIFO writer per socket; a slow consumer is closed, never reordered."""
    def __init__(self, relay, websocket, source, queue_packets=MAX_QUEUE_PACKETS, queue_bytes=MAX_QUEUE_BYTES):
        self.relay, self.websocket, self.source = relay, websocket, source
        self.queue = asyncio.Queue(maxsize=queue_packets)
        self.max_queue_bytes = queue_bytes
        self.queued_bytes = 0
        self.writer = None
        self.close_task = None
        self.closing = False
        self.cleaned = False
        self.room = None
        self.is_host = False
        self.client_id = 0
        self.ingress = None

    def start(self):
        self.writer = asyncio.create_task(self._write())

    @staticmethod
    def _size(message):
        return len(message) if isinstance(message, bytes) else len(message.encode("utf-8"))

    def offer(self, message):
        if self.closing:
            return False
        size = self._size(message)
        if self.queue.full() or self.queued_bytes + size > self.max_queue_bytes or self.relay.queued_bytes + size > self.relay.max_queue_bytes:
            self.closing = True
            self.close_task = asyncio.create_task(_close(self.websocket, 1013, "backpressure"))
            return False
        self.queued_bytes += size
        self.relay.queued_bytes += size
        self.queue.put_nowait(message)
        return True

    def _release(self, message):
        size = self._size(message)
        self.queued_bytes -= size
        self.relay.queued_bytes -= size

    async def _write(self):
        try:
            while True:
                message = await self.queue.get()
                try:
                    await asyncio.wait_for(self.websocket.send(message), SEND_TIMEOUT)
                finally:
                    self._release(message)
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, asyncio.TimeoutError):
            self.closing = True
            await _close(self.websocket, 1013, "slow_receiver")

    async def stop(self):
        self.closing = True
        if self.writer is not None:
            self.writer.cancel()
            with suppress(asyncio.CancelledError):
                await self.writer
        while not self.queue.empty():
            self._release(self.queue.get_nowait())
        if self.close_task is not None:
            await self.close_task


@dataclass
class Room:
    code: str
    host: Endpoint
    source: str
    clients: dict = field(default_factory=dict)
    next_client: int = 1


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RelayError("invalid_hello")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise RelayError("invalid_hello")


class RelayService:
    def __init__(self, proxy_mode=None):
        self.rooms = {}
        self.connections = set()
        self.peers = {}
        self.source_connections = {}
        self.source_rooms = {}
        self.connection_rates = {}
        self.max_rooms = 100
        self.max_rooms_per_source = 5
        self.max_clients = 19
        self.max_connections = 2048
        self.max_connections_per_source = 128
        self.max_rate_sources = 4096
        self.connection_rate = 2.0
        self.connection_burst = 60
        self.client_rate = 512 * 1024
        self.client_burst = 1024 * 1024
        self.host_rate = 8 * 1024 * 1024
        self.host_burst = 16 * 1024 * 1024
        self.max_queue_bytes = MAX_GLOBAL_QUEUE_BYTES
        self.queued_bytes = 0
        self.closing = False
        self.proxy_mode = os.environ.get("RELAY_PROXY_MODE", "direct") if proxy_mode is None else proxy_mode
        if self.proxy_mode not in ("direct", "render"):
            raise ValueError("RELAY_PROXY_MODE must be direct or render")

    def _source(self, websocket):
        try:
            if self.proxy_mode == "render":
                # Enable only behind Render's trusted public ingress, never directly.
                raw = websocket.request.headers["CF-Connecting-IP"].strip()
            else:
                raw = websocket.remote_address[0]
            if "%" in raw:
                raise ValueError("Scoped address")
            return str(ipaddress.ip_address(raw))
        except (ValueError, TypeError, KeyError, IndexError):
            raise RelayError("invalid_source") from None

    def _allow_connection(self, source):
        if self.closing or len(self.connections) >= self.max_connections or self.source_connections.get(source, 0) >= self.max_connections_per_source:
            raise RelayError("connections_full")
        now = time.monotonic()
        if source not in self.connection_rates:
            for key in list(self.connection_rates):
                if now - self.connection_rates[key].updated > 300 and not self.source_connections.get(key):
                    del self.connection_rates[key]
            if len(self.connection_rates) >= self.max_rate_sources:
                raise RelayError("rate_limit")
            self.connection_rates[source] = Bucket(self.connection_rate, self.connection_burst, now)
        if not self.connection_rates[source].consume(1, now):
            raise RelayError("rate_limit")

    @staticmethod
    def _hello(message):
        if not isinstance(message, str) or len(message.encode("utf-8")) > MAX_HELLO:
            raise RelayError("invalid_hello")
        try:
            value = json.loads(message, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        except (ValueError, UnicodeError):
            raise RelayError("invalid_hello") from None
        if not isinstance(value, dict) or type(value.get("protocol")) is not int or value["protocol"] != 1:
            raise RelayError("invalid_hello")
        if value.get("role") == "host" and set(value) == {"role", "protocol"}:
            return value
        if value.get("role") == "client" and set(value) == {"role", "protocol", "room"} and isinstance(value["room"], str) and re.fullmatch("[0-9a-f]{32}", value["room"]):
            return value
        raise RelayError("invalid_hello")

    @staticmethod
    async def _reject(websocket, error):
        with suppress(ConnectionClosed, OSError, asyncio.TimeoutError):
            await asyncio.wait_for(websocket.send(json.dumps({"error": error, "protocol": 1}, separators=(",", ":"))), SEND_TIMEOUT)
        await _close(websocket, 1008, error)

    def _register(self, websocket, source, hello):
        endpoint = Endpoint(self, websocket, source)
        if hello["role"] == "host":
            if len(self.rooms) >= self.max_rooms:
                raise RelayError("rooms_full")
            if self.source_rooms.get(source, 0) >= self.max_rooms_per_source:
                raise RelayError("host_limit")
            code = secrets.token_hex(16)
            while code in self.rooms:
                code = secrets.token_hex(16)
            room = Room(code, endpoint, source)
            endpoint.is_host = True
            endpoint.ingress = Bucket(self.host_rate, self.host_burst)
            self.rooms[code] = room
            self.source_rooms[source] = self.source_rooms.get(source, 0) + 1
            reply = {"room": code, "protocol": 1}
        else:
            room = self.rooms.get(hello["room"])
            if room is None or room.host.closing:
                raise RelayError("room_missing")
            if len(room.clients) >= self.max_clients:
                raise RelayError("room_full")
            if room.next_client > 0xFFFFFFFF:
                raise RelayError("room_exhausted")
            endpoint.client_id = room.next_client
            room.next_client += 1
            endpoint.ingress = Bucket(self.client_rate, self.client_burst)
            room.clients[endpoint.client_id] = endpoint
            reply = {"client": endpoint.client_id, "protocol": 1}
        endpoint.room = room
        self.peers[websocket] = endpoint
        endpoint.start()
        endpoint.offer(json.dumps(reply, separators=(",", ":")))
        return endpoint

    async def handle(self, websocket):
        endpoint = None
        source = None
        admitted = False
        try:
            source = self._source(websocket)
            self._allow_connection(source)
            self.connections.add(websocket)
            self.source_connections[source] = self.source_connections.get(source, 0) + 1
            admitted = True
            try:
                message = await asyncio.wait_for(websocket.recv(), HANDSHAKE_TIMEOUT)
            except asyncio.TimeoutError:
                raise RelayError("hello_timeout") from None
            endpoint = self._register(websocket, source, self._hello(message))
            async for message in websocket:
                if endpoint.closing:
                    break
                if not isinstance(message, bytes) or not 5 <= len(message) <= MAX_PACKET:
                    raise RelayError("invalid_packet")
                if not endpoint.ingress.consume(len(message)):
                    raise RelayError("rate_limit")
                identifier = struct.unpack_from("<I", message)[0]
                if endpoint.is_host:
                    target = endpoint.room.clients.get(identifier)
                    if target is not None:
                        target.offer(message)
                else:
                    if identifier != endpoint.client_id:
                        raise RelayError("client_id")
                    # Rewrite the prefix from the verified identity, never trust payload routing.
                    endpoint.room.host.offer(struct.pack("<I", endpoint.client_id) + message[4:])
        except RelayError as error:
            if endpoint is None:
                await self._reject(websocket, str(error))
            else:
                await _close(websocket, 1008, str(error))
        except ConnectionClosed:
            pass
        finally:
            if endpoint is not None:
                await self._disconnect(endpoint)
            if admitted:
                self.connections.discard(websocket)
                self.source_connections[source] -= 1
                if not self.source_connections[source]:
                    del self.source_connections[source]

    async def _disconnect(self, endpoint):
        if endpoint.cleaned:
            return
        endpoint.cleaned = True
        endpoint.closing = True
        self.peers.pop(endpoint.websocket, None)
        room = endpoint.room
        if endpoint.is_host:
            self.rooms.pop(room.code, None)
            self.source_rooms[room.source] -= 1
            if not self.source_rooms[room.source]:
                del self.source_rooms[room.source]
            clients = list(room.clients.values())
            room.clients.clear()
            await endpoint.stop()
            await asyncio.gather(*(_close(client.websocket, 1001, "host_left") for client in clients))
        else:
            if room.clients.get(endpoint.client_id) is endpoint:
                del room.clients[endpoint.client_id]
                room.host.offer(json.dumps({"left": endpoint.client_id}, separators=(",", ":")))
            await endpoint.stop()

    def process_request(self, _connection, request):
        if request.path == "/health":
            body = b'{"ok":true,"service":"unprepared-relay","version":1}'
            return Response(200, "OK", Headers([("Content-Type", "application/json"), ("Content-Length", str(len(body))), ("Cache-Control", "no-store")]), body)
        if request.path != "/relay":
            body = b'{"error":"not_found"}'
            return Response(404, "Not Found", Headers([("Content-Type", "application/json"), ("Content-Length", str(len(body)))]), body)
        return None

    def serve(self, host="0.0.0.0", port=8081):
        return serve(self.handle, host, port, process_request=self.process_request, max_size=MAX_PACKET, max_queue=16,
            compression=None, write_limit=64 * 1024, ping_interval=20, ping_timeout=20,
            open_timeout=10, close_timeout=5, server_header=None, logger=LOGGER)

    async def close(self):
        self.closing = True
        await asyncio.gather(*(_close(websocket, 1001, "shutdown") for websocket in list(self.connections)))


async def main():
    relay = RelayService()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(name, stop.set)
    port = int(os.environ.get("PORT", "8081"))
    if not 1 <= port <= 65535:
        raise ValueError("PORT must be between1 and65535")
    async with relay.serve("0.0.0.0", port) as server:
        print("Unprepared relay ready", flush=True)
        await stop.wait()
        server.close()
        await server.wait_closed()
        await relay.close()


if __name__ == "__main__":
    asyncio.run(main())
