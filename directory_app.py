"""Ephemeral WSGI lobby registry. Run one worker; rooms are not game servers.

Local smoke server: python -m server.directory_app --bind 127.0.0.1 --port 17955
Production: gunicorn --workers 1 --threads 4 --bind 0.0.0.0:$PORT server.directory_app:application
"""
import argparse
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import threading
import time
from http import HTTPStatus
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server


TTL_SECONDS = 45
MAX_BODY = 4096
ROOM_PATH = re.compile(r"/v1/rooms/([0-9a-f]{32})\Z")
IDEMPOTENCY_KEY = re.compile(r"[0-9a-f]{32}\Z")
FIELDS = {"name", "port", "players", "max_players", "has_password", "phase"}
PHASES = {"waiting", "seating", "presentation", "voting", "hands_result", "transition", "podium"}


class ApiError(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message


def _no_constants(_value):
    raise ValueError("Non-finite JSON constant")


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


class Application:
    def __init__(self, clock=time.monotonic, max_rooms=1000, max_rate_keys=4096, trust_proxy=None, proxy_mode=None):
        self.clock = clock
        self.max_rooms = max_rooms
        self.max_rate_keys = max_rate_keys
        self.trust_proxy = os.environ.get("DIRECTORY_TRUST_PROXY") == "1" if trust_proxy is None else trust_proxy
        self.proxy_mode = os.environ.get("DIRECTORY_PROXY_MODE", "append") if proxy_mode is None else proxy_mode
        if self.proxy_mode not in {"append", "render"}:
            raise ValueError("Unsupported proxy mode")
        self.rooms = {}
        self.idempotency = {}
        self.rate_windows = {}
        self.lock = threading.RLock()

    def __call__(self, environ, start_response):
        try:
            status, data = self.handle(environ)
        except ApiError as error:
            status, data = error.status, {"error": error.message}
        except Exception:
            # Do not reflect request bodies, credentials, or raw exception text.
            status, data = 500, {"error": "Error interno del directorio."}
        body = json.dumps(data, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        headers = [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(body))),
                   ("Cache-Control", "no-store"), ("X-Content-Type-Options", "nosniff")]
        if status == 429:
            headers.append(("Retry-After", "60"))
        start_response("%d %s" % (status, HTTPStatus(status).phrase), headers)
        return [body]

    def _address(self, environ):
        raw = environ.get("REMOTE_ADDR", "")
        if self.trust_proxy and self.proxy_mode == "render":
            # Render's public ingress passes through Cloudflare, which sets this
            # single address. XFF can retain caller-supplied prefixes and internal
            # hops, so do not use it for this provider. Missing header fails closed.
            raw = environ.get("HTTP_CF_CONNECTING_IP", "").strip()
        elif self.trust_proxy and environ.get("HTTP_X_FORWARDED_FOR"):
            raw = environ["HTTP_X_FORWARDED_FOR"].split(",")[-1].strip()
        try:
            if "%" in raw:
                raise ValueError("Scoped address")
            return str(ipaddress.ip_address(raw))
        except ValueError:
            raise ApiError(400, "Dirección de conexión inválida.") from None

    def _prune(self, now):
        for room_id in [key for key, room in self.rooms.items() if room["expires"] <= now]:
            del self.rooms[room_id]
        for key in [key for key, room_id in self.idempotency.items() if room_id not in self.rooms]:
            del self.idempotency[key]
        for key in [key for key, window in self.rate_windows.items() if now - window[0] >= 60]:
            del self.rate_windows[key]

    def _rate(self, address, category, now):
        key = address, category
        limit = {"create": 10, "read": 600, "owner": 180}[category]
        if key not in self.rate_windows:
            if len(self.rate_windows) >= self.max_rate_keys:
                raise ApiError(429, "Directorio ocupado; vuelve a intentarlo más tarde.")
            self.rate_windows[key] = [now, 0]
        window = self.rate_windows[key]
        if window[1] >= limit:
            raise ApiError(429, "Demasiadas solicitudes; espera un minuto.")
        window[1] += 1

    def _metadata(self, environ):
        if environ.get("HTTP_TRANSFER_ENCODING"):
            raise ApiError(400, "Transferencia no compatible.")
        length_text = environ.get("CONTENT_LENGTH", "")
        if not length_text or not length_text.isascii() or not length_text.isdecimal():
            raise ApiError(411, "Falta longitud válida del cuerpo.")
        if len(length_text) > 6 or int(length_text) > MAX_BODY:
            raise ApiError(413, "Solicitud demasiado grande.")
        if environ.get("CONTENT_TYPE", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ApiError(415, "Usa contenido JSON.")
        size = int(length_text)
        raw = environ["wsgi.input"].read(size)
        if len(raw) != size:
            raise ApiError(400, "Solicitud incompleta.")
        try:
            data = json.loads(raw.decode("utf-8"), parse_constant=_no_constants, object_pairs_hook=_object)
        except (ValueError, UnicodeError, RecursionError):
            raise ApiError(400, "JSON inválido.") from None
        if not isinstance(data, dict) or not FIELDS.issubset(data) or set(data) - FIELDS - {"allow_late_join"}:
            raise ApiError(400, "Metadatos de sala inválidos.")
        name = data["name"]
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 60 or any(ord(c) < 32 for c in name):
            raise ApiError(400, "Nombre de sala inválido.")
        if type(data["port"]) is not int or not 1 <= data["port"] <= 65535:
            raise ApiError(400, "Puerto inválido.")
        if type(data["max_players"]) is not int or data["max_players"] != 20:
            raise ApiError(400, "Capacidad inválida.")
        if type(data["players"]) is not int or not 1 <= data["players"] <= 20:
            raise ApiError(400, "Número de jugadores inválido.")
        if type(data["has_password"]) is not bool or not isinstance(data["phase"], str) or data["phase"] not in PHASES:
            raise ApiError(400, "Estado de sala inválido.")
        allow_late_join = data.get("allow_late_join", False)
        if type(allow_late_join) is not bool:
            raise ApiError(400, "Opción de incorporaciones inválida.")
        return dict(data, name=name.strip(), allow_late_join=allow_late_join)

    def handle(self, environ):
        method = environ.get("REQUEST_METHOD", "")
        path = environ.get("PATH_INFO", "")
        if path == "/health":
            if method != "GET":
                raise ApiError(405, "Método no permitido.")
            return 200, {"ok": True, "service": "unprepared-lobby-directory", "version": 1}
        match = ROOM_PATH.fullmatch(path)
        if path != "/v1/rooms" and not match:
            raise ApiError(404, "Ruta no encontrada.")
        if (path == "/v1/rooms" and method not in {"GET", "POST"}) or (match and method not in {"PATCH", "DELETE"}):
            raise ApiError(405, "Método no permitido.")
        address = self._address(environ)
        with self.lock:
            now = self.clock()
            self._prune(now)
            self._rate(address, "read" if method == "GET" else "create" if method == "POST" else "owner", now)
        # Read bodies outside the registry lock so a slow upload cannot block listing.
        metadata = self._metadata(environ) if method in {"POST", "PATCH"} else None
        key = environ.get("HTTP_IDEMPOTENCY_KEY", "")
        if key and not IDEMPOTENCY_KEY.fullmatch(key):
            raise ApiError(400, "Clave de registro inválida.")
        with self.lock:
            now = self.clock()
            self._prune(now)
            if method == "GET":
                rows = [dict(room["metadata"], id=room_id, address=room["address"]) for room_id, room in self.rooms.items()]
                return 200, {"rooms": rows}
            if method == "POST":
                identity = hashlib.sha256((address + ":" + key).encode()).hexdigest() if key else ""
                room_id = self.idempotency.get(identity)
                if room_id not in self.rooms:
                    if len(self.rooms) >= self.max_rooms:
                        raise ApiError(503, "El directorio está completo; vuelve a intentarlo más tarde.")
                    room_id = secrets.token_hex(16)
                    self.rooms[room_id] = {"token": secrets.token_urlsafe(32)}
                    if identity:
                        self.idempotency[identity] = room_id
                room = self.rooms[room_id]
                room.update(metadata=metadata, address=address, expires=now + TTL_SECONDS)
                return 201, {"room_id": room_id, "owner_token": room["token"], "ttl_seconds": TTL_SECONDS}
            room_id = match.group(1)
            room = self.rooms.get(room_id)
            if room is None:
                raise ApiError(404, "La sala ya no está registrada.")
            header = environ.get("HTTP_AUTHORIZATION", "")
            if not header.startswith("Bearer ") or not hmac.compare_digest(header[7:].encode("utf-8"), room["token"].encode("ascii")):
                raise ApiError(403, "No autorizado para modificar esta sala.")
            if method == "PATCH":
                room.update(metadata=metadata, address=address, expires=now + TTL_SECONDS)
            else:
                del self.rooms[room_id]
                for identity in [key for key, value in self.idempotency.items() if value == room_id]:
                    del self.idempotency[identity]
            return 200, {"ok": True}


class _ThreadedServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _QuietHandler(WSGIRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(8)

    def log_message(self, _format, *args):
        # Credentials and untrusted request targets are never written to access logs.
        pass


def make_local_server(bind, port, app):
    """Development/test adapter only. Use Gunicorn behind HTTPS for deployment."""
    return make_server(bind, port, app, server_class=_ThreadedServer, handler_class=_QuietHandler)


application = Application()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=17955)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    server = make_local_server(args.bind, args.port, application)
    print("Lobby directory listening on %s:%d (development adapter)" % (args.bind, args.port), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__": main()
