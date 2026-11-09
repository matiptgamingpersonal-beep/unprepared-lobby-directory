# Unprepared lobby directory

Standalone, ephemeral lobby discovery service. Contains no game assets, player files, installer keys or game builds.

Run one Gunicorn worker with four threads:

    gunicorn --workers 1 --threads 4 --timeout 15 --bind 0.0.0.0:$PORT directory_app:application

Health endpoint: /health. API: /v1/rooms. Records expire after 45 seconds.

The directory uses DIRECTORY_TRUST_PROXY=1 and DIRECTORY_PROXY_MODE=render only behind Render's trusted Cloudflare ingress.

## Optional game relay

A separate single-instance service runs relay_app.py with websockets==16.0 from relay-requirements.txt. It accepts outbound WSS on /relay and reports readiness at /health. Set RELAY_PROXY_MODE=render only behind Render ingress. Game passwords and admission stay with the authoritative game host.

The relay transports ephemeral ENet datagrams and generated or enabled game media; it never writes packet contents. TLS terminates at the hosting edge, not end-to-end between players. Connection and queue quotas close overloaded peers instead of accumulating unbounded packets. See source constants for limits.

Both services are prototypes using one in-memory instance each; restarting the relay closes its rooms. Free hosting has shared time/traffic limits. Direct UDP/LAN remains available in the game.
