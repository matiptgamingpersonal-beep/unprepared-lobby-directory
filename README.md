# Unprepared lobby directory

Standalone, ephemeral lobby discovery service. Contains no game assets, player files, installer keys or game builds.

Run one Gunicorn worker with four threads:

    gunicorn --workers 1 --threads 4 --timeout 15 --bind 0.0.0.0:$PORT directory_app:application

Health endpoint: /health. API: /v1/rooms. Records expire after 45 seconds.

DIRECTORY_TRUST_PROXY=1 is only for deployment behind the trusted Render proxy. This service lists game hosts; it does not relay game traffic or traverse NAT.
