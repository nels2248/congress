"""
local_server.py
───────────────
Serves the docs/ folder on http://localhost:8000 so the D3 dashboard
can load data/bills.json without CORS errors.

Run from Spyder's IPython console or a terminal:
    python local_server.py
Then open http://localhost:8000 in your browser.
"""

import http.server
import socketserver
import webbrowser
import os
from pathlib import Path

PORT    = 8000
DOCROOT = Path(__file__).parent


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DOCROOT), **kwargs)

    def log_message(self, fmt, *args):
        # Quieter output — only log requests, not every poll
        if not args[0].startswith("GET /data/"):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    os.chdir(DOCROOT)
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        url = f"http://localhost:{PORT}"
        print(f"Serving {DOCROOT} at {url}  (Ctrl+C to stop)")
        webbrowser.open(url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
