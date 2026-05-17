#!/usr/bin/env python3
"""Simple HTTP server for smoke testing praxis-prefill-proxy.
Responds 200 to any /prefill/* request and echoes the path for /decode/* requests.
Run: python3 test_server.py
"""
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.do_any()

    def do_POST(self):
        self.do_any()

    def do_any(self):
        path = self.path
        if path.startswith("/prefill"):
            body = b"prefill ok"
            self.send_response(200)
        elif path.startswith("/decode"):
            body = path.encode()
            self.send_response(200)
        else:
            body = b"unknown path"
            self.send_response(404)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"backend: {fmt % args}")


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", 8081), Handler)
    print("Backend listening on 127.0.0.1:8081")
    server.serve_forever()
