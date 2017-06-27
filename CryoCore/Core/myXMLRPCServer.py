#!/usr/bin/env python
import socket
import select

import xmlrpc.server
import socketserver


class MyXMLRPCServer(xmlrpc.server.SimpleXMLRPCServer, socketserver.ThreadingMixIn):

    address_family = socket.AF_INET  # Force to IPv4 for now

    # Override that blasted blocking thing!
    def get_request(self):
        """
        Get the request and client address from the socket.
        Override to allow non-blocking requests.

        WARNING: This will make "serve_forever" and "handle_request"
        throw exceptions and stuff! Serve_forever thus does not work!
        """

        # Use select for non-blocking IO
        if select.select([self.socket], [], [], 1)[0]:
            return self.socket.accept()
        else:
            return None
