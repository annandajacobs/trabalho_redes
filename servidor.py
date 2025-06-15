import socketserver
from cache import SimpleMemcached

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

class CacheTCPHandler(socketserver.StreamRequestHandler):
    cache = SimpleMemcached()

    def handle(self):
        while True:
            line = self.rfile.readline().decode().strip()
            if not line:
                break
            parts = line.split()
            cmd = parts[0].lower()

            try:
                if cmd == "get":
                    keys = parts[1:]
                    for key in keys:
                        val = self.cache.get(key)
                        if val is not None:
                            self.wfile.write(f"VALUE {key} 0 {len(val)}\r\n{val}\r\n".encode())
                    self.wfile.write(b"END\r\n")

                elif cmd == "gets":
                    key = parts[1]
                    result = self.cache.gets(key)
                    if result:
                        val, cas_token = result
                        self.wfile.write(f"VALUE {key} 0 {len(val)} {cas_token}\r\n{val}\r\n".encode())
                    self.wfile.write(b"END\r\n")

                elif cmd in ("set", "add", "replace"):
                    key, flags, exptime, nbytes = parts[1:5]
                    noreply = "noreply" in parts
                    flags, exptime, nbytes = int(flags), int(exptime), int(nbytes)
                    data = self.rfile.read(nbytes + 2)[:-2].decode()
                    fn = getattr(self.cache, cmd)
                    resp = fn(key, data, flags, exptime)
                    if not noreply:
                        self.wfile.write((resp + "\r\n").encode())
                
                elif cmd in ("append", "prepend"):
                    if len(parts) < 5:
                        self.wfile.write(b"CLIENT_ERROR bad command line format\r\n")
                        continue
                    key, flags, exptime, bytes_len = parts[1:5]
                    bytes_len = int(bytes_len)
                    value = self.rfile.read(bytes_len + 2)[:-2]  # remove \r\n
                    if cmd == "append":
                        response = self.cache.append(key, value)
                    else:
                        response = self.cache.prepend(key, value)
                    self.wfile.write((response + "\r\n").encode())

                elif cmd == "cas":
                    key, flags, exptime, nbytes, cas_token = parts[1:6]
                    noreply = "noreply" in parts
                    flags, exptime, nbytes, cas_token = int(flags), int(exptime), int(nbytes), int(cas_token)
                    data = self.rfile.read(nbytes + 2)[:-2].decode()
                    resp = self.cache.cas(key, data, cas_token, flags, exptime)
                    if not noreply:
                        self.wfile.write((resp + "\r\n").encode())

                elif cmd == "delete":
                    key = parts[1]
                    noreply = "noreply" in parts
                    resp = self.cache.delete(key)
                    if not noreply:
                        self.wfile.write((resp + "\r\n").encode())

                elif cmd in ("incr", "decr"):
                    key, value = parts[1], parts[2]
                    fn = getattr(self.cache, cmd)
                    resp = fn(key, value)
                    self.wfile.write((resp + "\r\n").encode())

                elif cmd == "flush_all":
                    noreply = "noreply" in parts
                    resp = self.cache.flush_all()
                    if not noreply:
                        self.wfile.write((resp + "\r\n").encode())

                else:
                    self.wfile.write(b"ERROR\r\n")

            except Exception as e:
                self.wfile.write(f"SERVER_ERROR {str(e)}\r\n".encode())

if __name__ == "__main__":
    server = ThreadedTCPServer(('localhost', 11211), CacheTCPHandler)
    print("Servidor memcache rodando em :11211 com suporte a múltiplas conexões e CAS thread-safe")
    server.serve_forever()
