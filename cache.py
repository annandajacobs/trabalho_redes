import time
import threading

class SimpleMemcached:
    def __init__(self):
        self.store = {}  # key -> (value, flags, expire_at, cas_token)
        self.cas_versions = {}  # key -> int
        self.lock = threading.Lock()

    def _is_expired(self, meta):
        _, _, expire, _ = meta
        return expire != 0 and time.time() > expire

    def _get_valid(self, key):
        meta = self.store.get(key)
        if meta and not self._is_expired(meta):
            return meta
        self.store.pop(key, None)
        self.cas_versions.pop(key, None)
        return None

    def _next_cas_token(self, key):
        self.cas_versions[key] = self.cas_versions.get(key, 0) + 1
        return self.cas_versions[key]

    def set(self, key, value, flags=0, exptime=0):
        with self.lock:
            expire_at = time.time() + exptime if exptime else 0
            cas_token = self._next_cas_token(key)
            self.store[key] = (value, flags, expire_at, cas_token)
            return "STORED"

    def get(self, key):
        with self.lock:
            meta = self._get_valid(key)
            return meta[0] if meta else None

    def gets(self, key):
        with self.lock:
            meta = self._get_valid(key)
            if meta:
                value, _, _, cas_token = meta
                return (value, cas_token)
            return None

    def get_multi(self, keys):
        with self.lock:
            return {k: self._get_valid(k)[0] for k in keys if self._get_valid(k)}

    def add(self, key, value, flags=0, exptime=0):
        with self.lock:
            if self._get_valid(key):
                return "NOT_STORED"
            return self.set(key, value, flags, exptime)

    def replace(self, key, value, flags=0, exptime=0):
        with self.lock:
            if not self._get_valid(key):
                return "NOT_STORED"
            return self.set(key, value, flags, exptime)

    def delete(self, key):
        with self.lock:
            if key in self.store and self._get_valid(key):
                del self.store[key]
                self.cas_versions.pop(key, None)
                return "DELETED"
            return "NOT_FOUND"

    def incr(self, key, value):
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            val = meta[0]
            try:
                new_val = str(int(val) + int(value))
                return self.set(key, new_val, meta[1], meta[2] - time.time())
            except ValueError:
                return "CLIENT_ERROR cannot increment"

    def decr(self, key, value):
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            val = meta[0]
            try:
                new_val = max(0, int(val) - int(value))
                return self.set(key, str(new_val), meta[1], meta[2] - time.time())
            except ValueError:
                return "CLIENT_ERROR cannot decrement"

    def cas(self, key, value, cas_token, flags=0, exptime=0):
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            _, _, _, current_token = meta
            if current_token != cas_token:
                return "EXISTS"
            return self.set(key, value, flags, exptime)

    def flush_all(self):
        with self.lock:
            self.store.clear()
            self.cas_versions.clear()
            return "OK"
