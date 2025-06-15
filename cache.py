import time
import threading

class SimpleMemcached:
    def __init__(self):
        '''
        - store: Cada valor associado a uma chave é uma tupla
        - cas_versions: Guarda a última versão (cas_token) de cada chave
        - RLock: Garante que múltiplas threads não acessem/modifiquem os dados ao mesmo tempo
        '''
        self.store = {}  # key -> (value (bytes), flags, expire_at, cas_token)
        self.cas_versions = {}  # key -> int
        self.lock = threading.RLock()

    def _is_expired(self, meta): 
        '''
        Verifica se o valor armazenado já expirou, com base no tempo atual
        '''
        _, _, expire, _ = meta
        return expire != 0 and time.time() > expire

    def _get_valid(self, key): 
        '''
        Recupera um valor não expirado do cache; Se expirou, remove a chave do cache e da versão CAS.
        '''
        meta = self.store.get(key)
        if meta and not self._is_expired(meta):
            return meta
        self.store.pop(key, None)
        self.cas_versions.pop(key, None)
        return None

    def _next_cas_token(self, key): 
        '''
        Gera e retorna o próximo token CAS (um número inteiro crescente) para controle de versões.
        '''
        self.cas_versions[key] = self.cas_versions.get(key, 0) + 1
        return self.cas_versions[key]

    def set(self, key, value, flags=0, exptime=0):
        '''
        Armazena um par chave-valor. Se a chave já existe, ela é sobrescrita.
        '''
        with self.lock:
            if isinstance(value, str):
                value = value.encode()
            expire_at = time.time() + exptime if exptime > 0 else 0
            cas_token = self._next_cas_token(key)
            self.store[key] = (value, flags, expire_at, cas_token)
            return "STORED"

    def get(self, key):
        '''
        Retorna apenas o valor associado a uma chave.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if meta:
                value = meta[0]
                return value.decode() if isinstance(value, bytes) else str(value)
            return None

    def gets(self, key):
        '''
        Retorna o valor e o token CAS (Check-And-Set). Este token é um número de versão do dado.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if meta:
                value, _, _, cas_token = meta
                return (value.decode(), cas_token) if isinstance(value, bytes) else (str(value), cas_token)
            return None

    def get_multi(self, keys):
        '''
        - Recebe uma lista de chaves.
        - Retorna um dicionário com os pares chave/valor válidos e não expirados.
        '''
        with self.lock:
            return {k: self._get_valid(k)[0].decode() for k in keys if self._get_valid(k)}

    def add(self, key, value, flags=0, exptime=0):
        '''
        Só armazena o valor se a chave não existir. Ideal para criar um novo registro sem risco de sobrescrever algo.
        '''
        with self.lock:
            if self._get_valid(key):
                return "NOT_STORED"
            if isinstance(value, str):
                value = value.encode()
            expire_at = time.time() + exptime if exptime else 0
            cas_token = self._next_cas_token(key)
            self.store[key] = (value, flags, expire_at, cas_token)
            return "STORED"

    def replace(self, key, value, flags=0, exptime=0):
        '''
        Substitui o valor de uma chave somente se ela já existir.
        '''
        with self.lock:
            if not self._get_valid(key):
                return "NOT_STORED"
            return self.set(key, value, flags, exptime)

    def append(self, key, value):
        '''
        Adiciona dados ao final do valor já armazenado.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_STORED"
            old_value, flags, expire, cas_id = meta
            if isinstance(value, str):
                value = value.encode()
            new_value = old_value + value
            self.store[key] = (new_value, flags, expire, cas_id)
            return "STORED"

    def prepend(self, key, value):
        '''
        Adiciona dados no início do valor armazenado.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_STORED"
            old_value, flags, expire, cas_id = meta
            if isinstance(value, str):
                value = value.encode()
            new_value = value + old_value
            self.store[key] = (new_value, flags, expire, cas_id)
            return "STORED"

    def delete(self, key):
        '''
        Remove a chave do cache, se existir e estiver válida.
        '''
        with self.lock:
            if key in self.store and self._get_valid(key):
                del self.store[key]
                self.cas_versions.pop(key, None)
                return "DELETED"
            return "NOT_FOUND"

    def incr(self, key, value):
        '''
        Incrementa um valor numérico armazenado na chave.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            val = meta[0]
            try:
                new_val = str(int(val.decode()) + int(value))
                return self.set(key, new_val, meta[1], meta[2] - time.time())
            except ValueError:
                return "CLIENT_ERROR cannot increment"

    def decr(self, key, value):
        '''
        Decrementa um valor numérico armazenado na chave.
        '''
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            val = meta[0]
            try:
                new_val = max(0, int(val.decode()) - int(value))
                return self.set(key, str(new_val), meta[1], meta[2] - time.time())
            except ValueError:
                return "CLIENT_ERROR cannot decrement"

    def cas(self, key, value, cas_token, flags=0, exptime=0):
        '''
        - Executa um Compare-And-Set: só altera o valor se o cas_token enviado for igual ao atual.
        - Permite múltiplos clientes tentarem modificar dados com segurança sem sobrescrever alterações uns dos outros (controle de concorrência).
        '''
        with self.lock:
            meta = self._get_valid(key)
            if not meta:
                return "NOT_FOUND"
            _, _, _, current_token = meta
            if current_token != cas_token:
                return "EXISTS"
            return self.set(key, value, flags, exptime)

    def flush_all(self):
        '''
        Esvazia totalmente o cache
        '''
        with self.lock:
            self.store.clear()
            self.cas_versions.clear()
            return "OK"
