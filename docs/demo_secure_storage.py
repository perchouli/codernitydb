#!/usr/bin/env python


from CodernityDB.hash_index import UniqueHashIndex
from CodernityDB.storage import Storage
from CodernityDB.database import Database
from hashlib import sha256

import salsa20
import marshal
import os


class Salsa20Storage(Storage):

    def __init__(self, db_path, name, enc_key):
        super(Salsa20Storage, self).__init__(db_path, name)
        self.enc_key = enc_key

    def data_from(self, data):
        iv = data[:8]
        sal = salsa20.Salsa20(self.enc_key, iv, 20)
        s_data = sal.decrypt(data[8:])
        m_data = marshal.loads(s_data)
        return m_data

    def data_to(self, data):
        iv = os.urandom(8)
        m_data = marshal.dumps(data)
        sal = salsa20.Salsa20(self.enc_key, iv, 20)
        s_data = sal.encrypt(m_data)
        return iv + s_data


class EncUniqueHashIndex(UniqueHashIndex):

    __enc_key = 'a' * 32

    custom_header = """
from demo_secure_storage import Salsa20Storage
from hashlib import sha256"""

    def __init__(self, *args, **kwargs):
        super(EncUniqueHashIndex, self).__init__(*args, **kwargs)

    @property
    def enc_key(self):
        return self.__enc_key

    @enc_key.setter
    def enc_key(self, value):
        if len(value) != 32:
            self.__enc_key = sha256(value).digest()
        else:
            self.__enc_key = value
        self.storage.enc_key = self.__enc_key

    def _setup_storage(self):
        if not self.storage:
            self.storage = Salsa20Storage(
                self.db_path, self.name, self.enc_key)

    def _open_storage(self):
        self._setup_storage()
        self.storage.open()

    def _create_storage(self):
        self._setup_storage()
        self.storage.create()


def main():
    db = Database('/tmp/demo_secure')
    key = 'abcdefgh'
    id_ind = EncUniqueHashIndex(db.path, 'id')
    db.set_indexes([id_ind])
    db.create()
    db.id_ind.enc_key = key
    print db.id_ind.storage

    for x in xrange(100):
        db.insert(dict(x=x, data='testing'))

    db.close()
    dbr = Database('/tmp/demo_secure')
    dbr.open()
    dbr.id_ind.enc_key = key

    for curr in dbr.all('id', limit=5):
        print curr


if __name__ == "__main__":
    main()
