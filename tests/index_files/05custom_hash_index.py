# custom_hash_index
# CustomHashIndex

# inserted automatically
import os
import marshal

import struct
import shutil

from hashlib import md5

# custom db code start


# custom index code start
from CodernityDB import rr_cache
# source of classes in index.classes_code
# index code start


class CustomHashIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['entry_line_format'] = '32sIIIcI'
        kwargs['hash_lim'] = 1
        super(CustomHashIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        d = data.get('test')
        if d is None:
            return None
        if d > 5:
            k = 1
        else:
            k = 0
        return k, dict(test=d)

    def make_key(self, key):
        return key
