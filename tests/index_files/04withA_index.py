# withA_index
# WithAIndex

# inserted automatically
import os
import marshal

import struct
import shutil

from hashlib import md5

# custom db code start


# custom index code start
# source of classes in index.classes_code
# index code start
class WithAIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['hash_lim'] = 4 * 1024
        super(WithAIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val:
            if not isinstance(a_val, basestring):
                a_val = str(a_val)
            return md5(a_val).hexdigest(), {}
        return None

    def make_key(self, key):
        if not isinstance(key, basestring):
            key = str(key)
        return md5(key).hexdigest()
