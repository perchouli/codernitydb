# md5_index
# Md5Index

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
class Md5Index(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['hash_lim'] = 4 * 1024
        super(Md5Index, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return md5(data['name']).hexdigest(), {}

    def make_key(self, key):
        return md5(key).hexdigest()
