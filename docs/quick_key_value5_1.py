#!/usr/bin/env python

from CodernityDB.database import Database
from CodernityDB.tree_index import TreeBasedIndex

import random


class WithXIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 10
        kwargs['key_format'] = 'I'
        super(WithXIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        t_val = data.get('x')
        if t_val is not None:
            return t_val, None
        return None

    def make_key(self, key):
        return key


def main():
    db = Database('/tmp/tut5_1')
    db.create()
    x_ind = WithXIndex(db.path, 'x')
    db.add_index(x_ind)

    for x in xrange(100):
        db.insert(dict(x=x, t=random.random()))

    l = []
    for curr in db.get_many('x', start=10, end=30, limit=-1, with_doc=True):
        l.append(curr['doc']['t'])
    print sum(l) / len(l)


if __name__ == '__main__':
    main()
