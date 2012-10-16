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

    def run_avg(self, db_obj, start, end):
        l = []
        gen = db_obj.get_many(
            'x', start=start, end=end, limit=-1, with_doc=True)
        for curr in gen:
            l.append(curr['doc']['t'])
        return sum(l) / len(l)


def main():
    db = Database('/tmp/tut5_2')
    db.create()
    x_ind = WithXIndex(db.path, 'x')
    db.add_index(x_ind)

    for x in xrange(100):
        db.insert(dict(x=x, t=random.random()))

    print db.run('x', 'avg', start=10, end=30)

if __name__ == '__main__':
    main()
