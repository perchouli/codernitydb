#!/usr/bin/env python

from CodernityDB.database import Database
from CodernityDB.tree_index import TreeBasedIndex


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
    db = Database('/tmp/tut_update')
    db.create()
    x_ind = WithXIndex(db.path, 'x')
    db.add_index(x_ind)

    # full examples so we had to add first the data
    # the same code as in previous step

    for x in xrange(100):
        db.insert(dict(x=x))

    for y in xrange(100):
        db.insert(dict(y=y))

    # end of insert part

    print db.count(db.all, 'x')

    for curr in db.all('x', with_doc=True):
        doc = curr['doc']
        if curr['key'] % 7 == 0:
            db.delete(doc)
        elif curr['key'] % 5 == 0:
            doc['updated'] = True
            db.update(doc)

    print db.count(db.all, 'x')

    for curr in db.all('x', with_doc=True):
        print curr

if __name__ == '__main__':
    main()
