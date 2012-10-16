#!/usr/bin/env python

from CodernityDB.database import Database


def main():
    db = Database('/tmp/tut1')
    db.create()

    for x in xrange(100):
        print db.insert(dict(x=x))

    for curr in db.all('id'):
        print curr

if __name__ == '__main__':
    main()
