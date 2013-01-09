#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011-2013 Codernity (http://codernity.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from CodernityDB.database_thread_safe import ThreadSafeDatabase
from shared import DB_Tests, WithAIndex
from hash_tests import HashIndexTests
from tree_tests import TreeIndexTests

from threading import Thread
import os
import time
import random
import pytest


class Test_Database(DB_Tests):

    _db = ThreadSafeDatabase


class Test_HashIndex(HashIndexTests):

    _db = ThreadSafeDatabase


class Test_TreeIndex(TreeIndexTests):

    _db = ThreadSafeDatabase


class Test_Threads(object):

    _db = ThreadSafeDatabase

    def test_one(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(WithAIndex(db.path, 'with_a'))
        ths = []
        for x in xrange(100):
            ths.append(Thread(target=db.insert, args=(dict(a=x),)))
        for th in ths:
            th.start()
        for th in ths:
            th.join()
        assert db.count(db.all, 'with_a') == 100
        l = range(100)
        for curr in db.all('with_a', with_doc=True):
            print curr
            a = curr['doc']['a']
            l.remove(a)
        assert l == []

    @pytest.mark.parametrize(('threads_num', ), [(x, ) for x in (3, 10, 20, 50, 100, 250)])
    def test_conc_update(self, tmpdir, threads_num):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(WithAIndex(db.path, 'with_a'))
        db.insert(dict(a=1))

        def updater():
            i = 0
            time.sleep(random.random() / 100)
            while True:
                rec = list(db.all('id', limit=1))
                doc = rec[0].copy()
                doc['a'] += 1
                try:
                    db.update(doc)
#                    pass
                except:
                    i += 1
                    if i > 100:
                        return False
                    time.sleep(random.random() / 100)
                else:
                    return True
        ths = []
        for x in xrange(threads_num):  # python threads... beware!!!
            ths.append(Thread(target=updater))
        for th in ths:
            th.start()
        for th in ths:
            th.join()

        assert db.count(db.all, 'with_a', with_doc=True) == 1
        assert db.count(db.all, 'id') == 1
