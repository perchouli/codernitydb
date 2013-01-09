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

from CodernityDB.database import Database, RecordDeleted, RecordNotFound
from CodernityDB.database import DatabaseException

from CodernityDB.hash_index import HashIndex, UniqueHashIndex
from CodernityDB.index import IndexException
from CodernityDB.misc import random_hex_32

from CodernityDB import rr_cache

import pytest
import os
import random
from hashlib import md5

try:
    from collections import Counter
except ImportError:
    class Counter(dict):
        'Mapping where default values are zero'
        def __missing__(self, key):
            return 0


class CustomHashIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = 'I'
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


class Md5Index(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        kwargs['hash_lim'] = 4 * 1024
        super(Md5Index, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return md5(data['name']).digest(), {}

    def make_key(self, key):
        return md5(key).digest()


class WithAIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        kwargs['hash_lim'] = 4 * 1024
        super(WithAIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val:
            if not isinstance(a_val, basestring):
                a_val = str(a_val)
            return md5(a_val).digest(), {}
        return None

    def make_key(self, key):
        if not isinstance(key, basestring):
            key = str(key)
        return md5(key).digest()


class HashIndexTests:

    def setup_method(self, method):
        self.counter = Counter()

    def test_simple(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        # db.set_indexes([UniqueHashIndex(db.path, 'id')])
        # db.initialize()
        db.add_index(UniqueHashIndex(db.path, 'id'), False)
        db.create()

        with pytest.raises(RecordNotFound):
            db.get("id", "not_existing")
        a1 = dict(a=1)
        db.insert(a1)
        test = db.get('id', a1['_id'])
        assert test['_id'] == a1['_id']
        a_id = a1['_id']
        assert test == a1
        a1['x'] = 'x'
        db.update(a1)
        test2 = db.get('id', a_id)
        assert test2['x'] == 'x'
        assert test2 != test
        db.delete(a1)
        with pytest.raises(RecordDeleted):
            db.get('id', a_id)
        db.close()

    def test_insert_with_id(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        # db.set_indexes([UniqueHashIndex(db.path, 'id')])
        # db.initialize()
        db.add_index(UniqueHashIndex(db.path, 'id'), False)
        db.create()

        doc = dict(a=1, _id=random_hex_32())
        ins = db.insert(doc)
        assert ins['_id'] == doc["_id"]
        db.close()

    def test_delete(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        ins = []
        for x in xrange(inserts):
            doc = dict(x=x)
            db.insert(doc)
            ins.append(doc)
            self.counter['ins'] += 1

        for i in xrange(inserts / 10):
            curr = ins.pop(random.randint(0, len(ins) - 1))
            db.delete(curr)

        assert len(ins) == db.count(db.all, 'id')

        for x in xrange(inserts):
            doc = dict(x=x)
            db.insert(doc)
            ins.append(doc)
            self.counter['ins'] += 1

        for i in xrange(inserts / 10):
            curr = ins.pop(random.randint(0, len(ins) - 1))
            db.delete(curr)

        assert len(ins) == db.count(db.all, 'id')

    def test_get_after_delete(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id'),
                        CustomHashIndex(db.path, 'custom')])
        db.create()

        for x in xrange(100):
            doc = dict(test=6)
            doc.update(db.insert(doc))
            if doc['test'] > 5:
                self.counter['r'] += 1
            else:
                self.counter['l'] += 1

        counted_bef = db.count(db.all, 'custom')
        elem = db.all('custom', with_doc=True, limit=1)

        doc = next(elem)['doc']
        assert db.delete(doc) == True

        counted_aft = db.count(db.all, 'custom')
        assert counted_bef - 1 == counted_aft

        from_ind = db.get('custom', 1, with_doc=True)
        assert from_ind['doc']['_id'] != doc['_id']

        alls = db.all('custom', with_doc=True, limit=90)
        for curr in alls:
            assert db.delete(curr['doc']) == True

        from_ind = db.get('custom', 1, with_doc=True)
        assert from_ind['doc'] != {}

    def test_delete_with_id_and_rev_only(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        ins = []
        for x in xrange(inserts):
            doc = dict(x=x)
            db.insert(doc)
            ins.append(doc)
            self.counter['ins'] += 1

        for i in xrange(inserts / 10):
            curr = ins.pop(random.randint(0, len(ins) - 1))
            d = {"_id": curr['_id'], '_rev': curr['_rev']}
            db.delete(d)

        assert len(ins) == db.count(db.all, 'id')

        for x in xrange(inserts):
            doc = dict(x=x)
            db.insert(doc)
            ins.append(doc)
            self.counter['ins'] += 1

        for i in xrange(inserts / 10):
            curr = ins.pop(random.randint(0, len(ins) - 1))
            db.delete(curr)

        assert len(ins) == db.count(db.all, 'id')

    def test_update_custom_unique(self, tmpdir, inserts):
            db = self._db(os.path.join(str(tmpdir), 'db'))
            db.set_indexes([UniqueHashIndex(db.path, 'id'),
                            CustomHashIndex(db.path, 'custom')])
            db.create()

            ins = []

            for x in xrange(inserts):
                doc = dict(test=1)
                db.insert(doc)
                ins.append(doc)
                self.counter['ins'] += 1

            assert len(ins) == db.count(db.all, 'id')
            assert len(ins) == db.count(db.all, 'custom')
            assert len(ins) == db.count(
                db.get_many, 'custom', key=0, limit=inserts + 1)
            assert 0 == db.count(
                db.get_many, 'custom', key=1, limit=inserts + 1)

            sample = random.sample(ins, inserts / 10)
            for curr in sample:
                curr['test'] = 10
                db.update(curr)
                self.counter['upd'] += 1

            assert self.counter['ins'] == db.count(db.all, 'id')
            assert self.counter['ins'] == db.count(db.all, 'custom')
            assert self.counter['upd'] == db.count(
                db.get_many, 'custom', key=1, limit=inserts + 1)
            assert self.counter['ins'] - self.counter['upd'] == db.count(
                db.get_many, 'custom', key=0, limit=inserts + 1)

    def test_update_custom_same_key_new_value(self, tmpdir, inserts):
            db = self._db(os.path.join(str(tmpdir), 'db'))
            db.set_indexes([UniqueHashIndex(db.path, 'id'),
                            CustomHashIndex(db.path, 'custom')])
            db.create()

            inserted = []
            for x in xrange(inserts):
                inserted.append(db.insert(dict(test=x)))
                inserted[-1]['test'] = x
            for el in inserted[::20]:
                for i in xrange(4):
                    curr = db.get('id', el['_id'], with_storage=True)
                    assert el['test'] == curr['test']
                    el['test'] += random.randint(1, 3)
                    db.update(el)
            assert len(inserted) == db.count(db.all, 'custom')

            db.close()

    def test_double_insert(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        a_id = '54bee5c4628648b5a742379a1de89b2d'
        a1 = dict(a=1, _id=a_id)
        db.insert(a1)
        a2 = dict(a=2, _id=a_id)
        with pytest.raises(IndexException):
            db.insert(a2)

    def test_adv1(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        l = []
        for i in xrange(inserts):
            c = dict(i=i)
            db.insert(c)
            l.append(c)
        for j in range(inserts):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']
            assert c['i'] == j
        for i in xrange(inserts):
            curr = l[i]
            c = db.get("id", curr['_id'])
            c['update'] = True
            db.update(c)

        for j in xrange(inserts):
            curr = l[i]
            c = db.get('id', curr['_id'])
            assert c['update'] == True

        for j in xrange(inserts):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert db.delete(c) == True

        for j in xrange(inserts):
            with pytest.raises(RecordDeleted):
                db.get('id', l[j]['_id'])

        db.close()

    def test_all(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        l = []
        for i in xrange(inserts):
            c = dict(i=i)
            db.insert(c)
            l.append(c)

        assert db.count(db.all, 'id') == inserts

        to_delete = random.randint(0, inserts - 1)
        db.delete(l[to_delete])

        assert db.count(db.all, 'id') == inserts - 1

    def test_compact(self, tmpdir):
            db = self._db(os.path.join(str(tmpdir), 'db'))
            db.set_indexes([UniqueHashIndex(db.path, 'id')])
            db.create()
            l = []
            for i in xrange(10):
                c = dict(i=i)
                db.insert(c)
                l.append(c)

            for i in xrange(10):
                curr = l[i]
                c = db.get("id", curr['_id'])
                c['update'] = True
                c.update(db.update(c))

            db.compact()

            for j in range(10):
                curr = l[j]
                c = db.get('id', curr['_id'])
                assert c['_id'] == curr['_id']
                assert c['i'] == j

            db.close()

    def test_compact2(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        l = []
        for i in xrange(10):
            if i % 2 == 0:
                c = dict(i=i, even=True)
            else:
                c = dict(i=i)
            c.update(db.insert(c))
            l.append(c)

        for i, curr in enumerate(db.all('id')):
            if i % 2:
                db.delete(curr)

        db.compact()
        db.compact()
        db.compact()
        assert len(list(db.all('id', with_doc=True))) == 5

        for curr in db.all('id'):
            db.delete(curr)

        assert len(list(db.all('id', with_doc=True))) == 0

    def test_similar(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes(
            [UniqueHashIndex(db.path, 'id'), Md5Index(db.path, 'md5')])
        db.create()

        a = dict(name='pigmej')
        db.insert(a)
        db.get('md5', 'pigmej')

        with pytest.raises(RecordNotFound):
            db.get("md5", 'pigme')

    def test_custom_index(self, tmpdir):

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(
            db.path, 'id'), CustomHashIndex(db.path, 'custom')])
        db.create()

        l_1 = []
        l_0 = []
        for i in xrange(10):
            c = dict(test=i)
            db.insert(c)
            if i > 5:
                l_1.append(c)
            else:
                l_0.append(c)

        i = 0
        for curr in db.all('custom', with_doc=False):
            i += 1

        assert i == len(l_1) + len(l_0)

        db.compact()

        gen = db.get_many('custom', key=1, limit=10, offset=0, with_doc=True)
        got = []
        while True:
            try:
                d = gen.next()
            except StopIteration:
                break
            else:
                got.append(d)
        assert len(l_1) == len(got)

        for doc in l_1:
            doc['test'] = 0
            db.update(doc)

        gen = db.get_many('custom', key=0, limit=100, offset=0, with_doc=False)
        got = []
        while True:
            try:
                d = gen.next()
            except StopIteration:
                break
            else:
                got.append(d)

        assert len(l_1) + len(l_0) == len(got)

        db.compact()

        gen = db.get_many('custom', key=0, limit=100, offset=0, with_doc=False)
        got = []
        while True:
            try:
                d = gen.next()
            except StopIteration:
                break
            else:
                got.append(d)

        assert len(l_1) + len(l_0) == len(got)

    def test_custom_index_2(self, tmpdir):

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes(
            [UniqueHashIndex(db.path, 'id'), WithAIndex(db.path, 'with_a')])
        db.create()

        all_ins = []
        for i in range(10):
            curr = dict(something='other')
            if i % 2 == 0:
                curr['a'] = str(i)
            all_ins.append(curr)
            db.insert(curr)

        l_0 = len(list(db.all('id')))
        l_1 = len(list(db.all('with_a')))
        assert l_1 != l_0

        all_a = list(db.all('with_a', with_doc=True))
        curr_a = all_a[0]['doc']
        del curr_a['a']
        db.update(curr_a)
        l_2 = len(list(db.all('with_a')))
        assert l_2 + 1 == l_1

        curr_a = all_a[-1]['doc']
        db.delete(curr_a)

        l_3 = len(list(db.all('with_a')))
        assert l_3 + 2 == l_1

    def test_insert_delete_compact_get_huge(self, tmpdir, inserts):
        inserts *= 10
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        l = []
        for i in xrange(inserts):
            c = dict(i=i)
            db.insert(c)
            l.append(c)

        assert db.count(db.all, 'id') == inserts

        will_delete = random.randint(0, inserts / 15)
        for x in range(will_delete):
            to_delete = random.randint(0, len(l) - 1)
            db.delete(l.pop(to_delete))

        assert db.count(db.all, 'id') == inserts - will_delete

        db.compact()

        assert db.count(db.all, 'id') == inserts - will_delete

    def test_all_same_keys(self, tmpdir, inserts):

            db = self._db(os.path.join(str(tmpdir), 'db'))
            db.set_indexes([UniqueHashIndex(db.path, 'id'),
                            WithAIndex(db.path, 'with_a')])

            db.create()
            l = 0
            r = 0
            z = 0
            data = []
            for i in xrange(inserts):
                a = random.randint(0, 10)
                if a > 5:
                    r += 1
                elif a == 0:
                    z += 1
                else:
                    l += 1
                c = dict(a=a)
                db.insert(c)
                data.append(c)

            assert l + r + z == db.count(db.all, "id")

            assert l + r == db.count(db.all, "with_a")

    def test_update_same(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id'),
                        WithAIndex(db.path, 'with_a'),
                        CustomHashIndex(db.path, 'custom')])
        db.create()

        inserted = {}

        for i in xrange(inserts):
            a = random.randint(1, 9)
            # a = 10
            if a > 5:
                self.counter['r'] += 1
            else:
                self.counter['l'] += 1
            c = dict(test=a)
            db.insert(c)
            inserted[c['_id']] = c

        def _check():

            assert self.counter['l'] == db.count(db.get_many,
                                                 'custom', key=0, limit=inserts)
            assert self.counter['r'] == db.count(db.get_many,
                                                 'custom', key=1, limit=inserts)
            assert self.counter['r'] + self.counter[
                'l'] == db.count(db.all, "custom")

        _check()

        d = inserted.values()[inserts / 2]
        for curr_u in range(20):
            last_test = d['test']
            if last_test > 5:
                d['test'] = 1
                self.counter['r'] -= 1
                self.counter['l'] += 1
            else:
                d['test'] = 6
                self.counter['l'] -= 1
                self.counter['r'] += 1
            d['upd'] = curr_u
            db.update(d)
        _check()

        num = inserts / 10
        num = num if num < 20 else 20

        to_update = random.sample(inserted.values(), num)
        for d in to_update:
            for curr_u in range(10):
                last_test = d['test']
                if last_test > 5:
                    d['test'] = 1
                    self.counter['r'] -= 1
                    self.counter['l'] += 1
                else:
                    d['test'] = 6
                    self.counter['l'] -= 1
                    self.counter['r'] += 1
                d['upd'] = curr_u
                db.update(d)
        _check()

        to_update = random.sample(inserted.values(), num)
        for curr_u in range(10):
            for d in to_update:
                last_test = d['test']
                if last_test > 5:
                    d['test'] = 1
                    self.counter['r'] -= 1
                    self.counter['l'] += 1
                else:
                    d['test'] = 6
                    self.counter['l'] -= 1
                    self.counter['r'] += 1
                d['upd'] = curr_u
                db.update(d)
        _check()

        db.close()

    def test_insert_on_deleted(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        a = db.insert(dict(_id='dd0604e2110d41e9a2ec630461ffd067'))
        db.insert(dict(_id='892508163deb4da0b44e8a00802dc75a'))
        db.delete(a)
        db.insert(dict(_id='d0a6e4e1aa74476a9012f9b8d7181a95'))
        db.get('id', '892508163deb4da0b44e8a00802dc75a')

        db.close()

    def test_offset_in_functions(self, tmpdir, inserts):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id'),
                        CustomHashIndex(db.path, 'custom')])
        db.create()
        offset = inserts // 10 or 1
        real_inserts = inserts if inserts < 1000 else 1000
        for x in xrange(real_inserts):
                db.insert(dict(test=x))
        assert real_inserts - offset == db.count(db.all, 'id', offset=offset)
        assert real_inserts - offset == db.count(db.all, 'custom',
                                                 offset=offset)
        assert 1 == db.count(db.get_many, 'custom', 1, limit=1, offset=offset)

        db.close()
