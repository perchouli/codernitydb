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
from CodernityDB.database import DatabaseException, RevConflict, DatabasePathException, DatabaseConflict, PreconditionsException, IndexConflict

from CodernityDB.hash_index import HashIndex, UniqueHashIndex, MultiHashIndex
from CodernityDB.index import IndexException, TryReindexException, IndexNotFoundException, IndexPreconditionsException

from CodernityDB.tree_index import TreeBasedIndex, MultiTreeBasedIndex

from CodernityDB.debug_stuff import database_step_by_step

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
        # kwargs['entry_line_format'] = '<32sIIIcI'
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
        # kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['key_format'] = '16s'
        kwargs['hash_lim'] = 4 * 1024
        super(Md5Index, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        return md5(data['name']).digest(), None

    def make_key(self, key):
        return md5(key).digest()


class WithAIndex2(HashIndex):

    def __init__(self, *args, **kwargs):
        # kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['key_format'] = '16s'
        kwargs['hash_lim'] = 4 * 1024
        super(WithAIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val is not None:
            if not isinstance(a_val, basestring):
                a_val = str(a_val)
                return md5(a_val).digest(), None
                return None

    def make_key(self, key):
        if not isinstance(key, basestring):
            key = str(key)
            return md5(key).digest()


class WithAIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        # kwargs['entry_line_format'] = '<32s32sIIcI'
        kwargs['key_format'] = '16s'
        kwargs['hash_lim'] = 4 * 1024
        super(WithAIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val is not None:
            if not isinstance(a_val, basestring):
                a_val = str(a_val)
            return md5(a_val).digest(), None
        return None

    def make_key(self, key):
        if not isinstance(key, basestring):
            key = str(key)
        return md5(key).digest()


class Simple_TreeIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 100
        kwargs['key_format'] = 'I'
        super(Simple_TreeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        t_val = data.get('t')
        if t_val is not None:
            return t_val, {}
        return None

    def make_key(self, key):
        return key


class WithRun_Index(HashIndex):

    def __init__(self, *args, **kwargs):
        # kwargs['entry_line_format'] = '<32sIIIcI'
        kwargs['key_format'] = 'I'
        kwargs['hash_lim'] = 4 * 1024
        super(WithRun_Index, self).__init__(*args, **kwargs)

    def run_sum(self, db_obj, key):
        gen = db_obj.get_many(index_name=self.name, key=key,
                              limit=-1, with_storage=True)
        vals = []
        while True:
            try:
                d = gen.next()
            except StopIteration:
                break
            else:
                vals.append(d.get('x', 0))
        return sum(vals)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val is not None:
            out = {'x': data.get('x')}
            return a_val, out
        return None

    def make_key(self, key):
        return key


class WithRunEdit_Index(HashIndex):

    def __init__(self, *args, **kwargs):
        # kwargs['entry_line_format'] = '<32sIIIcI'
        kwargs['key_format'] = 'I'
        kwargs['hash_lim'] = 4 * 1024
        super(WithRunEdit_Index, self).__init__(*args, **kwargs)

    def run_sum(self, db_obj, key):
        gen = db_obj.get_many(index_name=self.name, key=key,
                              limit=-1, with_storage=True)
        vals = []
        while True:
            try:
                d = gen.next()
            except StopIteration:
                break
            else:
                vals.append(d.get('x', 0))
        return sum(vals)

    def make_key_value(self, data):
        a_val = data.get("a")
        if a_val is not None:
            out = {'x': data.get('x') * 2}
            return a_val, out
        return None

    def make_key(self, key):
        return key


class TreeMultiTest(MultiTreeBasedIndex):

    custom_header = """from CodernityDB.tree_index import MultiTreeBasedIndex
from itertools import izip"""

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = '16s'
        super(TreeMultiTest, self).__init__(*args, **kwargs)
        self.__l = kwargs.get('w_len', 2)

    def make_key_value(self, data):
        name = data['w']
        l = self.__l
        max_l = len(name)
        out = set()
        for x in xrange(l - 1, max_l):
            m = (name, )
            for y in xrange(0, x):
                m += (name[y + 1:],)
            out.update(set(''.join(x).rjust(
                16, '_').lower() for x in izip(*m)))  # ignore import error
        return out, dict(name=name)

    def make_key(self, key):
        return key.rjust(16, '_').lower()


class DB_Tests:

    def setup_method(self, method):
        self.counter = Counter()

    def test_update_conflict(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        doc = dict(a=1)
        db.insert(doc)
        doc2 = doc.copy()
        doc2['_rev'] = '00000000'
        with pytest.raises(RevConflict):
            db.update(doc2)

    def test_wrong_id(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        with pytest.raises(IndexPreconditionsException):
            db.insert(dict(_id='1', a=1))

        with pytest.raises(IndexPreconditionsException):
            db.insert(dict(_id=1, a=1))

    def test_open_close(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        l = []
        for i in xrange(5):
            c = dict(i=i)
            db.insert(c)
            l.append(c)
        db.close()
        db.open()
        db.close()
        db2 = self._db(os.path.join(str(tmpdir), 'db'))
        # db2.set_indexes([UniqueHashIndex(db.path, 'id')])
        db2.open()
        for j in xrange(5):
            assert l[j] == db2.get('id', l[j]['_id'])
        db2.close()

    def test_destroy(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        for i in xrange(5):
            db.insert(dict(i=i))
        db.destroy()
        db = self._db(os.path.join(str(tmpdir), 'db'))
        with pytest.raises(DatabasePathException):
            db.open()

    def test_exists(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')
        db = self._db(p)
        assert db.exists() == False
        db.create()
        assert db.exists() == True
        db.destroy()
        assert db.exists() == False

    def test_double_create(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')
        db = self._db(p)
        db.create()
        db2 = self._db(p)
        with pytest.raises((DatabaseConflict, IndexConflict)):
            db2.create()
        db2.open()
        db.destroy()
        db2 = self._db(p)
        db.create()

    def test_real_life_example_random(self, tmpdir, operations):

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id'),
                        WithAIndex(db.path, 'with_a'),
                        CustomHashIndex(db.path, 'custom'),
                        Simple_TreeIndex(db.path, 'tree')])
        db.create()
        database_step_by_step(db)

        inserted = {}
        updated = {}

        def _insert():
            doc = dict(i=random.randint(0, 1000))
            if random.randint(0, 1):
                doc['a'] = random.randint(1, 15)
            if random.randint(0, 1):
                doc['test'] = random.randint(1, 9)
                if doc['test'] > 5:
                    self.counter['r'] += 1
                else:
                    self.counter['l'] += 1
            if random.randint(0, 1):
                doc['t'] = random.randint(0, 500)
            db.insert(doc)
            inserted[doc['_id']] = doc
            return True

        def _update():
            vals = inserted.values()
            if not vals:
                return False
            doc = random.choice(vals)
            a = random.randint(0, 1000)
            doc['upd'] = a
            was = doc.get('test')
            if was is not None:
                if was > 5:
                    self.counter['r'] -= 1
                else:
                    self.counter['l'] -= 1
            doc['test'] = random.randint(1, 9)
            if doc['test'] > 5:
                self.counter['r'] += 1
            else:
                self.counter['l'] += 1
            db.update(doc)
            assert db.get('id', doc['_id'])['upd'] == a
            updated[doc['_id']] = doc
            return True

        def _delete():
            vals = inserted.values()
            if not vals:
                return False
            doc = random.choice(vals)
            was = doc.get('test')
            if was is not None:
                if was > 5:
                    self.counter['r'] -= 1
                else:
                    self.counter['l'] -= 1
            db.delete(doc)
            del inserted[doc['_id']]
            try:
                del updated[doc['_id']]
            except:
                pass
            return True

        def _get():
            vals = inserted.values()
            if not vals:
                return False
            doc = random.choice(vals)
            got = db.get('id', doc['_id'])
            assert got == doc
            return True

        def _compact():
            db.compact()

        def _reindex():
            db.reindex()

        def count_and_check():
            assert len(inserted) == db.count(db.all, 'id')
            l_c = db.count(db.get_many, 'custom', key=0, limit=operations)
            r_c = db.count(db.get_many, 'custom', key=1, limit=operations)
            same = set(map(lambda x: x['_id'], db.get_many('custom', key=0, limit=operations))).intersection(set(map(lambda x: x['_id'], db.get_many('custom', key=1, limit=operations))))
            assert same == set()
            assert self.counter['l'] == l_c
            assert self.counter['r'] == r_c
            assert self.counter['l'] + self.counter[
                'r'] == db.count(db.all, 'custom')

        fcts = (
            _insert,) * 20 + (_get,) * 10 + (_update,) * 10 + (_delete,) * 5
        for i in xrange(operations):
            f = random.choice(fcts)
            f()

        # db.reindex()
        count_and_check()

        db.reindex()

        count_and_check()

        fcts = (
            _insert,) * 20 + (_get,) * 10 + (_update,) * 10 + (_delete,) * 5
        for i in xrange(operations):
            f = random.choice(fcts)
            f()

        count_and_check()

        db.close()

    def test_add_new_index_to_existing_db(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))

        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        new_index = UniqueHashIndex(db.path, 'unique_hash_index')
        db.add_index(new_index)

        new_index = Md5Index(db.path, 'md5_index')
        db.add_index(new_index)

        new_index = WithAIndex(db.path, 'with_a_index')
        db.add_index(new_index)

        assert len(db.indexes) == 4

    def test_add_new_index_to_existing_db_2(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        new_index = HashIndex(db.path, 'hash_index')
        db.add_index(new_index)

        new_index = Md5Index(db.path, 'md5_index')
        db.add_index(new_index)

        new_index = WithAIndex(db.path, 'withA_index')
        db.add_index(new_index)

        for y in range(100):
            db.insert(dict(y=y))

        for index in ('id', 'hash_index', 'md5_index', 'withA_index'):
            elements = db.all('hash_index')
            assert len(list(elements)) == 100

    def test_add_duplicate_index_throws_exception(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))

        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        new_index = UniqueHashIndex(db.path, 'another_index')
        db.add_index(new_index)

        new_index = UniqueHashIndex(db.path, 'another_index')
        with pytest.raises(IndexException):
            db.add_index(new_index)

    def test_add_new_index_from_string(self, tmpdir):

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        file_names = ('tests/index_files/03md5_index.py',
                      'tests/index_files/04withA_index.py',
                      'tests/index_files/05custom_hash_index.py')

        indexes_names = [db.add_index(open(f).read()) for f in file_names]

        assert len(db.indexes) == len(file_names) + 1  # 'id' + from files

        for y in range(100):
            db.insert(dict(a='blah', test='blah', name=str(y), y=y))

        for index_name in indexes_names:
            assert db.count(db.all, index_name) == 100

    def test_adding_index_creates_dot_py_file(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        path_indexes = os.path.join(db.path, '_indexes')

        before = set(os.listdir(path_indexes))

        new_index = Md5Index(db.path, 'md5_index')
        db.add_index(new_index)

        after = set(os.listdir(path_indexes))

        added_file = tuple(after - before)[0]

        assert len(after) == len(before) + 1
        assert new_index.name + '.py' in added_file

    def test_removing_index_from_db(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        path_indexes = os.path.join(db.path, '_indexes')

        def file_exists_in_indexes_dir(name):
            return [f for f in os.listdir(path_indexes) if name in f]

        indexes = [klass(db.path, klass.__name__) for klass in (
            HashIndex, WithAIndex, Md5Index)]

#        with name
        for index in indexes:
            index_name = index.name
            db.add_index(index)
            assert db.get_index_details(index_name)  # wants name
            assert file_exists_in_indexes_dir(index_name)

            db.destroy_index(index_name)

            assert not file_exists_in_indexes_dir(index_name)
            print file_exists_in_indexes_dir(index_name)
            with pytest.raises(IndexNotFoundException):
                db.get_index_details(index_name)
#        with instance
        indexes = [klass(db.path, klass.__name__) for klass in (
            HashIndex, WithAIndex, Md5Index)]
        for index in indexes:
            index_name = index.name
            db.add_index(index)
            assert db.get_index_details(index_name)  # wants name
            assert file_exists_in_indexes_dir(index_name)
            index_to_destory = db.indexes_names[index_name]
            db.destroy_index(index_to_destory)

            assert not file_exists_in_indexes_dir(index_name)
            print file_exists_in_indexes_dir(index_name)
            with pytest.raises(IndexNotFoundException):
                db.get_index_details(index_name)

#        with other instance
        indexes = [klass(db.path, klass.__name__) for klass in (
            HashIndex, WithAIndex, Md5Index)]
        for index in indexes:
            index_name = index.name
            db.add_index(index)
            assert db.get_index_details(index_name)  # wants name
            assert file_exists_in_indexes_dir(index_name)

            klass = index.__class__

            with pytest.raises(DatabaseException):
                db.destroy_index(klass(db.path, klass.__name__))

            assert file_exists_in_indexes_dir(index_name)
            print file_exists_in_indexes_dir(index_name)
            assert db.get_index_details(index_name)

    def test_removing_index_from_db_2(self, tmpdir):
        '''indexes are added from strings'''

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        path_indexes = os.path.join(db.path, '_indexes')

        def file_exists_in_indexes_dir(name):
            return [f for f in os.listdir(path_indexes) if name in f]

        file_names = ('tests/index_files/03md5_index.py',
                      'tests/index_files/04withA_index.py',
                      'tests/index_files/05custom_hash_index.py')

        indexes_names = [db.add_index(open(f).read()) for f in file_names]

        for index_name in indexes_names:
            assert db.get_index_details(index_name)
            assert file_exists_in_indexes_dir(index_name)

            db.destroy_index(index_name)

            assert not file_exists_in_indexes_dir(index_name)
            with pytest.raises(IndexException):
                db.get_index_details(index_name)

    def test_update_index(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        hash_index = HashIndex(db.path, 'my_index')
        db.add_index(hash_index)
        assert db.get_index_details(hash_index.name)

        db.destroy_index(hash_index.name)
        with pytest.raises(IndexNotFoundException):
                db.get_index_details(hash_index.name)

        new_index = Md5Index(db.path, 'my_index')
        db.add_index(new_index)
        assert db.get_index_details(new_index.name)

    def test_create_index_duplicate_from_object(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()

        hash_index = HashIndex(db.path, 'my_index')
        db.add_index(hash_index)

        l = len(db.indexes)
        hash_index = HashIndex(db.path, 'my_index')
        with pytest.raises(IndexException):
            db.add_index(hash_index)

        assert l == len(db.indexes)

    def test_create_index_duplicate_from_string(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.set_indexes([UniqueHashIndex(db.path, 'id')])
        db.create()
        path_indexes = os.path.join(db.path, '_indexes')

        file_name = 'tests/index_files/04withA_index.py'

        db.add_index(open(file_name).read())
        l = len(db.indexes)
        with pytest.raises(IndexException):
            db.add_index(open(file_name).read())

        assert l == len(db.indexes)

    def test_create_with_path(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(None)
        db.create(p, with_id_index=False)

        ind = UniqueHashIndex(p, 'id')
        db.add_index(ind)

        for x in range(10):
            db.insert(dict(x=x))

        assert db.count(db.all, 'id') == 10

    def test_auto_add_id_index(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(None)
        db.initialize(p)
        db.create()

        ind = UniqueHashIndex(p, 'id')
        with pytest.raises(IndexException):
            db.add_index(ind)

        for x in range(10):
            db.insert(dict(x=x))

        assert db.count(db.all, 'id') == 10

    def test_auto_add_id_index_without_initialize(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(None)
        db.create(p)

        ind = UniqueHashIndex(p, 'id')
        with pytest.raises(IndexException):
            db.add_index(ind)

        for x in range(10):
            db.insert(dict(x=x))

        assert db.count(db.all, 'id') == 10

    def test_open_with_path(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(p)
        ind = UniqueHashIndex(p, 'id')
        db.set_indexes([ind])
        db.create(with_id_index=False)

        for x in range(10):
            db.insert(dict(x=x))

        db.close()

        db = self._db(p)
        db.open(p)

        assert db.count(db.all, 'id') == 10

    def test_create_path_delayed1(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(None)
        db.initialize(p)

        ind = UniqueHashIndex(p, 'id')
        db.add_index(ind, create=False)
        db.create(with_id_index=False)

        for x in range(10):
            db.insert(dict(x=x))

        db.close()

        db = self._db(p)
        db.open(p)

        assert db.count(db.all, 'id') == 10

    def test_create_path_delayed2(self, tmpdir):
        p = os.path.join(str(tmpdir), 'db')

        db = self._db(None)
        db.initialize(p)
        db.create(with_id_index=False)

        ind = UniqueHashIndex(p, 'id')
        db.add_index(ind)

        for x in range(10):
            db.insert(dict(x=x))

        db.close()

        db = self._db(p)
        db.open(p)

        assert db.count(db.all, 'id') == 10

    def test_compact_index_with_different_types(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        ind_id = UniqueHashIndex(db.path, 'id')
        db.set_indexes([ind_id])
        db.create()
        l = []
        for i in xrange(10):
            c = dict(i=i)
            c.update(db.insert(c))
            l.append(c)

#        with name
        for i in xrange(10):
            curr = l[i]
            c = db.get("id", curr['_id'])
            c['update'] = True
            c.update(db.update(c))

        db.compact_index(ind_id.name)
        for j in range(10):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']
            assert c['i'] == j

#        with instance
        for i in xrange(10):
            curr = l[i]
            c = db.get("id", curr['_id'])
            c['update'] = True
            c.update(db.update(c))

        ind_id = db.indexes_names[ind_id.name]
        db.compact_index(ind_id)
        for j in range(10):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']
            assert c['i'] == j

 #        with different instance
        for i in xrange(10):
            curr = l[i]
            c = db.get("id", curr['_id'])
            c['update'] = True
            c.update(db.update(c))

        with pytest.raises(DatabaseException):
            db.compact_index(UniqueHashIndex(db.path, 'id'))

        for j in range(10):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']
            assert c['i'] == j

        db.close()

    def test_reindex_index_with_different_types(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        ind_id = UniqueHashIndex(db.path, 'id')
        ind_a = WithAIndex(db.path, 'with_a')
        ind_c = CustomHashIndex(db.path, 'custom')

        db.set_indexes([ind_id,
                        ind_a,
                        ind_c])
        ind_id = db.indexes_names[ind_id.name]
        ind_a = db.indexes_names[ind_a.name]

        db.create()

        l = []
        for i in xrange(100):
            c = dict(i=i)
            db.insert(c)
            l.append(c)

        for i in range(10):
            nr = random.randint(0, len(l) - 1)
            curr = l[nr]
            db.delete(curr)
            del(l[nr])

#        index id
        with pytest.raises(DatabaseException):
            db.reindex_index(ind_id)

        for j in range(len(l)):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']

#        with instance
        for i in range(10):
            nr = random.randint(0, len(l) - 1)
            curr = l[nr]
            db.delete(curr)
            del(l[nr])

        db.reindex_index(ind_a)
        for j in range(len(l)):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']

#        with name
        for i in range(10):
            nr = random.randint(0, len(l) - 1)
            curr = l[nr]
            db.delete(curr)
            del(l[nr])

        db.reindex_index(ind_c.name)
        for j in range(len(l)):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']

        for i in range(10):
            nr = random.randint(0, len(l) - 1)
            curr = l[nr]
            db.delete(curr)
            del(l[nr])
#        with different instance
        with pytest.raises(DatabaseException):
            db.reindex_index(WithAIndex(db.path, 'with_a'))

        for j in range(len(l)):
            curr = l[j]
            c = db.get('id', curr['_id'])
            assert c['_id'] == curr['_id']

        db.close()

    def test_add_new_index_update_before_reindex_new_value(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        for x in xrange(20):
            db.insert(dict(t=x, test=x))
        el = db.insert(dict(t=1, test=1))
        el['new_data'] = 'new'
        el['test'] = 2
        db.add_index(CustomHashIndex(db.path, 'custom'))
        db.add_index(Simple_TreeIndex(db.path, 'tree'))
        with pytest.raises(TryReindexException):
            db.update(el)

    def test_add_new_index_update_before_reindex_old_value(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        for x in xrange(20):
            db.insert(dict(t=x, test=x))
        el = db.insert(dict(t=1, test=1))
        el['new_data'] = 'new'
        db.add_index(CustomHashIndex(db.path, 'custom'))
        db.add_index(Simple_TreeIndex(db.path, 'tree'))
        with pytest.raises(TryReindexException):
            db.update(el)

    def test_add_new_index_delete_before_reindex(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        for x in xrange(20):
            db.insert(dict(t=x, a=x))
        el = db.insert(dict(t=1, a=1))
#        el['new_data']='new'
        db.add_index(WithAIndex(db.path, 'with_a'))
        db.add_index(Simple_TreeIndex(db.path, 'tree'))
        db.delete(el)

    def test_runnable_functions(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(WithRun_Index(db.path, 'run'))
        for x in xrange(20):
            db.insert(dict(a=x % 2, x=x))
        assert db.run('run', 'sum', 0) == 90
        assert db.run('run', 'sum', 1) == 100
        assert db.run('run', 'sum', 2) == 0
        # not existsing
        with pytest.raises(IndexException):
            db.run('run', 'not_existing', 10)
        # existing, but should't run because of run_ prefix
        with pytest.raises(IndexException):
            db.run('run', 'destroy', 10)

    def test_get_error(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        _id = md5('test').hexdigest()
        with pytest.raises(RecordNotFound):
            db.get('id', _id)
        db.insert(dict(_id=_id, test='test'))
        db.get('id', _id)
        db.close()

    def test_edit_index(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(WithRun_Index(db.path, 'run'))
        for x in xrange(20):
            db.insert(dict(a=x % 2, x=x))
        assert db.run('run', 'sum', 0) == 90
        assert db.run('run', 'sum', 1) == 100
        assert db.run('run', 'sum', 2) == 0
        db.edit_index(WithRunEdit_Index(db.path, 'run'))
        assert db.run('run', 'sum', 0) == 90
        assert db.run('run', 'sum', 1) == 100
        assert db.run('run', 'sum', 2) == 0
        db.insert(dict(a=1, x=1))
        assert db.run('run', 'sum', 1) == 102
        db.edit_index(WithRunEdit_Index(db.path, 'run'), reindex=True)
        assert db.run('run', 'sum', 0) == 90 * 2
        assert db.run('run', 'sum', 1) == 100 * 2 + 2
        assert db.run('run', 'sum', 2) == 0

    def test_add_index_bef(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        with pytest.raises(PreconditionsException):
            db.add_index(CustomHashIndex(db.path, 'custom'))
        db.add_index(UniqueHashIndex(db.path, 'id'))
        db.add_index(CustomHashIndex(db.path, 'custom'))

    def test_multi_index(self, tmpdir):
        with open('tests/misc/words.txt', 'r') as f:
            data = f.read().split()
        words = map(
            lambda x: x.strip().replace('.', "").replace(',', ""), data)
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(TreeMultiTest(db.path, 'words'))
        for word in words:
            db.insert(dict(w=word))
        assert db.count(db.all, 'words') == 3245
        assert db.get('words', 'Coder')['name'] == 'Codernity'
        assert db.get('words', "dern")['name'] == "Codernity"
        assert db.get('words', 'Codernity')['name'] == "Codernity"

        u = db.get('words', 'Codernit', with_doc=True)
        doc = u['doc']
        doc['up'] = True
        db.update(doc)
        assert db.get('words', "dern")['name'] == "Codernity"

        db.delete(doc)
        with pytest.raises(RecordNotFound):
            db.get('words', "Codern")

    def test_add_indented_index(self, tmpdir):
        class IndentedMd5Index(HashIndex):

            def __init__(self, *args, **kwargs):
                # kwargs['entry_line_format'] = '<32s32sIIcI'
                kwargs['key_format'] = '16s'
                kwargs['hash_lim'] = 4 * 1024
                super(IndentedMd5Index, self).__init__(*args, **kwargs)

            def make_key_value(self, data):
                return md5(data['name']).digest(), None

            def make_key(self, key):
                return md5(key).digest()

        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        db.add_index(IndentedMd5Index(db.path, 'ind'))

        db.insert(dict(name='a'))
        assert db.get('ind', 'a', with_doc=True)['doc']['name'] == 'a'

    def test_patch_flush_fsync(self, tmpdir):
        from CodernityDB.patch import patch_flush_fsync
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()
        patch_flush_fsync(db)  # patch it
        for x in xrange(100):
            db.insert(dict(x=x))
        db.close()

    def test_revert_index(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        db.create()

        ok = """name: test_revert
type: HashIndex
key_format: I
make_key_value:
x, None
"""

        ok2 = """name: test_revert
type: HashIndex
key_format: I
make_key_value:
x * 10, None
"""
        db.add_index(ok)

        for x in xrange(10):
            db.insert(dict(x=x))

        a = sum(map(lambda x: x['key'], db.all('test_revert')))
        assert a == 45

        db.edit_index(ok2, reindex=True)

        a = sum(map(lambda x: x['key'], db.all('test_revert')))
        assert a == 450

        db.revert_index('test_revert', reindex=True)

        a = sum(map(lambda x: x['key'], db.all('test_revert')))
        assert a == 45

        with pytest.raises(DatabaseException):
            db.revert_index('test_revert', reindex=True)  # second restore
