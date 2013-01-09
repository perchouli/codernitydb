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


import pytest
from CodernityDB.database import Database
from CodernityDB.sharded_hash import ShardedUniqueHashIndex
from CodernityDB.index import IndexPreconditionsException


class ShardedUniqueHashIndex5(ShardedUniqueHashIndex):

    custom_header = 'from CodernityDB.sharded_hash import ShardedUniqueHashIndex'

    def __init__(self, *args, **kwargs):
        kwargs['sh_nums'] = 5
        super(ShardedUniqueHashIndex5, self).__init__(*args, **kwargs)


class ShardedUniqueHashIndex10(ShardedUniqueHashIndex):

    custom_header = 'from CodernityDB.sharded_hash import ShardedUniqueHashIndex'

    def __init__(self, *args, **kwargs):
        kwargs['sh_nums'] = 10
        super(ShardedUniqueHashIndex10, self).__init__(*args, **kwargs)


class ShardedUniqueHashIndex50(ShardedUniqueHashIndex):

    custom_header = 'from CodernityDB.sharded_hash import ShardedUniqueHashIndex'

    def __init__(self, *args, **kwargs):
        kwargs['sh_nums'] = 50
        super(ShardedUniqueHashIndex50, self).__init__(*args, **kwargs)


class ShardTests:

    def test_create(self, tmpdir):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        db.add_index(ShardedUniqueHashIndex(db.path, 'id', sh_nums=3))

    @pytest.mark.parametrize(('sh_nums', ), [(x,) for x in (5, 10, 50)])
    def test_num_shards(self, tmpdir, sh_nums):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        n = globals()['ShardedUniqueHashIndex%d' % sh_nums]
        db.add_index(n(db.path, 'id'))
        assert db.id_ind.sh_nums == sh_nums

    @pytest.mark.parametrize(('sh_nums', ), [(x,) for x in (5, 10, 50)])
    def test_insert_get(self, tmpdir, sh_nums):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        n = globals()['ShardedUniqueHashIndex%d' % sh_nums]
        db.add_index(n(db.path, 'id'))
        l = []
        for x in xrange(10000):
            l.append(db.insert(dict(x=x))['_id'])

        for curr in l:
            assert db.get('id', curr)['_id'] == curr

    @pytest.mark.parametrize(('sh_nums', ), [(x,) for x in (5, 10, 50)])
    def test_all(self, tmpdir, sh_nums):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        n = globals()['ShardedUniqueHashIndex%d' % sh_nums]
        db.add_index(n(db.path, 'id'))
        l = []
        for x in xrange(10000):
            l.append(db.insert(dict(x=x))['_id'])

        for curr in db.all('id'):
            l.remove(curr['_id'])

        assert l == []

    def test_to_many_shards(self, tmpdir):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        # it's ok to use sharded directly there
        with pytest.raises(IndexPreconditionsException):
            db.add_index(ShardedUniqueHashIndex(db.path, 'id', sh_nums=300))
        with pytest.raises(IndexPreconditionsException):
            db.add_index(ShardedUniqueHashIndex(db.path, 'id', sh_nums=256))

    def test_compact_shards(self, tmpdir):
        db = Database(str(tmpdir) + '/db')
        db.create(with_id_index=False)
        db.add_index(ShardedUniqueHashIndex5(db.path, 'id'))

        for x in xrange(100):
            db.insert({'x': x})

        db.compact()
        assert db.count(db.all, 'id') == 100
