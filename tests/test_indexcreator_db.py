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


from CodernityDB.indexcreator import Parser, IndexCreatorValueException, IndexCreatorFunctionException
from CodernityDB.database import Database, RecordNotFound
from CodernityDB.hash_index import HashIndex
from CodernityDB.tree_index import TreeBasedIndex
from CodernityDB.tree_index import MultiTreeBasedIndex
from CodernityDB.hash_index import MultiHashIndex
from itertools import izip
from hashlib import md5
from py.test import raises
import os
import uuid

# class db_set():
    # def __init__(self,t):
        # self.t = t
    # def __enter__(self):
        # self.db = Database(os.path.join(str(self.t), 'db'))
        # self.db.create()
        # return self.db
    # def __exit__(self,type,value,traceback):
        # self.db.destroy()


def pytest_funcarg__db(request):
    db = Database(os.path.join(str(request.getfuncargvalue('tmpdir')), 'db'))
    db.create()
    return db


def compare_for_multi_index(db, name, s, key_name, data, keys):
    db.add_index(s)

    for i in data:
        db.insert({key_name: i})

    # for i in db.all(name):
        # print i

    for i, k in keys:
        if k is None:
            with raises(RecordNotFound):
                db.get(name, i, with_doc=True)
        else:
            assert db.get(name, i, with_doc=True)['doc'][key_name] == k


class TestIndexCreatorWithDatabase:
    def test_output_check(self, db, tmpdir):
        s = """
        type = HashIndex
        name = s
        key_format =     32s
        hash_lim = 1
        make_key_value:
        0,None
        make_key:
        0
        """
        db.add_index(s)
        assert s == db.get_index_code('s', code_switch='S')

        s1 = """
        type = TreeBasedIndex
        name = s1
        key_format =     32s
        make_key_value:
        0,None
        make_key:
        0
        """
        db.add_index(s1)
        assert s1 == db.get_index_code('s1', code_switch='S')


class TestMultiIndexCreatorWithInternalImports:
    def test_infix(self, db):
        s = """
        name = s
        type = MultiTreeBasedIndex
        key_format: 3s
        make_key_value:
        infix(a,2,3,3),null
        make_key:
        fix_r(key,3)
        """
        compare_for_multi_index(db, 's', s, 'a', ['abcd'], [("a", None),
                                                            ("ab", "abcd"),
                                                            ("abc", "abcd"),
                                                            ("b", None),
                                                            ("abcd", "abcd"),  # fix_r will trim it to 3 letters!
                                                            ("bcd", "abcd"),
                                                            ("abdc", None)
                                                            ])

        s2 = """
        name = s2
        type = MultiTreeBasedIndex
        key_format: 5s
        make_key_value:
        infix(a,0,20,5),None
        make_key:
        fix_r(key,5)
        """
        compare_for_multi_index(db, 's2', s2, 'a', ["abcd"], [("a", "abcd"),
                                                              ("ab", "abcd"),
                                                              ("abc", "abcd"),
                                                              ("b", "abcd"),
                                                              ("abcd", "abcd"),
                                                              ("bcd", "abcd"),
                                                              ("abdc", None)
                                                              ])

    def test_more_than_one_func(self, db):
        s = """
        name = s
        type = MultiTreeBasedIndex
        key_format: 3s
        make_key_value:
        len(a)>3:infix(a,2,3,3),null
        prefix(a,2,3,3),none
        make_key:
        fix_r(key,3)
        """
        compare_for_multi_index(db, 's', s, 'a', ['abcd'], [("a", None),
                                                            ("ab", "abcd"),
                                                            ("abc", "abcd"),
                                                            ("b", None),
                                                            ("abcd", "abcd"),  # fix_r will trim it to 3 letters!
                                                            ("bcd", "abcd"),
                                                            ("abdc", None)
                                                            ])
