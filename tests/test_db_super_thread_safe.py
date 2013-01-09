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


from CodernityDB.database_super_thread_safe import SuperThreadSafeDatabase

from shared import DB_Tests
from hash_tests import HashIndexTests
from tree_tests import TreeIndexTests
from test_db_thread_safe import Test_Threads


class Test_Database(DB_Tests):

    _db = SuperThreadSafeDatabase


class Test_HashIndex(HashIndexTests):

    _db = SuperThreadSafeDatabase


class Test_TreeIndex(TreeIndexTests):

    _db = SuperThreadSafeDatabase


class Test_Threads(Test_Threads):

    _db = SuperThreadSafeDatabase
