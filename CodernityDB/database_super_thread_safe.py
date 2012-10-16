#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011-2012 Codernity (http://codernity.com)
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


from threading import RLock

from CodernityDB.env import cdb_environment

cdb_environment['mode'] = "threads"
cdb_environment['rlock_obj'] = RLock

from database import Database

from functools import wraps
from types import FunctionType, MethodType


class SuperLock(type):

    @staticmethod
    def wrapper(f):
        @wraps(f)
        def _inner(*args, **kwargs):
            db = args[0]
            with db.super_lock:
                res = f(*args, **kwargs)
                if db.opened:
                    db.flush()
                return res
        return _inner

    def __new__(cls, classname, bases, attr):
        new_attr = {}
        for base in bases:
            for b_attr in dir(base):
                a = getattr(base, b_attr, None)
                if isinstance(a, MethodType) and not b_attr.startswith('_'):
                    if b_attr == 'flush' or b_attr == 'flush_indexes':
                        pass
                    else:
                        #setattr(base, b_attr, SuperLock.wrapper(a))
                        new_attr[b_attr] = SuperLock.wrapper(a)
        for attr_name, attr_value in attr.iteritems():
            if isinstance(attr_value, FunctionType) and not attr_name.startswith('_'):
                attr_value = SuperLock.wrapper(attr_value)
            new_attr[attr_name] = attr_value
        new_attr['super_lock'] = RLock()
        return type.__new__(cls, classname, bases, new_attr)


class SuperThreadSafeDatabase(Database):

    __metaclass__ = SuperLock

    def __init__(self, *args, **kwargs):
        super(SuperThreadSafeDatabase, self).__init__(*args, **kwargs)
