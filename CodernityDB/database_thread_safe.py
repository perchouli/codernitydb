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
from CodernityDB.database import PreconditionsException

cdb_environment['mode'] = "threads"
cdb_environment['rlock_obj'] = RLock


from database import Database


class ThreadSafeDatabase(Database):

    def __init__(self, path):
        super(ThreadSafeDatabase, self).__init__(path)
        self.main_lock = RLock()
        self.close_open_lock = RLock()
        self.indexes_locks = {}

    def initialize(self, *args, **kwargs):
        res = None
        try:
            self.close_open_lock.acquire()
            res = super(ThreadSafeDatabase, self).initialize(*args, **kwargs)
            for name in self.indexes_names.iterkeys():
                self.indexes_locks[name] = RLock()
        finally:
            self.close_open_lock.release()
        return res

    def open(self, *args, **kwargs):
        res = None
        try:
            self.close_open_lock.acquire()
            res = super(ThreadSafeDatabase, self).open(*args, **kwargs)
            for name in self.indexes_names.iterkeys():
                self.indexes_locks[name] = RLock()
        finally:
            self.close_open_lock.release()
        return res

    def create(self, *args, **kwargs):
        res = None
        try:
            self.close_open_lock.acquire()
            res = super(ThreadSafeDatabase, self).create(*args, **kwargs)
            for name in self.indexes_names.iterkeys():
                self.indexes_locks[name] = RLock()
        finally:
            self.close_open_lock.release()
        return res

    def close(self):
        res = None
        try:
            self.close_open_lock.acquire()
            res = super(ThreadSafeDatabase, self).close()
        finally:
            self.close_open_lock.release()
        return res

    def destroy(self):
        res = None
        try:
            self.close_open_lock.acquire()
            res = super(ThreadSafeDatabase, self).destroy()
        finally:
            self.close_open_lock.release()
            return res

    def add_index(self, *args, **kwargs):
        res = None
        try:
            self.main_lock.acquire()
            res = super(ThreadSafeDatabase, self).add_index(*args, **kwargs)
        finally:
            if self.opened:
                self.indexes_locks[res] = RLock()
            self.main_lock.release()
        return res

    def edit_index(self, *args, **kwargs):
        res = None
        try:
            self.main_lock.acquire()
            res = super(ThreadSafeDatabase, self).edit_index(*args, **kwargs)
        finally:
            if self.opened:
                self.indexes_locks[res] = RLock()
            self.main_lock.release()
        return res

    def set_indexes(self, *args, **kwargs):
        try:
            self.main_lock.acquire()
            super(ThreadSafeDatabase, self).set_indexes(*args, **kwargs)
        finally:
            self.main_lock.release()

    def _read_indexes(self, *args, **kwargs):
        try:
            self.main_lock.acquire()
            super(ThreadSafeDatabase, self)._read_indexes(*args, **kwargs)
        finally:
            self.main_lock.release()

    def _insert_id_index(self, *args, **kwargs):
        lock = self.indexes_locks['id']
        try:
            lock.acquire()
            res = super(ThreadSafeDatabase,
                        self)._insert_id_index(*args, **kwargs)
        finally:
            lock.release()
        return res

    def _update_id_index(self, *args, **kwargs):
        lock = self.indexes_locks['id']
        res = None
        try:
            lock.acquire()
            res = super(
                ThreadSafeDatabase, self)._update_id_index(*args, **kwargs)
        finally:
            lock.release()
        return res

    def _delete_id_index(self, *args, **kwargs):
        lock = self.indexes_locks['id']
        res = None
        try:
            lock.acquire()
            res = super(
                ThreadSafeDatabase, self)._delete_id_index(*args, **kwargs)
        finally:
            lock.release()
        return res

    def _single_update_index(self, index, *args, **kwargs):
        lock = self.indexes_locks[index.name]
        try:
            lock.acquire()
            super(ThreadSafeDatabase,
                  self)._single_update_index(index, *args, **kwargs)
        finally:
            lock.release()

    def _single_insert_index(self, index, *args, **kwargs):
        lock = self.indexes_locks[index.name]
        try:
            lock.acquire()
            super(ThreadSafeDatabase,
                  self)._single_insert_index(index, *args, **kwargs)
        finally:
            lock.release()

    def _single_delete_index(self, index, *args, **kwargs):
        lock = self.indexes_locks[index.name]
        try:
            lock.acquire()
            super(ThreadSafeDatabase,
                  self)._single_delete_index(index, *args, **kwargs)
        finally:
            lock.release()

    def _single_compact_index(self, index, *args, **kwargs):
        lock = self.indexes_locks[index.name]
        try:
            lock.acquire()
            super(ThreadSafeDatabase, self).compact_index(
                index, *args, **kwargs)
        finally:
            lock.release()

    def reindex_index(self, index, *args, **kwargs):
        if isinstance(index, basestring):
            if not index in self.indexes_names:
                raise PreconditionsException("No index named %s" % index)
            index = self.indexes_names[index]
        key = index.name + "reind"
        self.main_lock.acquire()
        if key in self.indexes_locks:
            lock = self.indexes_locks[index.name + "reind"]
        else:
            self.indexes_locks[index.name + "reind"] = RLock()
            lock = self.indexes_locks[index.name + "reind"]
        self.main_lock.release()
        try:
            lock.acquire()
            super(ThreadSafeDatabase, self).reindex_index(
                index, *args, **kwargs)
        finally:
            lock.release()

    def destroy_index(self, index, *args, **kwargs):
        if isinstance(index, basestring):
            if not index in self.indexes_names:
                raise PreconditionsException("No index named %s" % index)
            index = self.indexes_names[index]
        lock = self.indexes_locks[index.name]
        try:
            lock.acquire()
            super(ThreadSafeDatabase, self).destroy_index(
                index, *args, **kwargs)
        finally:
            lock.release()

    def flush(self):
        try:
            self.main_lock.acquire()
            super(ThreadSafeDatabase, self).flush()
        finally:
            self.main_lock.release()

    def fsync(self):
        try:
            self.main_lock.acquire()
            super(ThreadSafeDatabase, self).fsync()
        finally:
            self.main_lock.release()
