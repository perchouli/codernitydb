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

from CodernityDB import patch


class TestPatches:

    def test_lfu(self):

        from CodernityDB.lfu import lfu_cache

        assert lfu_cache.__name__ == 'lfu_cache'
        del lfu_cache

        from threading import RLock
        patch.patch_lfu(RLock)
        from CodernityDB.lfu import lfu_cache

        assert lfu_cache.__name__ != 'lfu_cache'
