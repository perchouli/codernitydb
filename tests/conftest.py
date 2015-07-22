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


def pytest_addoption(parser):
    parser.addoption("--inserts", type="int",
                     help="how many inserts", default=2000)

    parser.addoption("--operations", type="int",
                     help="how many operations", default=5000)


def pytest_generate_tests(metafunc):
    if "inserts" in metafunc.funcargnames:
        metafunc.addcall(funcargs=dict(inserts=metafunc.config.option.inserts))
    if "operations" in metafunc.funcargnames:
        metafunc.addcall(funcargs=dict(operations=metafunc.config.
                                       option.operations))
