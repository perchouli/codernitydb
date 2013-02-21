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

from setuptools import setup
import os


def get_meta(inc, name):
    import re
    return eval(re.search(r'(?:%s)\s*=\s*(.*)' % name, inc).group(1))


with open(os.path.join("CodernityDB", '__init__.py'), 'r') as _init:
    _init_d = _init.read()

__version__ = get_meta(_init_d, '__version__')
__license__ = get_meta(_init_d, '__license__')


with open('README') as f:
    L_DESCR = f.read()


keywords = ' '.join(('database', 'python', 'nosql', 'key-value', 'key/value', 'db'))

setup(name='CodernityDB',
      version=__version__,
      description="Pure python, fast, schema-less, NoSQL database",
      long_description=L_DESCR,
      keywords=keywords,
      author='Codernity',
      author_email='contact@codernity.com',
      url='http://labs.codernity.com/codernitydb',
      packages=['CodernityDB'],
      platforms='any',
      license=__license__,
      classifiers=[
      "License :: OSI Approved :: Apache Software License",
      "Programming Language :: Python",
      "Programming Language :: Python :: 2.6",
      "Programming Language :: Python :: 2.7",
      "Operating System :: OS Independent",
      "Topic :: Internet",
      "Topic :: Database",
      "Topic :: Software Development",
      "Intended Audience :: Developers",
      "Development Status :: 4 - Beta"])
