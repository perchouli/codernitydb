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


# This test are for checking Parser only, using simple exec isn't exactly what database does with simple index code,
# they are used only because they allow handy check of correctness of
# generated code

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


def pytest_funcarg__p(request):
    p = Parser()
    return p


def simple_compare(s, args_mkv, args_mk):
    p = Parser()
    n = "_" + uuid.uuid4().hex
    exec p.parse(s, n)[1] in globals()

    # n2 = p.parse(s)
    # exec n2 in globals()
    # n2 = n2.splitlines()[1][2:]

    for a, o in args_mkv:
        assert globals()[n]('test', 'test').make_key_value(a) == o
        # assert globals()[n2]('test','test').make_key_value(a) == o

    for a, o in args_mk:
        assert globals()[n]('test', 'test').make_key(a) == o
        # assert globals()[n2]('test','test').make_key(a) == o

# special comparator for comparing lists


def simple_compare_without_order(s, args_mkv, args_mk):
    p = Parser()
    n = "_" + uuid.uuid4().hex
    exec p.parse(s, n)[1] in globals()

    for a, o in args_mkv:
        a, b = globals()[n]('test', 'test').make_key_value(a)
        if a is not None:
            a = sorted(a)
        if b is not None:
            b = sorted(b)
        assert (a, b) == o

    for a, o in args_mk:
        assert globals()[n]('test', 'test').make_key(a) == o


class TestIndexCreatorRightInput:

    def test_2nd_arg_of_mkv(self, p):
        s = """
        name = "s"
        type = 'HashIndex'
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a),{'a':a}
        make_key:
        md5(a)"""

        s2 = """
        name = s
        type =       HashIndex
        key_format =     I
        hash_lim = 1
        make_key:
        md5(a)
        make_key_value:
        a,{'a':a}
        """
        s3 = """
        name = s3
        type =       HashIndex
        key_format =     I
        hash_lim = 1
        make_key:
        md5(a)
        make_key_value:
        a,a"""

        s4 = """
        name = s4
        type =       HashIndex
        key_format =     I
        hash_lim = 1
        make_key:
        md5(a)
        make_key_value:
        md5(b),a"""

        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), {'a': 'a'})),
                           ({'a': 'qwerty'}, (
                            md5('qwerty').digest(), {'a': 'qwerty'}))
                           ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])
        simple_compare(s2, [({'a': 'a'}, ('a', {'a': 'a'})),
                            ({'a': 'qwerty'}, ('qwerty', {'a': 'qwerty'}))
                            ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

        simple_compare(s3, [({'a': 'a', 'b': 'b'}, ('a', 'a')),
                            ({'a': 'qwerty', 'b': 'b'}, ('qwerty', 'qwerty'))
                            ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

        simple_compare(s4, [({'a': 'a', 'b': 'b'}, (md5('b').digest(), 'a')),
                            ({'a': 'qwerty', 'b':
                              'b'}, (md5('b').digest(), 'qwerty'))
                            ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

    def test_name_or_type_as_string(self):
        s = """
        name = "s"
        type = 'HashIndex'
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a),None
        make_key:
        md5(a)"""

        s2 = """
        name = \"\"\"s\"\"\"
        type =       'HashIndex'
        key_format =     I
        hash_lim = 1
        make_key:
        md5(a)
        make_key_value:
        md5(a),None
        """
        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), None)),
                           ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                           ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

        simple_compare(s2, [({'a': 'a'}, (md5('a').digest(), None)),
                            ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                            ],
                       [
                       ({'a': 'e'}, (md5('e').digest())),
                       ({'a': 'qwerty'}, (md5('qwerty').digest()))
                       ])

    def test_fliped_definitions(self):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a),None
        make_key:
        md5(a)"""

        s2 = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1
        make_key:
        md5(a)
        make_key_value:
        md5(a),None
        """
        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), None)),
                           ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                           ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

        simple_compare(s2, [({'a': 'a'}, (md5('a').digest(), None)),
                            ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                            ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

    def test_None_equivalents(self):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        null
        make_key:
        none"""
        simple_compare(s, [({'a': 'a'}, None),
                           ({'a': 'qwerty'}, None)
                           ],
                       [
                       ({'a': 'e'}, None),
                       ({'a': 'qwerty'}, None)
                       ])

    def test_assign_prop_equivalents(self):
        s = """
        name : s
        type : HashIndex
        key_format :     I
        hash_lim : 1

        make_key_value:
        md5(a),None
        make_key:
        md5(a)"""

        s2 = """
        name:s
        type:HashIndex
        key_format:'I'
        hash_lim:1
        make_key:
        md5(a)
        make_key_value:
        md5(a),None
        """
        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), None)),
                           ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                           ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])
        simple_compare(s2, [({'a': 'a'}, (md5('a').digest(), None)),
                            ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                            ],
                       [
                       ({'a': 'e'}, md5('e').digest()),
                       ({'a': 'qwerty'}, md5('qwerty').digest())
                       ])

    def test_automatic_generated_class_name(self, p):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a),None
        make_key:
        a"""

        _, r = p.parse(s)
        rs = r.splitlines()
        assert rs[0] == "# s"
        assert rs[1][2:] == rs[2][6:39]

        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), None)),
                           ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                           ],
                       [
                       ({'a': 'e'}, 'e')
                       ])

    def test_md5(self):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a),None
        make_key:
        a != 'e' : md5(a)
        a"""

        simple_compare(s, [({'a': 'a'}, (md5('a').digest(), None)),
                           ({'a': 'qwerty'}, (md5('qwerty').digest(), None))
                           ],
                       [
                       ({'a': 'e'}, 'e'),
                       ({'a': 'eeee'}, md5('eeee').digest())
                       ])

        s2 = """
        name = s2
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        md5(a+'aaa')+'a',None
        make_key:
        a != 'e' : md5(a)+a
        a"""

        simple_compare(s2, [({'a': 'a'}, (md5('aaaa').digest() + 'a', None)),
                            ({'a': 'qwerty'}, (
                             md5('qwertyaaa').digest() + 'a', None))
                            ],
                       [
                       ({'a': 'e'}, 'e'),
                       ({'a': 'eeee'}, md5('eeee').digest() + 'eeee')
                       ])

    def test_no_conditionals_return_dicts(self):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        data

        make_key:
        key"""

        simple_compare(
            s, [({'test': 'a', 'b': 'e'}, {'test': 'a', 'b': 'e'}),
                ({}, {})
                ],
            [(
             {'test': 'a', 'b': 'e'}, {'test': 'a', 'b': 'e'}),
             ({}, {})
             ])

        s2 = """
        name = s2
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        {'t' : 'a','e' : 1}
        make_key:
        key"""

        simple_compare(s2, [({}, {'t': 'a', 'e': 1})
                            ],
                       [({'e': 2, 't': 'b'}, {'e': 2, 't': 'b'})
                        ])

        s3 = """
        type = HashIndex
        name = s3
        key_format =     I
        hash_lim = 1

        make_key_value:
        "::::::",None
        make_key:
        "::::::":'a'"""
        simple_compare(s3, [({}, ("::::::", None))
                            ],
                       [({}, "a")
                        ])

    def test_simple_conditionals(self):
        s = """
        name = s
        type = HashIndex
        key_format =     I
        hash_lim = 1

        make_key_value:
        test > 5 :  1,None
        0,None

        make_key:
        test < 1: 1
        key"""

        simple_compare(s, [({'test': 4}, (0, None)),
                      ({'test': 6}, (1, None))
        ],
            [({'test': 1}, {'test': 1}),
             ({'test': 0}, 1)
             ])

        s2 = """
        type = HashIndex
        name = s2
        key_format =     I
        hash_lim = 1

        make_key_value:
        test > 5 : 1,None
        test-1 == 3 : 3,None
        0,None

        make_key:
        test < 1: 1
        key"""

        simple_compare(s2, [({'test': 4}, (3, None)),
                      ({'test': 6}, (1, None)),
            ({'test': 3}, (0, None))
        ],
            [({'test': 1}, {'test': 1}),
             ({'test': 0}, 1)
             ])
        # unfortunately tokenize seems to not parse - sign in right way (it doesn't set its type to token.MINUS :/)
        # added a hack to indexcreator
        s3 = """
        type = HashIndex
        name = s3
        key_format =     I
        hash_lim = 1

        make_key_value:
        (a > 5 or a < -1) and (b > 15 and b > 16): 1,None
        (a-1)*5 == 25 : 3,None
        0,None

        make_key:
        a-1-1-1-1-9*4 > 0: 1
        key"""

        simple_compare(s3, [({'a': 6, 'b': 17}, (1, None)),
                      ({'a': 6, 'b': 3}, (3, None)),
            ({'a': 3, 'b': 17}, (0, None))
        ],
            [({'a': 41}, 1),
             ({'a': 40}, {'a': 40})
             ])

    def test_or_and_in_conditionals(self):
        s = """
        type = HashIndex
        name = s
        key_format =     I
        hash_lim = 1

        make_key_value:
        a > 5 or b<3 and 5==3 : 8,None
        a > 5 or b<3 : 7,None
        a in [4,3,5]: 'c',None
        b,None

        make_key:
        key<1 and key>-3: 'a'
        key in [1,3]: 'c'
        'b'"""

        simple_compare(s, [({'a': 5, 'b': 2}, (7, None)),
                      ({'a': 6, 'b': 4}, (8, None)),
            ({'a': 6, 'b': 2}, (8, None)),
            ({'a': 1, 'b': 2}, (7, None)),
            ({'a': 1, 'b': 4}, (4, None)),
            ({'b': 3, 'a': 4}, ('c', None))
        ],
            [(-2, 'a'),
             (2, 'b'),
             (1, 'c')
             ])

        s2 = """
        type = HashIndex
        key_format =     I
        hash_lim = 1
        name = s2

        make_key_value:
        a && b: 1 ,None
        0,None

        make_key:
        key & 6
        """

        simple_compare(s2, [({'a': True, 'b': True}, (1, None)),
                      ({'a': False, 'b': True}, (0, None)),
            ({'a': False, 'b': False}, (0, None)),
            ({'a': True, 'b': False}, (0, None))
        ],
            [(5, 4)
             ])

        s3 = """
        type = HashIndex
        key_format =     I
        name = s3
        hash_lim = 1

        make_key_value:
        a || b: 1,None
        0,None

        make_key:
        key | 6
        """

        simple_compare(s3, [({'a': True, 'b': True}, (1, None)),
                      ({'a': False, 'b': True}, (1, None)),
            ({'a': False, 'b': False}, (0, None)),
            ({'a': True, 'b': False}, (1, None)),
        ],
            [(5, 7)
             ])

    def test_props(self, p):
        s = """
        type = HashIndex
        key_format =     32s
        name = s
        hash_lim = 1
        make_key_value:
        0,None
        make_key:
        0"""

        _, n = p.parse(s)
        exec n in globals()
        n = n.splitlines()[1][2:]
        assert globals()[n]('test', 'test').hash_lim == 1
        assert globals()[n]('test', 'test').entry_struct.format[4:7] == '32s'

        s2 = """
        type = HashIndex
        key_format=32s
        hash_lim=30 * (15+5) - 0
        name = s2
        make_key_value:
        0,None
        make_key:
        0"""
        _, n2 = p.parse(s2)
        exec n2 in globals()
        n2 = n2.splitlines()[1][2:]
        assert globals()[n2]('test', 'test').hash_lim == 600
        assert globals()[n2]('test', 'test').entry_struct.format[4:7] == '32s'

        s3 = """
        name = s3
        type = HashIndex
        key_format =     'I'
        hash_lim = 2/2
        make_key_value:
        0,None
        make_key:
        0"""
        _, n3 = p.parse(s3)
        exec n3 in globals()
        n3 = n3.splitlines()[1][2:]
        assert globals()[n3]('test', 'test').hash_lim == 1
        assert globals()[n3]('test', 'test').entry_struct.format[4] == 'I'

        s4 = """
        name = s4
        type = TreeBasedIndex
        key_format =     I
        make_key_value:
        0,None
        make_key:
        0"""
        _, n4 = p.parse(s4)
        exec n4 in globals()
        n4 = n4.splitlines()[1][2:]
        assert globals()[n4]('test', 'test').key_format == 'I'

    def test_no_make_key(self):
        s = """
        name = s
        type = HashIndex
        key_format =     32s
        hash_lim = 1
        make_key_value:
        0,None
        """
        simple_compare(s, [({'a': 3, 'b': 4}, (0, None))
                           ],
                       [(5, 5),
                      ("aaa", "aaa"),
                           ({'a': 3, 'b': 4}, {'a': 3, 'b': 4})
                       ])

    def test_functions(self, p):
        s = """
        name = s
        type = HashIndex
        key_format =     32s
        hash_lim = 1
        make_key_value:
        md5(str(md5(a))),None
        """

        simple_compare(s, [({'a': 'a', 'b': 4}, (md5(str(md5('a').digest())).digest(), None))
                           ],
                       [(5, 5),
                      ("aaa", "aaa"),
                           ({'a': 3, 'b': 4}, {'a': 3, 'b': 4})
                       ])

        s2 = """
        name = s2
        type = HashIndex
        key_format =     32s
        hash_lim = 1
        make_key_value:
        b == 1: md5(str(b)),None
        len(a) > 2: md5(a),None
        a,None
        """

        simple_compare(
            s2, [({'a': 'a', 'b': 1}, (md5(str(1)).digest(), None)),
                 ({'a': 'a', 'b': 4}, ('a', None)),
                 ({'a': 'eee', 'b':
                   1}, (md5(str(1)).digest(), None)),
                 ({'a': 'eee', 'b':
                   3}, (md5('eee').digest(), None))
                         ],
            [(5, 5),
             ("aaa", "aaa"),
             ({'a': 3, 'b': 4}, {'a': 3, 'b': 4})
             ])

        s3 = """
        name = s
        type = HashIndex
        key_format =     32s
        hash_lim = 1
        make_key_value:
        fix_r(a,5),None
        """

        simple_compare(s3, [({'a': 'aaaa', 'b': 4}, ('_aaaa', None)),
                      ({'a': 'aaaaaa', 'b': 4}, ('aaaaa', None)),
            ({'a': '', 'b': 4}, ('_____', None)),
            ({'a': 'aaaaa', 'b': 4}, ('aaaaa', None))
        ],
            [(5, 5),
             ("aaa", "aaa"),
             ({'a': 3, 'b': 4}, {'a': 3, 'b': 4})
             ])

    def test_enclosures(self, p):

        s = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=="'aa":1,None

        make_key:
        1"""

        simple_compare(s, [({'a': "'aa", 'b': 4}, (1, None))
                           ],
                       [(5, 1)
                        ])

        s1 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=="a)a":1,None

        make_key:
        1"""

        simple_compare(s1, [({'a': "a)a", 'b': 4}, (1, None)),
                      ({'a': "a}a", 'b': 4}, None)
        ],
            [(5, 1)
             ])

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        2==(a+({'a' : 1}['a'])):1,None

        make_key:
        1"""

        simple_compare(s2, [({'a': 2, 'b': 4}, None),
                      ({'a': 1, 'b': 4}, (1, None))
        ],
            [(5, 1)
             ])


class TestIndexCreatorExceptions():

    def test_no_properity(self, p):
        s = """
        make_key_value:
        a
        b
        c
        """

        with raises(IndexCreatorValueException):
            p.parse(s)

    def test_wrong_properity_assingment(self, p):
        s = """
        name = eeee
        type = HashIndex
        aa aa a222a$ 2 * "eee" aa bb
        cc 33 1
        make_key_value:
        a
        """

        s2 = """
        name  eeee
        type = HashIndex
        make_key_value:
        a
        """

        s3 = """
        name = sss
        type = 2
        make_key_value:
        a
        """

        s4 = """
        name = sss
        type = HashIndex,
        make_key_value:
        a
        """

        s5 = """
        name = sss
        type = HashIndex
        a : a, b
        make_key_value:
        a
        """

        s6 = """
        name = sss
        type = HashIndex

        2a : a
        make_key_value:
        a
        """

        s7 = """
        name = sss
        type = HashIndex

        22 : a
        make_key_value:
        a
        """

        with raises(IndexCreatorValueException):
            p.parse(s)
        with raises(IndexCreatorValueException):
            p.parse(s2)
        with raises(IndexCreatorValueException):
            p.parse(s3)
        with raises(IndexCreatorValueException):
            p.parse(s4)
        with raises(IndexCreatorValueException):
            p.parse(s5)
        with raises(IndexCreatorValueException):
            p.parse(s6)
        with raises(IndexCreatorValueException):
            p.parse(s7)

    def test_empty_function(self, p):

        s = """
        type = HashIndex
        name = s
        key_format =     32s
        hash_lim = 1
        make_key_value:

        make_key:
        """
        s2 = """
        type = HashIndex
        name = s2
        key_format =     32s
        hash_lim = 1
        make_key_value:
        0
        make_key:
        """
        s3 = """
        type = HashIndex
        name = s3
        key_format =     32s
        hash_lim = 1
        make_key_value:

        make_key:
        0
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s, 'TestIndex')
        with raises(IndexCreatorFunctionException):
            p.parse(s2, 'TestIndex')
        with raises(IndexCreatorFunctionException):
            p.parse(s3, 'TestIndex')

    def test_no_type(self, p):
        s = """
        key_format =     32s
        name = s
        hash_lim = 1
        make_key_value:
        0,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

    def test_no_make_key_value(self, p):
        s = """
        name = s
        key_format =     32s
        hash_lim = 1
        make_key:
        0
        """
        s2 = """
        key_format =     32s
        name = s2
        hash_lim = 1
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s, 'TestIndex')
        with raises(IndexCreatorFunctionException):
            p.parse(s2, 'TestIndex')

    def test_no_name(self, p):
        s = """
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        0,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

    def test_no_2nd_arg_and_1st_isnt_enough_for_mkv(self, p):
        s = """
        name = s
        key_format =     16s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

        s2 = """
        name = s
        key_format =     16s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        "aaa"
        """
        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     16s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        md5(a)
        """
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

    def test_wrong_2nd_ret_arg_of_mkv(self, p):
        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        md5(a),a
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')
        s2 = """
        name = s2
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        md5(a),"aaa"
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s2)
        s3 = """
        name = s3
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        md5(a),3
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s3)

    def test_empty_return_with_condition(self, p):
        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3:
        md5(a),None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3:
        md5(a),None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key:

        a > 3:
        0
        make_key_value:
        md5(a),a
        """
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

        s4 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        md5(a),None
        make_key:
        a>2:"""
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

    def test_wrong_nr_of_args_to_ret(self, p):
        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3: a,None,
        md5(a),None
        make_key:
        0
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s, 'TestIndex')

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3: a,None
        md5(a),None,
        make_key:
        0
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3: a,None
        md5(a),None
        make_key:
        0,
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s3, 'TestIndex')

        s4 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a > 3: a,None
        md5(a),None
        make_key:
        ,
        """
        with raises(IndexCreatorFunctionException):
            p.parse(s4, 'TestIndex')

        s5 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1,
        md5(a),None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s5, 'TestIndex')

    def test_wrong_comp_operators(self, p):

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        'a'==1:1,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

        s4 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1>='a':1,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s4, 'TestIndex')

        s5 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        'a'<="b":1,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s5, 'TestIndex')

        s6 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        <=1:1,None
        make_key:
        0
        """
        with raises(IndexCreatorValueException):
            p.parse(s6, 'TestIndex')

        s7 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        1=="""
        with raises(IndexCreatorValueException):
            p.parse(s7, 'TestIndex')

        s8 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1

        make_key:
        1==
        make_key_value:
        1<=2:1,None
        """
        with raises(IndexCreatorValueException):
            p.parse(s8, 'TestIndex')

    def test_no_operator_between(self, p):
        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        a b"""
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        a 'b'"""
        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        'a' b"""
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

        s4 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        a>=1:a b,None
        make_key:
        a + b"""
        with raises(IndexCreatorValueException):
            p.parse(s4, 'TestIndex')

        s5 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        'a' 'b'"""
        with raises(IndexCreatorValueException):
            p.parse(s5, 'TestIndex')

        s6 = """
        name = s
        key_format =    aa ee
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        1"""
        with raises(IndexCreatorValueException):
            p.parse(s6, 'TestIndex')

    def test_too_much_colons(self, p):
        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1:1,None
        make_key:
        1"""
        with raises(IndexCreatorFunctionException):
            p.parse(s, 'TestIndex')
        s2 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        a>1::"""
        with raises(IndexCreatorFunctionException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     : 32s
        type = HashIndex
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

    def test_name_or_type_set_twice(self, p):

        s = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        type = HashIndex
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""
        with raises(IndexCreatorValueException):
            p.parse(s, 'TestIndex')

        s2 = """
        type = TreeBasedIndex
        name = s
        key_format =     : 32s
        type = HashIndex
        hash_lim = 1

        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""
        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')
        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        hash_lim = 1
        name: a
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')
        s4 = """
        name = s
        key_format =     32s
        type = HashIndex
        name = s
        hash_lim = 1
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""
        with raises(IndexCreatorValueException):
            p.parse(s4, 'TestIndex')

    def test_wrong_enclosures(self, p):
        s1 = """
        name = s
        key_format =     32s
        type = HashIndex
        make_key_value:
        1<=(2)):1,None
        make_key:
        a>1:1"""

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=="aa("):1,None

        make_key:
        1"""

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=={"aa"):1,None

        make_key:
        1"""

        s4 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=={"aa":1,None

        make_key:
        1"""

        s5 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a==""aa":1,None

        make_key:
        1"""

        s7 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a==":1,None

        make_key:
        1"""

        s8 = """
        name = s
        key_format =     32s
        type = HashIndex

        make_key_value:

        a=="aa:1,None

        make_key:
        1"""

        with raises(IndexCreatorValueException):
            p.parse(s1, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s4, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s5, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s7, 'TestIndex')
        with raises(IndexCreatorValueException):
            p.parse(s8, 'TestIndex')

    def test_excessive_return(self, p):
        s1 = """
        name = s
        key_format =     32s
        type = HashIndex
        make_key_value:
        1<=2:1,None
        1,None
        2,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorFunctionException):
            p.parse(s1, 'TestIndex')

        s2 = """
        name = s
        key_format =     32s
        type = HashIndex
        make_key_value:
        1<=2:1,None
        1,None
        1>3:2,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorFunctionException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     32s
        type = HashIndex
        make_key_value:
        1<=2:1,None
        1>3:2,None
        make_key:
        1
        2"""

        with raises(IndexCreatorFunctionException):
            p.parse(s3, 'TestIndex')

    def test_wrong_properities(self, p):
        s1 = """
        name = s
        key_format =     32s
        node_capacity=100
        type = HashIndex
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorValueException):
            p.parse(s1, 'TestIndex')

        s2 = """
        name = s
        key_format =     32s
        hash_lim=3
        type = TreeBasedIndex
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorValueException):
            p.parse(s2, 'TestIndex')

        s3 = """
        name = s
        key_format =     32s
        aaa=100
        type = HashIndex
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorValueException):
            p.parse(s3, 'TestIndex')

    def test_duplicate_props(self, p):
        s1 = """
        name = s
        key_format =     32s
        key_format: 8
        type = HashIndex
        make_key_value:
        1<=2:1,None
        make_key:
        a>1:1"""

        with raises(IndexCreatorValueException):
            p.parse(s1, 'TestIndex')


class TestMultiIndexCreator:
    def test_prefix(self):
        s = """
        name = s
        type = MultiTreeBasedIndex
        make_key_value:
        prefix(a,2,3,3),None
        """

        simple_compare_without_order(
            s, [({'a': 'abcd', 'b': 4}, (sorted(["_ab", "abc"]), None)),
                ({'a': 'ab', 'b':
                  4}, (sorted(["_ab"]), None)),
                ({'a': 'a', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': '', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': 'abc', 'b': 4},
                 (sorted(["_ab", "abc"]), None))
                ],
            [
            ])
        s2 = """
        name = s2
        type = MultiTreeBasedIndex
        make_key_value:
        prefix(a,0,20,5),None
        """

        simple_compare_without_order(
            s2, [(
                {'a': 'abcd', 'b': 4}, (sorted(["___ab", "__abc", "_abcd", "____a"]), None)),
                ({'a': 'ab', 'b': 4}, (sorted(
                                       ["___ab", "____a"]), None)),
                ({'a': 'a', 'b': 'abcd'},
                 (sorted(["____a"]), None)),
                ({'a': '', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': 'abc', 'b': 4}, (sorted(["___ab",
                 "__abc", "____a"]), None))
            ],
            [
            ])

    def test_suffix(self):
        s = """
        name = s
        type = MultiTreeBasedIndex
        make_key_value:
        suffix(a,2,3,4),None
        """

        simple_compare_without_order(
            s, [({'a': 'abcd', 'b': 4}, (sorted(["__cd", "_bcd"]), None)),
                ({'a': 'ab', 'b':
                  4}, (sorted(["__ab"]), None)),
                ({'a': 'a', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': '', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': 'abc', 'b': 4},
                 (sorted(["__bc", "_abc"]), None))
                ],
            [
            ])

        s2 = """
        name = s2
        type = MultiTreeBasedIndex
        make_key_value:
        suffix(a,0,20,5),None
        """

        simple_compare_without_order(
            s2, [(
                {'a': 'abcd', 'b': 4}, (sorted(["___cd", "__bcd", "_abcd", "____d"]), None)),
                ({'a': 'ab', 'b': 4}, (sorted(
                                       ["___ab", "____b"]), None)),
                ({'a': 'a', 'b': 'abcd'},
                 (sorted(["____a"]), None)),
                ({'a': '', 'b':
                  'abcd'}, (sorted([]), None)),
                ({'a': 'abc', 'b': 4}, (sorted(["___bc",
                 "__abc", "____c"]), None))
            ],
            [
            ])
