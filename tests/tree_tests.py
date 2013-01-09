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

from CodernityDB.database import RecordDeleted, RecordNotFound

from CodernityDB.hash_index import UniqueHashIndex

from CodernityDB.tree_index import TreeBasedIndex

from CodernityDB.debug_stuff import database_step_by_step

import pytest
import os
import random


class SimpleTreeIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 13
        kwargs['key_format'] = 'I'
        super(SimpleTreeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get('a')
        if a_val is not None:
            return a_val, None
        return None

    def make_key(self, key):
        return key


class CustomTreeIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 13
        kwargs['key_format'] = 'I'
        super(CustomTreeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        c = data.get('cstm')
        if c is None:
            return None
        k = c - c % 10
        return k, dict(cstm=c)

    def make_key(self, key):
        return key - key % 10


class EvenCapacityTreeIndex(TreeBasedIndex):

    def __init__(self, *args, **kwargs):
        kwargs['node_capacity'] = 8
        kwargs['key_format'] = 'I'
        super(EvenCapacityTreeIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get('a')
        if a_val is not None:
            return a_val, {}
        return None

    def make_key(self, key):
        return key


def sort_by_key(list):

    def _comp(a, b):
        return cmp(a['a'], b['a'])
    list.sort(_comp)


def check_if_keys_match(gen, list):
    for el in list:
        cur = next(gen)
        assert cur['key'] == el['a']
    return True


class TreeIndexTests:

    def random_database_usage(self, db, operations, inserted={}, updated={}, with_get=True, max_key_value=1000):

        def _insert():
            doc = dict(i=random.randint(0, 1000))
            doc['a'] = random.randint(1, 200)
            doc['cstm'] = random.randint(1, 200)
            db.insert(doc)
            inserted[doc['_id']] = doc
            return True

        def _update():
            vals = inserted.values()
            if not vals:
                return False
            doc = random.choice(vals)
            a = random.randint(0, max_key_value)
            doc['upd'] = a
            doc['a'] = random.randint(1, 200)
            doc['cstm'] = random.randint(1, 200)
            db.update(doc)
            inserted[doc['_id']] = doc
            assert db.get('id', doc['_id'])['upd'] == a
            updated[doc['_id']] = doc
            return True

        def _delete():
            vals = inserted.values()
            if not vals:
                return False
            doc = random.choice(vals)
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
            got = db.get('tree', doc['a'])
            assert got['key'] == doc['a']
            return True

        if with_get:
            fcts = (_insert,) * 20 + (_get,) * 10 + (_update,
                                                     ) * 10 + (_delete,) * 5
        else:
            fcts = (_insert,) * 20 + (_update,) * 10 + (_delete,) * 5

        for i in xrange(operations):
            f = random.choice(fcts)
            f()

    def test_first_insert(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        a = dict(a=1)
        db.insert(a)
        test = db.get('tree', a['a'])
        assert test['key'] == a['a']
        db.close()

    def test_update_leaf_new_key_is_last_el(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        for i in range(2):
            a = dict(a=i)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_update_leaf(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = range(3)
        for i in range(3):
            a = dict(a=key_values.pop())
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_new_root_new_key_in_first_half(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = range(6)
        for i in range(6):
            a = dict(a=key_values.pop())
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_many_equal_keys_nk1h(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [2, 5, 5, 5, 5, 1]
        for i in key_values:
            a = dict(a=i)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_many_equal_keys_nk2h(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [1, 2, 2, 2, 5, 4]
        for i in key_values:
            a = dict(a=i)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_many_equal_keys_nklast(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [1, 2, 2, 2, 4, 5]
        for i in key_values:
            a = dict(a=i)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_new_root_new_key_is_last_el(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        for el in range(6):
            a = dict(a=el)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_leaf_with_new_root_new_key_in_second_half(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(0, 9, 2)]
        key_values += [7]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_update_node_new_key_is_last_el(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = range(9)
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_update_node(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(2, 19, 2)]
        key_values += [9, 11, 13, 0, 1, 5]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_new_root_nk_last(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(21)]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_new_root_2nd_half(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(0, 39, 2)]
        key_values += [25, 27, 29]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_new_key_to_root(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(0, 39, 2)]
        key_values += [13, 15, 17]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_new_root_1st_half(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(0, 39, 2)]
        key_values += [7, 9, 11]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_new_root_nk_first(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(0, 39, 2)]
        key_values += [1, 3, 5]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_insert_key_to_parent(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(30)]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_split_node_and_parent(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(66)]
        for i in range(len(key_values)):
            a = dict(a=key_values[i])
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_random_inserts(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        database_step_by_step(db)
        key_values = range(10000)
        for x in (random.choice(key_values) for x in xrange(10000)):
            a = dict(a=x)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_random_inserts_many_equal_keys(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        database_step_by_step(db)
        key_values = range(5)
        for x in (random.choice(key_values) for x in xrange(1000)):
            a = dict(a=x)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_pre_defined_vals_1(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [110, 418, 15, 564, 985, 35, 615, 914, 392, 581, 369,
                      147, 31, 159, 287, 266, 253, 602, 258, 383, 277, 837, 440, 111,
                      365, 814, 449, 942, 231, 547, 610, 741, 362, 825, 445, 743, 772,
                      406, 540, 659, 816, 949, 793, 60, 41, 32, 694, 762, 698, 472, 252,
                      25, 132, 550, 536, 377, 538, 419, 109, 190, 579, 131, 953, 260,
                      322, 879, 184, 581, 458, 372, 7, 229, 829, 661, 735, 30, 43, 354,
                      18, 39, 608, 734, 918, 109, 759, 547, 764, 360, 371, 220, 229, 391,
                      645, 753, 327, 444, 873, 395, 836, 301, 216, 502, 862, 671, 532,
                      542, 112, 663, 979, 491, 63, 454, 191, 331, 784, 824, 872, 396,
                      93, 331, 583, 173, 808, 408, 304, 288, 460, 135, 599, 858, 991,
                      748, 215, 813, 524, 23, 88, 953, 291, 813, 25, 80, 843, 58, 916,
                      509, 645, 184, 463, 681, 400, 318, 157, 380, 234, 781, 566,
                      144, 496, 804, 524, 301, 429, 705, 548, 896, 326, 425, 47,
                      539, 208, 501, 141, 617, 618, 543, 152, 178, 87, 738, 917, 391,
                      314, 669, 337, 312, 297, 671, 331, 166, 553, 121, 159, 788, 33, 914,
                      453, 654, 599, 848, 692, 377, 562, 248, 778, 13, 483, 229, 125, 466,
                      717, 455, 676, 512, 589, 771, 794, 134, 544, 295, 595, 732, 204, 14,
                      291, 570, 842, 972, 111, 259, 632, 963, 691, 826, 884, 517, 436, 698,
                      898, 231, 502, 153, 34, 844, 186, 591, 179, 23, 689, 668, 667, 291,
                      713, 165, 355, 816, 165, 962, 343, 474, 513, 403, 426, 903, 942, 166,
                      118, 130, 84, 997, 696, 630, 86, 972, 44, 485, 205, 752, 169, 67, 747,
                      537, 253, 235, 983, 723, 516, 163, 165, 994, 837, 42, 447, 477, 170,
                      743, 474, 531, 720, 573, 710, 372, 682, 304, 789, 976, 106, 575, 451,
                      438, 324, 107, 990, 498, 505, 780, 640, 442, 161, 42, 49, 775, 901,
                      942, 237, 922, 446, 868, 176, 901, 949, 236, 871, 738, 525, 925,
                      499, 93, 632, 281, 531, 442, 212, 830, 507, 130, 40, 6, 275, 804, 813,
                      517, 277, 345, 319, 712, 787, 594, 774, 473, 455, 265, 700, 318, 82,
                      365, 446, 158, 999, 629, 106, 208, 424, 427, 712, 876, 224, 415, 245,
                      701, 804, 265, 115, 239, 885, 864, 579, 236, 184, 71, 599, 865, 954,
                      403, 538, 812, 240, 566, 638, 131, 686, 52, 767, 210, 565, 973, 855,
                      527, 91, 111, 917, 887, 734, 81, 438, 182, 403, 350, 102, 248, 152,
                      939, 306, 833, 603, 593, 472, 521, 790, 150, 628, 617, 344, 440, 28,
                      965, 772, 441, 21, 799, 709, 104, 539, 408, 971, 235, 811, 899, 345,
                      902, 237, 795, 902, 143, 863, 96, 219, 5, 77, 73, 586, 541, 105, 525,
                      400, 611, 828, 26, 693, 719, 490, 101, 209, 403, 973, 838, 354, 592,
                      691, 658, 430, 296, 419, 887, 90, 302, 429, 40, 943, 488, 61, 809,
                      633, 617, 551, 161, 356, 279, 529, 283, 979, 790, 46, 281, 982, 114,
                      283, 236, 773, 957, 795, 979, 597, 870, 349, 383, 945, 925, 649,
                      683, 605, 964, 688, 372, 965, 398, 562, 809, 349, 986, 110, 493,
                      467, 702, 406, 659, 66, 607, 549, 491, 408, 758, 648, 569, 989,
                      179, 749, 234, 844, 802, 940, 845, 715, 556, 428, 553, 680,
                      618, 245, 34, 54, 255, 273, 289, 335, 861, 773, 752, 657, 698,
                      116, 479, 891, 370, 91, 110, 86, 539, 345, 846, 653, 202, 578,
                      684, 243, 695, 87, 638, 242, 36, 424, 880, 446, 392, 681, 84,
                      97, 776, 158, 359, 66, 597, 252, 890, 199, 410, 114, 319,
                      204, 347, 464, 212, 468, 112, 304, 722, 975, 839, 958, 120, 673,
                      456, 220, 811, 706, 373, 144, 52, 960, 603, 291, 933, 768, 605, 868,
                      346, 268, 662, 808, 597, 719, 186, 623, 280, 228, 0, 259, 605, 90,
                      69, 624, 368, 378, 492, 941, 772, 542, 955, 240, 912, 396, 705, 353,
                      435, 594, 573, 169, 928, 948, 340, 957, 752, 291, 747, 361, 727, 313,
                      465, 441, 899, 795, 737, 301, 811, 899, 941, 220, 90, 196, 482, 609,
                      397, 608, 549, 127, 127, 644, 535, 145, 580, 407, 596, 361, 631, 323,
                      273, 160, 345, 55, 637, 748, 756, 440, 133, 570, 509, 171, 747, 228,
                      334, 696, 888, 170, 934, 206, 732, 168, 27, 903, 94, 634, 580, 48, 549,
                      974, 371, 58, 347, 901, 75, 854, 993, 106, 790, 938, 219, 143, 644,
                      366, 711, 86, 310, 847, 565, 555, 889, 812, 698, 434, 123, 628, 809,
                      368, 264, 874, 372, 824, 461, 11, 460, 984]
        for i in range(len(key_values)):
            a = dict(a=random.choice(key_values))
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_pre_defined_vals_2(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [93, 17, 6, 24, 8, 29, 90, 39, 73, 15, 26, 84, 72, 2,
                      93, 70, 69, 12, 12, 97, 61, 16, 68, 71, 70, 16, 62, 40, 44, 19, 32,
                      68, 85, 84, 55, 43, 16, 93, 49, 79, 17, 59, 75, 34, 3, 76, 48, 40,
                      62, 28, 63, 41, 3, 83, 36, 49, 36, 31, 26, 32, 41, 52, 1]
        for i in range(len(key_values)):
            a = dict(a=random.choice(key_values))
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_multiple_equal_keys_in_splitting_node(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [i for i in range(22)]
        for i in range(35):
            key_values += [18]

        for i in range(len(key_values)):
            a = dict(a=random.choice(key_values))
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.close()

    def test_simple_delete(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        a = dict(a=1)
        db.insert(a)
        test = db.get('tree', a['a'])
        assert test['key'] == a['a']
        db.delete(a)
        with pytest.raises(RecordNotFound):
            db.get('tree', 1)
        db.close()

    def test_insert_on_deleted(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        a = dict(a=1)
        db.insert(a)
        test = db.get('tree', a['a'])
        assert test['key'] == a['a']
        db.delete(a)
        a = dict(a=2)
        db.insert(a)
        test = db.get('tree', a['a'])
        assert test['key'] == a['a']
        db.close()

    def test_simple_update_same_key(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [2, 4, 9, 17, 20, 25]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        up_data = db.get('tree', 17, with_doc=True)
        up_data = up_data['doc']
        up_data['stor'] = 'a'
        db.update(up_data)
        db.close()

    def test_simple_update_with_new_key(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [2, 4, 9, 17, 20, 25]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        up_data = db.get('tree', 17, with_doc=True)
        up_data = up_data['doc']
        up_data['a'] = 5
        db.update(up_data)
        assert up_data['a'] == db.get('tree', up_data['a'])['key']
        db.close()

    def test_get_many(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 17, 20, 20, 20, 20, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', 20, limit=-1) == len(
            filter(lambda k: k == 20, key_values))
        db.close()

    def test_get_all_smaller(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1, end=20,
                        inclusive_end=False) == len(filter(lambda k: k < 20, key_values))
        db.close()

    def test_get_all_smaller_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1,
                        end=20) == len(filter(lambda k: k <= 20, key_values))
        db.close()

    def test_get_all_bigger(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1, start=20, inclusive_start=False) == len(filter(lambda k: k > 20, key_values))
        db.close()

    def test_get_all_bigger_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1,
                        start=20) == len(filter(lambda k: k >= 20, key_values))
        db.close()

    def test_get_all_between(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many,
                        'tree',
                        limit=-1,
                        start=4,
                        end=22,
                        inclusive_start=False,
                        inclusive_end=False) == len(filter(lambda k: k < 22 and k > 4, key_values))
        db.close()

    def test_get_all_between_not_existing_keys(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many,
                        'tree',
                        limit=-1,
                        start=3,
                        end=19,
                        inclusive_start=False,
                        inclusive_end=False) == len(filter(lambda k: k < 19 and k > 3, key_values))
        db.close()

    def test_get_all_between_not_existing_keys_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1, start=3,
                        end=19) == len(filter(lambda k: k < 19 and k > 3, key_values))
        db.close()

    def test_get_all_between_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 9, 20, 13, 20, 22, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.get_many, 'tree', limit=-1, start=13,
                        end=20) == len(filter(lambda k: k <= 20 and k >= 13, key_values))
        db.close()

    def test_get_all(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 17, 20, 20, 20, 20, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.all, 'tree') == db.count(db.all, 'id')
        db.close()

    def test_delete_with_math_doc_id_case(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        key_values = [20, 2, 4, 9, 17, 20, 20, 20, 20, 20, 25, 34, 20]
        for key in key_values:
            a = dict(a=key)
            db.insert(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        db.delete(a)
        with pytest.raises(RecordDeleted):
            db.delete(a)
        db.close()

    def test_insert_delete_middle_half_insert(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        inserted = []
        for key in xrange(1000):
            a = dict(a=key)
            db.insert(a)
            inserted.append(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.all, 'tree') == len(inserted)
        for rec in inserted[250:750]:
            db.delete(rec)
        inserted[250:750] = []
        assert db.count(db.all, 'tree') == len(inserted)
        for key in xrange(250, 750):
            a = dict(a=key)
            db.insert(a)
            inserted.append(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.all, 'tree') == len(inserted)
        db.close()

    def test_insert_delete_half_update_half(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        inserted = []
        for key in xrange(1000):
            a = dict(a=key)
            db.insert(a)
            inserted.append(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.all, 'tree') == len(inserted)
        for rec in inserted[::2]:
            db.delete(rec)
        assert db.count(db.all, 'tree') == len(inserted) / 2
        for rec in inserted[1::2]:
            a = random.randint(0, 1000)
            rec['upd'] = a
            db.update(rec)
        assert db.count(db.all, 'tree') == len(inserted) / 2
        db.close()

    def test_delete_compact(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        inserted = []
        for key in xrange(500):
            a = dict(a=key)
            db.insert(a)
            inserted.append(a)
            test = db.get('tree', a['a'])
            assert test['key'] == a['a']
        assert db.count(db.all, 'tree') == len(inserted)
        deleted = 0
        for rec in inserted[50:700:3]:
            db.delete(rec)
            deleted += 1
        assert db.count(db.all, 'tree') == len(inserted) - deleted
        db.compact()
        assert db.count(db.all, 'tree') == len(inserted) - deleted
        db.close()

    def test_tree_real_life_example_random(self, tmpdir, operations):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(inserted):
            assert len(inserted) == db.count(db.all, 'tree')
            assert len(inserted) == db.count(db.all, 'custom_tree')
            inserted_vals = inserted.values()
            sort_by_key(inserted_vals)
            assert check_if_keys_match(
                db.all('tree', with_storage=False), inserted_vals)

        self.random_database_usage(
            db, operations, inserted, updated)
        count_and_check(inserted)
        db.reindex()
        count_and_check(inserted)

        self.random_database_usage(
            db, operations, inserted, updated)

        count_and_check(inserted)
        db.close()

    def test_even_cap_tree_real_life_example_random(self, tmpdir, operations):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = EvenCapacityTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(inserted):
            assert len(inserted) == db.count(db.all, 'tree')
            inserted_vals = inserted.values()
            sort_by_key(inserted_vals)
            assert check_if_keys_match(
                db.all('tree', with_storage=False), inserted_vals)

        self.random_database_usage(
            db, operations, inserted, updated)
        count_and_check(inserted)
        db.reindex()
        count_and_check(inserted)

        self.random_database_usage(
            db, operations, inserted, updated)

        count_and_check(inserted)
        db.close()

    def test_random_get_smaller(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_smaller, end_key):
            assert len(
                all_smaller) == db.count(db.get_many, 'tree', limit=-1, end=end_key,
                                         inclusive_end=False)
            sort_by_key(all_smaller)
            all_smaller.reverse()
            result = db.get_many('tree', limit=-1,
                                 end=end_key,
                                 inclusive_end=False,
                                 with_storage=False)
            assert check_if_keys_match(result, all_smaller)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        end_key = random.choice(inserted.values())['a']
        all_smaller = filter(lambda k: k['a'] < end_key, inserted.values())

        count_and_check(all_smaller, end_key)

        db.close()

    def test_random_get_smaller_equal(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_smaller_equal, end_key):
            assert len(all_smaller_equal) == db.count(
                db.get_many, 'tree', limit=-1, end=end_key)
            sort_by_key(all_smaller_equal)
            all_smaller_equal.reverse()
            result = db.get_many('tree', limit=-1,
                                 end=end_key,
                                 with_storage=False)
            assert check_if_keys_match(result, all_smaller_equal)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        end_key = random.choice(inserted.values())['a']
        all_smaller_equal = filter(
            lambda k: k['a'] <= end_key, inserted.values())

        count_and_check(all_smaller_equal, end_key)
        db.close()

    def test_random_get_bigger(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_bigger, start_key):
            assert len(
                all_bigger) == db.count(db.get_many, 'tree', limit=-1, start=start_key,
                                        inclusive_start=False)
            sort_by_key(all_bigger)
            result = db.get_many('tree', limit=-1,
                                 start=start_key,
                                 inclusive_start=False,
                                 with_storage=False)
            assert check_if_keys_match(result, all_bigger)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        all_bigger = filter(lambda k: k['a'] > start_key, inserted.values())

        count_and_check(all_bigger, start_key)
        db.close()

    def test_random_get_bigger_equal(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_bigger_equal, start_key):
            assert len(all_bigger_equal) == db.count(
                db.get_many, 'tree', limit=-1, start=start_key)
            sort_by_key(all_bigger_equal)
            result = db.get_many('tree', limit=-1,
                                 start=start_key,
                                 with_storage=False)
            assert check_if_keys_match(result, all_bigger_equal)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        all_bigger_equal = filter(
            lambda k: k['a'] >= start_key, inserted.values())

        count_and_check(all_bigger_equal, start_key)
        db.close()

    def test_random_get_between(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_between, start_key, end_key):
            assert len(all_between) == db.count(db.get_many, 'tree', limit=-1,
                                                start=start_key, end=end_key,
                                                inclusive_start=False, inclusive_end=False)
            sort_by_key(all_between)
            result = db.get_many('tree', limit=-1,
                                 start=start_key, end=end_key,
                                 inclusive_start=False, inclusive_end=False,
                                 with_storage=False)
            assert check_if_keys_match(result, all_between)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        all_between = filter(lambda k: k['a'] > start_key and k[
            'a'] < end_key, inserted.values())

        count_and_check(all_between, start_key, end_key)
        db.close()

    def test_find_existing_and_fail(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        el = db.insert(dict(a=1))
        el2 = db.insert(dict(a=1))
        db.delete(el)
        db.delete(el2)
        with pytest.raises(RecordNotFound):
            db.get('tree', 1)
        db.insert(dict(a=2))
        with pytest.raises(RecordNotFound):
            db.get('tree', 1)
        db.close()

    def test_random_get_between_offset(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_between, start_key, end_key):
            offset = random.randint(5, 20)
            count = len(all_between) - offset
            if count == 0:
                pass  # lucky...
            else:
                assert count if count > 0 else 0 == db.count(
                    db.get_many, 'tree', limit=-1, offset=offset,
                    start=start_key, end=end_key,
                    inclusive_start=False, inclusive_end=False)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        all_between = filter(lambda k: k['a'] > start_key and k[
            'a'] < end_key, inserted.values())

        count_and_check(all_between, start_key, end_key)
        db.close()

    def test_random_get_between_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_between_inclusive, start_key, end_key):
            assert len(
                all_between_inclusive) == db.count(db.get_many, 'tree', limit=-1,
                                                   start=start_key, end=end_key)
            sort_by_key(all_between_inclusive)
            result = db.get_many('tree', limit=-1,
                                 start=start_key, end=end_key,
                                 with_storage=False)
            assert check_if_keys_match(result, all_between_inclusive)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        all_between_inclusive = filter(lambda k: k['a'] >= start_key and k[
            'a'] <= end_key, inserted.values())

        count_and_check(all_between_inclusive, start_key, end_key)
        db.close()

    def test_random_get_between_start_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_between_start_inclusive, start_key, end_key):
            assert len(
                all_between_start_inclusive) == db.count(db.get_many, 'tree', limit=-1,
                                                         start=start_key, end=end_key,
                                                         inclusive_end=False)
            sort_by_key(all_between_start_inclusive)
            result = db.get_many('tree', limit=-1,
                                 start=start_key, end=end_key,
                                 inclusive_end=False, with_storage=False)
            assert check_if_keys_match(result, all_between_start_inclusive)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp
        all_between_start_inclusive = filter(lambda k: k['a']
                                             >= start_key and k['a'] < end_key, inserted.values())
        count_and_check(all_between_start_inclusive, start_key, end_key)
        db.close()

    def test_random_get_between_end_inclusive(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(all_between_end_inclusive, start_key, end_key):
            assert len(
                all_between_end_inclusive) == db.count(db.get_many, 'tree', limit=-1,
                                                       start=start_key, end=end_key,
                                                       inclusive_start=False, with_storage=False)
            sort_by_key(all_between_end_inclusive)
            result = db.get_many('tree', limit=-1,
                                 start=start_key, end=end_key,
                                 inclusive_start=False, with_storage=False)
            assert check_if_keys_match(result, all_between_end_inclusive)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        all_between_end_inclusive = filter(lambda k: k['a'] >
                                           start_key and k['a'] <= end_key, inserted.values())

        count_and_check(all_between_end_inclusive, start_key, end_key)
        db.close()

    def test_open_close(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        inserted = {}
        updated = {}

        def count_and_check(inserted):
            assert len(inserted) == db.count(db.all, 'tree')
            inserted_vals = inserted.values()
            sort_by_key(inserted_vals)
            assert check_if_keys_match(
                db.all('tree', with_storage=False), inserted_vals)

        self.random_database_usage(
            db, 100, inserted, updated)
        count_and_check(inserted)
        db.close()
        db.open()
        count_and_check(inserted)
        self.random_database_usage(
            db, 100, inserted, updated)
        count_and_check(inserted)
        db.close()

    def test_get_deleted_from_leaf_with_one_el(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        el = db.insert(dict(a=1))
        db.delete(el)
        with pytest.raises(RecordNotFound):
            db.get('tree', 1)
        db.close()

    def test_offset_in_get_many_functions(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        custom_tree = CustomTreeIndex(db.path, 'custom_tree')
        db.set_indexes([id, tree, custom_tree])
        db.create()
        database_step_by_step(db)
        inserted = {}
        updated = {}

        def count_and_check(lists, start_key, end_key):
            offset = random.randint(5, 20)
            count = lists[0] - offset
            assert (count if count >= 0 else 0) == db.count(
                db.all, 'tree', offset=offset)
            count = lists[1] - offset
            assert (
                count if count >= 0 else 0) == db.count(db.get_many, 'tree', limit=-1, offset=offset,
                                                        start=start_key, end=end_key,
                                                        inclusive_start=False, inclusive_end=False)
            count = lists[2] - offset
            assert (
                count if count >= 0 else 0) == db.count(db.get_many, 'tree', limit=-1, offset=offset,
                                                        end=end_key, inclusive_end=False)
            count = lists[3] - offset
            assert (
                count if count >= 0 else 0) == db.count(db.get_many, 'tree', limit=-1, offset=offset,
                                                        end=end_key)
            count = lists[4] - offset
            assert (
                count if count >= 0 else 0) == db.count(db.get_many, 'tree', limit=-1, offset=offset,
                                                        start=start_key, inclusive_start=False)
            count = lists[5] - offset
            assert (
                count if count >= 0 else 0) == db.count(db.get_many, 'tree', limit=-1, offset=offset,
                                                        start=start_key)
            count = lists[6] - offset
            assert (count if count >= 0 else 0) == db.count(
                db.get_many, 'tree', start_key, offset=offset)

        self.random_database_usage(db, 1000, inserted, updated, with_get=False)
        start_key = random.choice(inserted.values())['a']
        end_key = random.choice(inserted.values())['a']
        if start_key > end_key:
            tmp = start_key
            start_key = end_key
            end_key = tmp

        lists = [len(inserted)]  # all
        lists.append(len(filter(lambda k: k['a'] > start_key and k[
            'a'] < end_key, inserted.values())))  # all_between
        lists.append(len(filter(lambda k: k['a'] < end_key,
                                inserted.values())))  # all_smaller
        lists.append(len(filter(lambda k: k['a'] <= end_key,
                                inserted.values())))  # all_smaller_equal
        lists.append(len(filter(lambda k: k['a'] > start_key,
                                inserted.values())))  # all_bigger
        lists.append(len(filter(lambda k: k['a'] >= start_key,
                                inserted.values())))  # all_bigger_equal
        lists.append(len(filter(lambda k: k['a'] == start_key,
                                inserted.values())))  # many start key

        count_and_check(lists, start_key, end_key)
        db.close()

    def test_find_key_in_one_el_leaf(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        db.insert(dict(a=1))
        assert 1 == db.count(db.get_many, 'tree', 1, limit=-1)
        assert 1 == db.count(db.get_many, 'tree', end=1, limit=-1)
        assert 0 == db.count(
            db.get_many, 'tree', end=1, limit=-1, inclusive_end=False)
        assert 1 == db.count(
            db.get_many, 'tree', end=2, limit=-1, inclusive_end=False)
        with pytest.raises(RecordNotFound):
            db.get('tree', 2)
        db.close()

    def test_find_key_many_multiple_leaves(self, tmpdir):
        db = self._db(os.path.join(str(tmpdir), 'db'))
        id = UniqueHashIndex(db.path, 'id')
        tree = SimpleTreeIndex(db.path, 'tree')
        db.set_indexes([id, tree])
        db.create()
        for x in xrange(20):
            db.insert(dict(a=1))
        assert 20 == db.count(db.get_many, 'tree', 1, limit=-1)
        db.insert(dict(a=2))
        assert 20 == db.count(db.get_many, 'tree', 1, limit=-1)
        db.close()
