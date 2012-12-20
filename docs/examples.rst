.. _examples:

====================
Examples
====================

Here you will find some examples, from different projects / ideas. So
they might miss some *context* information etc.


.. _example_indexes:

Example indexes
---------------

Custom Hash
"""""""""""
It defines index that separates data in two groups ``>5`` and ``<=5`` based on ``test`` parameter.

.. code-block:: python

    class CustomHashIndex(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = 'I'
            kwargs['hash_lim'] = 1
            super(CustomHashIndex, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            d = data.get('test')
            if d is None:
                return None
            if d > 5:
                k = 1
            else:
                k = 0
            return k, dict(test=d)

        def make_key(self, key):
            return key

MD5 Hash
""""""""

Nothing more than index shown in :ref:`design`.

.. code-block:: python

    class Md5Index(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = '32s'
            super(Md5Index, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            return md5(data['name']).hexdigest(), None

        def make_key(self, key):
            return md5(key).hexdigest()

With A
""""""

It allows to search in database for objects that contains ``a`` in
their structure.

.. code-block:: python

    class WithAIndex(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = '32s'
            super(WithAIndex, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            a_val = data.get("a")
            if a_val:
                if not isinstance(a_val, basestring):
                    a_val = str(a_val)
                return md5(a_val).hexdigest(), None
            return None

        def make_key(self, key):
            if not isinstance(key, basestring):
                key = str(key)
            return md5(key).hexdigest()

Simple Tree
"""""""""""

That index uses Tree index. It will allow you to search **in order**
for elements that have ``t`` in their structure.


.. code-block:: python

    class Simple_TreeIndex(TreeBasedIndex):

        def __init__(self, *args, **kwargs):
            kwargs['node_capacity'] = 100
            kwargs['key_format'] = 'I'
            super(Simple_TreeIndex, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            t_val = data.get('t')
            if t_val is not None:
                return t_val, None
            return None

        def make_key(self, key):
            return key


With Run
""""""""

This index will allow you to run ``sum`` function on database context
(you don't have to retrieve all the data first, and then process
it). Strongly recommended on :ref:`server` usage.

.. code-block:: python

    class WithRun_Index(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = 'I'
            super(WithRun_Index, self).__init__(*args, **kwargs)

        def run_sum(self, db_obj, key):
            gen = db_obj.get_many(index_name=self.name, key=key, limit=-1, with_storage=True)
            vals = []
            while True:
                try:
                    d = gen.next()
                except StopIteration:
                    break
                else:
                    vals.append(d.get('x', 0))
            return sum(vals)

        def make_key_value(self, data):
            a_val = data.get("a")
            if a_val is not None:
                out = {'x': data.get('x')}
                return a_val, out
            return None

        def make_key(self, key):
            return key


Example sharded hash
""""""""""""""""""""

Example sharded index, it will shard records on ``key`` into 10 shards. (see :ref:`sharding_in_indexes`)

.. code-block:: python

    class MySharded(ShardedHashIndex):

        custom_header = """from CodernityDB.sharded_hash import ShardedHashIndex"""

        def __init__(self, *args, **kwargs):
            kwargs['sh_nums'] = 10
            kwargs['key_format'] = 'I'
            kwargs['use_make_keys'] = True
            super(MySharded, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            return data['x'] % 10, None

        def calculate_shard(self, key):
            return key % self.sh_nums



.. _example_storage:

Example storages
----------------


.. _secure_storage_example:

Secure storage
""""""""""""""

It allows you to crypt storage information with Salsa20_ algorithm. To
use it you need to have index that will open storage with encryption key.

.. literalinclude:: demo_secure_storage.py


.. _Salsa20: http://en.wikipedia.org/wiki/Salsa20



Example database functions
--------------------------

.. _join_like1:

Join like 1
"""""""""""

It will join user with timeline entries. See |CodernityDB-Demos| ``minitwitt`` to see more things like this.

.. code-block:: python

    def run_timeline(self, db, user, limit):
        u = db.get('user', user)
        it = db.get_many(self.name, user, end=10 ** 11, limit=limit, with_doc=True)
        for curr in it:
            curr['username'] = user
            curr['email'] = u['email']
            curr['pub_date'] = curr['doc']['pub_date']
            curr['text'] = curr['doc']['text']
            del curr['doc']
            yield curr
