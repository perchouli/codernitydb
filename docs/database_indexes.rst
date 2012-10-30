.. _database_indexes:

Database indexes
================


Currently there are two main Index implementations.
You need to know the difference between them before you will choose the base for your Index.
Remember that you don't need to code whole index code, just required
methods or even parts of them.

The default implemented indexes uses 2 files. One is for index
metadata / structure, the second one is for storage. So the total
number of files opened by database is usually
``number_of_indexes * 2``. Indexes makes usage of `sparse files`_ to
store information.


To see how to write index see below

.. note::

    The object that you pass to database to add index, it's not the
    same object that will be database side!


.. seealso::

    :ref:`internal_index_functions` for description of powerfull mechanizm of inside index (database) functions.

Using custom index is quite simple. You need to decide if your index needs to be Hash based (:ref:`details <internal_hash_index>`) or Tree based (:ref:`details <internal_tree_index>` for
pros and cons)


.. warning::
    Please remember that the object that is finally used by Database is
    not the object that you passed into!

There is special class attribute to solve import problems in indexes
in DB.

custom_header
    It's string that will be inserted to final index file. It's
    usefull to pass the custom imports there. You will find an example
    in :ref:`Examples - secure storage <secure_storage_example>`

storage_class
    It defines what storage to use. By default all indexes will use :py:class:`CodernityDB.storage.Storage`


.. _internal_hash_index:

Hash Index
----------

This index is based on `Hash Table`_ with `separate chaining`_. You
can refer also for `ISAM`_.


* Pros
    - Fast
* Cons
    - Records are not in order of insert / update / delete but in *random* like
    - Can be queried only for given key, or iterate over all keys


There are two major implementations

* :py:class:`~CodernityDB.hash_index.UniqueHashIndex` - should be used
  only for **id** index
* :py:class:`~CodernityDB.hash_index.HashIndex` - a general use Hash Index.

They differs in several places, for details you should read the code
of them both.

.. seealso::

    :ref:`Hash Index speed tests <hash_speed>`
        For speed tests

    :py:class:`CodernityDB.hash_index.HashIndex`
        For documentation

.. _conflict_resolution:

conflict resolution
    When using ``0xfffff`` *hash_lim* after ``1200`` inserts there is
    50% probability that conflict will occur
    (`birthday problem`_). Conflicts are solved by `separate
    chaining`_, so keys with the same hash function results are linked
    into list, then traversed when needed.

duplicate keys
   For duplicate keys the same mechanizm is used as for
   :ref:`conflict resolution <conflict_resolution>`. All indexes different than *id* one can
   accept more than one record with the same key
   (:py:meth:`~CodernityDB.database.Database.get_many`).


.. _birthday problem: http://en.wikipedia.org/wiki/Birthday_problem
.. _separate chaining: http://en.wikipedia.org/wiki/Hash_table
.. _ISAM: http://en.wikipedia.org/wiki/ISAM


.. _custom_hash_index:

Hash Index details
~~~~~~~~~~~~~~~~~~

.. note::
   For api documentation please see :py:class:`.HashIndex`


Below you will find explained in details parameters for that index
type.

.. _key_format:

key_format
    It defines what type is your key.

    The most important for you as you're interested to write index for
    your use is the key size.

    For example if you want to use MD5 based index you would need set
    the ``key_format`` to ``16s`` which would set the size for *key*
    to 16 characters exactly how long is md5 hash (digest).

    An example code for Md5 based index can be found :ref:`design`,
    more examples in :ref:`examples`

    .. note:: For format specification and explaination please visit
        `Python struct documentation`_


hash_lim
    It defines how big results will return index hash function.

    Current default is ``0xfffff`` which means ``1048575`` different
    hash function results.

    .. hint:: In perfect conditions you will be abble to store those
        number of unique records without conflicts, in practice you
        will be abble to store like ``1200`` records without conflict
        with 50% probability (for example `birthday problem`_). Lookup
        when conflict occurs is slower because linked list is
        traversed. More informations about conflicts :ref:`Hash Index
        <internal_hash_index>`.

    .. hint:: If you want to have index that searches for let's say
        ``True`` and  ``False`` values, you **should** set that
        parameter to ``2``. Because the rest values will be not used
        (however nothing bad will happen).


make_key_value
    (:py:meth:`~CodernityDB.index.Index.make_key_value`)

    That function is called by database when inserting new or updating
    objects in database.  It **has** to return ``None`` if index is
    not matched (not required to operate on it) and 2 values if index
    is mached. That 2 values are in order: *key* and *value*. Please
    remember that key must fit your :py:attr:`entry_line_format`.


make_key
    (:py:meth:`~CodernityDB.index.Index.make_key`)

    That function is called when query operations are performed on
    database. It should format the key correctly to match that one
    returned by :py:meth:`CodernityDB.index.Index.make_key_value`


entry_line_format
    (*for advanced users*, please check if :ref:`key format <key_format>` is not
    enough for you)

    Entry line format contains all metadata required for Hash Index to
    work.

    First You need to decide it will look like. The default is
    ``<32s{key}IIcI`` which means in order:

    0. mark for use *little-endian* encoding ``<``
    1. document id format ``32s``
    2. index key format ``{key}``, it will be replaced with ``c`` or
       if defined with value from ``key_format`` parameter.
    3. start of a record in storage format ``I``
    4. size of a record in storage format ``I``
    5. status format ``c`` (you probably do not want to change it)
    6. next record (in case of conflicts) format ``I``


    .. note:: If you expect that your index might require more than
        *4294967295* bytes of space or metadata (that's the max number
        for ``I`` format), change it to ``Q``.


Hash Index Example
""""""""""""""""""


Let's assume that you want to write hash based index that will
separate objects with ``a % 2 == 0`` from ``a % 2 == 1``.


.. code-block:: python

    class AIndex(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = '?'
            kwargs['hash_lim'] = 2
            super(AIndex, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            val = data.get('a')
            if val is none:
                return None
            return val % 2, None

        def make_key(self, key):
            return key


It will allow you to perform for example:

.. code-block:: python

    [...]
    number_of_zeros = db.count(db.get_many, 'a', 0)
    number_of_ones = db.count(db.get_many, 'a', 1)
    [...]


.. note::
    Please see :ref:`examples` for more examples, and
    :py:mod:`CodernityDB.hash_index` for full Hash Index documentation


.. _Python struct documentation: http://docs.python.org/library/struct.html#format-characters
.. _birthday problem: http://en.wikipedia.org/wiki/Birthday_problem



.. _internal_tree_index:

B Plus Tree Index
-----------------

This index is based on `B Plus Tree`_. Duplicate keys are stored
inside Tree structure (on leafs/nodes).


* Pros
    - Can be queried for range queries
    - Records are in order (depends of your keys)
* Cons
    - Slower than Hash based indexes


.. seealso::

    :ref:`Tree Index speed tests <tree_speed>`
        For speed tests

    :py:class:`CodernityDB.tree_index.TreeBasedIndex`
        For documentation


duplicate keys
    Duplicate keys are stored inside tree structure. So in worst case
    when you have more duplicate keys than ``node_size`` tree will
    became suboptimal (a half of one node will be always empty)



.. _Hash Table: http://en.wikipedia.org/wiki/Hash_table
.. _B Plus Tree: http://en.wikipedia.org/wiki/B%2B_tree
.. _sparse files: http://en.wikipedia.org/wiki/Sparse_file


Tree Index details
~~~~~~~~~~~~~~~~~~

.. note::
   For api documentation please see :py:class:`.TreeBasedIndex`

That index is based on `BPlus tree`_. It's main advantage is that it
stores data in order. And you can make *range* queries.

key_format
   It's the same as in hash index, so please refer to :ref:`key_format
   in hash index details <key_format>`

pointer_format
   It's information about internal pointer format in tree. Change it
   only when you need it (for example when your index file might be
   bigger than ``4294967295 bytes``)

meta_format
   Contains similar information as ``entry_line_format`` in hash
   index. Change it when you really need to.

node_capacity
   One of the most important parameters in whole tree index. It
   defines how big is one leaf / node inside tree. If you expect much
   data to come into your index, you should play with a bit to adjust
   the most correct size. Generally, bigger value means less tree
   height, but it might mean more shifts inside when doing insert
   operation. It can be said, that bigger node_capacity means faster
   get operations.


Tree Index Example
""""""""""""""""""

.. code-block:: python

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

It will allow you to perform for example:

.. code-block:: python

    [...]
    from_3_to_10 = db.get_many('tree', limit=-1, start=3, end=10, inclusive_start=True, inclusive_end=True)
    [...]

And you will get all records that have ``a`` value from 3 to 10.



.. _internal_index_functions:

Index functions
---------------


Quite important thing in CodernityDB are index functions. You can do with them anything you want they have access to database object, so they can perform operations on multiple indexes. If you want join like operation, you should write function. Then you will be abble to run that function database side when using |CodernityDB-HTTP-link|. The only mandatory argument for that kind of function is ``db``, the rest are function arguments.


Writting function is easy see an example there:

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


That function performs simple JOIN operation (that JOIN known from SQL databases). As you can see it's exactly the same as you would code in your code to archive that.


Function should start it's name from ``run_`` then you can call it:

.. code-block:: python

    gen = db.run("index", "timeline", "user_a", 10)



As mentioned before, while you work in embedded mode it makes no big difference, but when using |CodernityDB-HTTP-link| it makes huge.


.. note::

    Please remember that CodernityDB is *not* relational Database, forcing it to work in that model will usually work, but it's not recommended. You should try to denormalize it (`Database normalization`_).

.. _Database normalization: http://en.wikipedia.org/wiki/Database_normalization


.. note::
   Please see :ref:`examples` for more examples, and :py:mod:`CodernityDB.tree_index` for full documentation


.. _BPlus tree: http://en.wikipedia.org/wiki/B%2B_tree
