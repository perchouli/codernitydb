.. _database_indexes:

Database indexes
================

.. note::

    At first you should read :ref:`what's index in design section<database_design_index>`. 


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

    :ref:`internal_index_functions` for description of powerful mechanism of inside index (database) functions.

Using custom index is quite simple. You need to decide if your index needs to be Hash based (:ref:`details <internal_hash_index>`) or Tree based (:ref:`details <internal_tree_index>` for
pros and cons)


.. warning::
    Please remember that the object that is finally used by Database is
    not the object that you passed into!

There is special class attribute to solve import problems in indexes
in DB.

custom_header
    It's string that will be inserted to final index file. It's
    useful to pass the custom imports there. You will find an example
    in :ref:`Examples - secure storage <secure_storage_example>`.

storage_class
    It defines what storage to use. By default all indexes will use :py:class:`CodernityDB.storage.Storage`. If your Storage needs to be initialized in custom way please look at :ref:`Examples - secure storage <secure_storage_example>`.


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
   For duplicate keys the same mechanism is used as for
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


.. seealso::

    :ref:`multiple_keys_index`
       for Multiindex hash based implementation (more than one key per database data).


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

    .. note:: For format specification and explanation please visit
        `Python struct documentation`_


hash_lim
    It defines how big results will return index hash function.

    Current default is ``0xfffff`` which means ``1048575`` different
    hash function results.

    .. hint:: In perfect conditions you will be able to store those
        number of unique records without conflicts, in practice you
        will be able to store like ``1200`` records without conflict
        with 50% probability (for example `birthday problem`_). Look up
        when conflict occurs is slower because linked list is
        traversed. More information about conflicts :ref:`Hash Index
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
    is matched. That 2 values are in order: *key* and *value*. Please
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

    :ref:`multiple_keys_index`
       for Multiindex tree based implementation (more than one key per database data).


duplicate keys
    Duplicate keys are stored inside tree structure. So in worst case
    when you have more duplicate keys than ``node_size`` tree will
    became sub-optimal (a half of one node will be always empty)



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



.. _multiple_keys_index:

Multikeys index
----------------

Multikeys indexes (aka Multiindex) are indexes where you can have more than one key per database data. They share the same properties as their bases (:ref:`internal_hash_index` and :ref:`internal_tree_index`).

Imagine something like infix search:

.. code-block:: python
    
    class TreeMultiTest(MultiTreeBasedIndex):

        custom_header = """from CodernityDB.tree_index import MultiTreeBasedIndex
    from itertools import izip"""

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = '16s'
            super(TreeMultiTest, self).__init__(*args, **kwargs)
            self.__l = kwargs.get('w_len', 2)

        def make_key_value(self, data):
            name = data['w']
            l = self.__l
            max_l = len(name)
            out = set()
            for x in xrange(l - 1, max_l):
                m = (name, )
                for y in xrange(0, x):
                    m += (name[y + 1:],)
                out.update(set(''.join(x).rjust(16, '_').lower() for x in izip(*m)))  #ignore import error
            return out, dict(w=name)

        def make_key(self, key):
            return key.rjust(16, '_').lower()
    
By using that index you will be able to perform infix search over all words in your database. Only one difference from non multi index is that ``make_key_value`` has to return iterable (the best will be set because it has unique values). Then you can easily run something like that:
    
.. code-block:: python
    
    db = Database('/tmp/multi')
    db.create()
    db.add_index(TreeMultiTest(db.path, "words"))
    
    db.insert(dict(w='Codernity'))
    
    print db.get('words', 'dern')['w']  # "Codernity"
    print db.get('words', 'cod')['w']  # "Codernity"
    

As you can see implementing infix/suffix/prefix search mechanism in CodernityDB is very easy.
    
.. note::
    Multiindex requires more time to insert data. Get speed is exactly as fast as in non multiindex (same rules applies to both of them).
    
Obviously that's not only one use case for that indexes, it's just probably the most obvious usage example.
    
Currently both Hash and Tree indexes have multiindex implementations: ``MultiHashIndex`` and ``MultiTreeBasedIndex`` (yes they are just prefixed by word ``Multi``).




.. _internal_index_functions:

Index functions
---------------


Quite important thing in CodernityDB are index functions. You can do with them anything you want they have access to database object, so they can perform operations on multiple indexes. If you want join like operation, you should write function. Then you will be able to run that function database side when using |CodernityDB-HTTP-link|. The only mandatory argument for that kind of function is ``db``, the rest are function arguments.


Writing function is easy see an example there:

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



.. _simple_index:

Easier way of creating indexes
------------------------------

    Do I really need to code indexes in Python, is there an easier way to create indexes?

You bet there is! We prepared special, simplified mode for creating indexes. That code will be translated to Python code, and then used exactly in the same way as the rest indexes (so that simple indexes are exactly the same fast as pure Python ones). They are just simple by name not by possibilities.

.. note:: 
    Don't be surprised, if we will name it SimpleIndex in several places there.

Usage of this mode is really basic, you just need to provide 2 (or optionally 3) things: 

* properties of the index like this:
    
::
    
    name = MyTestIndex
    type = HashIndex
    key_format = I
    etc. etc. ...

* body for *make_key_value* containing our simplified syntax:
   
::
 
    make_key_value:
    a > 1: 1, None
    a, None

* and optionally body for *make_key* (if you don't provide it, it will be generated automatically and set to return key value as it is):
    
::

    make_key:
    key > 1: 1
    key

Syntax of function body is really basic, every line preceded by a statement with colon at the end means that 
if conditions before colon are met, function will return everything that follows colon. If there is no colon, 
value will be always returned. Of course the order of lines actually matters, so if you provide body like that:

::

    a > 1: 1, None
    a > 2: 2, None

The second value will be never returned (because if *a* is less then 1 it's for sure less than 2).
Every name will be look for in dictionaries given to functions, so body like:

::
    
    a > 1: a, None

will generate python code like that:

.. code-block:: python
    
    if data["a"] > 1:
        return data["a"], None

That's everything you need to know to work with our simplified index creator, you just need to always provide *name* and *type* in index properties
and provide body for make_key_value, which has to return always two values (the 2nd has to be a dictionary or None). 
Here you have some examples and their equivalents in python. Remember that this simplified creator doesn't provide python power,
so if you want to write more sophisticated index, you will have to learn python.


::

    name = Test
    type = HashIndex
    key_format = 16s

    make_key_value:
    md5(a),None

    make_key:
    md5(key)

is an equivalent to: 

.. code-block:: python

    class Test( HashIndex ):
        def __init__(self,*args,**kwargs):         
            kwargs["key_format"] = '16s' 
            super( Test ,self).__init__(*args,**kwargs)
        def make_key_value(self,data):         
            return md5 ( data["a"] ) .digest() , None 
        def make_key(self,key): 
            return md5 ( key ) .digest()


Keywords & Helpers in simple index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please note that Python ``None`` value can be written as: ``none``, ``None``, ``null``.

Supported logic operators:

* ``and``
* ``&&``
* ``or``
* ``||``

Functions that you can use in ``make_key`` and ``make_key_value``:

.. method:: str(value)

    it will convert value to string

    :param value: value to convert to string
    :returns: string value of ``value``
    :rtype: string

.. method:: len(value) 

    it will return length of a value

    :param value: a value to check length
    :returns: length of value
    :rtype: integer

.. method:: md5(value)

    it will return md5 value of a string
    
    :param string value: string value to get md5 from it
    :returns: md5 sum of value
    :rtype: string

.. method::  fix_r(value, length)

    it will return fixed string length. ``Value`` shorter than ``length``
    will be right filled with ``_``.

    :param string value: value to adjust string length
    :param integer length: how long should be output string
    :returns: fixed size string
    :rtype: string

.. method:: infix(value, min_len, max_len, fixed_len)

    it will generate all possible infixes of ``value`` not shorter than ``min_len`` 
    and not longer than ``max_len`` while all of them will have fixed length
    defined in ``fixed_len`` (which works exactly as ``fix_r``)
    
    :param string value: a string which all infixes will be generated from
    :param integer min_len: minimal length of an infix
    :param integer max_len: maximal length of an infix
    :param integer fixed_len: fixed size of all infixes
    :returns: set containing fixed size infixes
    :rtype: set  

.. method:: prefix(value, min_len, max_len, fixed_len)

    it will generate all possible prefixes of ``value`` not shorter than ``min_len`` 
    and not longer than ``max_len`` while all of them will have fixed length
    defined in ``fixed_len`` (which works exactly as ``fix_r``)
    
    :param string value: a string which all prefixes will be generated from
    :param integer min_len: minimal length of an prefix
    :param integer max_len: maximal length of an prefix
    :param integer fixed_len: fixed size of all prefixes
    :returns: set containing fixed size prefixes
    :rtype: set   

.. method:: suffix(value, min_len, max_len, fixed_len)

    it will generate all possible suffixes of ``value`` not shorter than ``min_len`` 
    and not longer than ``max_len`` while all of them will have fixed length
    defined in ``fixed_len`` (which works exactly as ``fix_r``)
    
    :param string value: a string which all suffixes will be generated from
    :param integer min_len: minimal length of an suffix
    :param integer max_len: maximal length of an suffix
    :param integer fixed_len: fixed size of all suffixes
    :returns: set containing fixed size suffixes
    :rtype: set   

.. note::
    Obviously you can use that simple indexes in |CodernityDB-HTTP-link| without any problem.

.. note::
    Error reporting / handling system in that mode will tell you exactly what's wrong with your code.


.. _tables_collections_q:

Tables, collections...?
-------------------------

    OK, I got it, but can I store more than one data type in Database. Is there something like table or collection ?

.. note::

   In |CodernityDB-demos| you can find minitwit example which is rewrite from Sqlite application.

Sure! You can use Index mechanism do to it. As it has been mentioned before, Index mechanism in CodernityDB is like read only Table in SQL databases (see :ref:`index design <database_design_index>`). So all you need is to define how your records will differ each other.

Let's assume that you want to users and users and some data that belongs to user. You will probably want to be able to get all users, and all things that belongs to him, right? So.

.. literalinclude:: codes/tables_like_indexes.py
   :linenos:


Having that indexes in your database will allow you to query for single user and for items of that user. Isn't it simple?
As you can see, index in CodernityDB is not an index that you probably get used to. It's much more.

How an index code is processed by CodernityDB?
-----------------------------------------------

    When you provide CodernityDB with an index, it uses `getsource <http://docs.python.org/2/library/inspect.html#inspect.getsource>`_ function from `inspect module <http://docs.python.org/2/library/inspect.html>`_ to get index code. This means, that after you call add_index function, it will look for the index class in current scope, take the whole class code as it is (including intends) and place it inside it's own code. Hence there are few cons you have to bear in mind:

* You can not generate class code on the fly inside, let's say, a function, like that:
    
.. code-block:: python

    def create_index(self,a):
        class MyIndex(HashIndex):
            def __init__(self, *args, **kwargs):
                kwargs['key_format'] = 'I'
                super(MyIndex, self).__init__(*args, **kwargs)
            def make_key_value(self,data):
                if data['a'] == a:
                    return data['b'], None
            def make_key(self,key):
                return key
        return MyIndex

Despite of code being correct in python terms, it will produce an error in CodernityDB, since class isn't defined in proper scope. 

    * You can not provide index class code with a variable defined outside this class:

.. code-block:: python
    
    a = 5
    class MyIndex(HashIndex):
        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = 'I'
            super(MyIndex, self).__init__(*args, **kwargs)
        def make_key_value(self,data):
            if data['a'] == a:
                return data['b'], None
        def make_key(self,key):
                return key

Even if now class is in proper scope, the example won't work, because variable ``a`` isn't known to CodernityDB.


.. _sharding_in_indexes:

Sharding in indexes
-------------------

For advanced users we have sharded indexes.

All you need to do if you want to use Sharded indexes is just:

.. literalinclude:: codes/shard_demo.py
   :linenos:


.. warning::

   Just remember that you have to **hardcode** ShardIndex parameters (unlike other indexes). So you **really** should derive from it's class.



.. _sharding_performance:


Performance
~~~~~~~~~~~

Consider this script

.. literalinclude:: codes/shard_vs_noshard.py
   :linenos:


.. list-table::
   :header-rows: 1

   * - Number of inserts
     - Time in sharded
     - Time in non sharded
   * - 5 000 000
     - 65.405 seconds
     - 74.699 seconds
   * - 10 000 000
     - 148.095 seconds
     - 186.383 seconds


As you can see, sharding **does matter**. It gives you almost **25%** performance boost. Totally free. Similar performance boost applies to get operations.


.. note::

    What's even more important in Sharding is that as you probably already know CodernityDB index metadata stores data position and size by using ``struct`` module. By default those fields are ``I`` format (``unsigned int``). So when you need to change that format to ``Q`` without sharding, you probably can switch to sharding and still use ``I`` format. ``I`` format can accept values up to ``4294967295`` bytes so about 4GB. Having 100 shards will mean that you can index up to ``4GB * 100`` data.


.. note::

   Currently one index can have up to 255 shards.
