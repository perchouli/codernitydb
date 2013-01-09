.. _design:

======
Design
======


.. note::

    Please remember that CodernityDB is *not* relational Database, forcing it to work in that model will usually work, but it's not recommended. You should try to denormalize it (`Database normalization`).

.. _Database normalization: http://en.wikipedia.org/wiki/Database_normalization



How it's build
--------------

CodernityDB is build from 3 important parts. Please read also :ref:`database_operations_description` to understand how CodernityDB works.


Database
^^^^^^^^


It holds information about Indexes, and mostly does operation on those
indexes. It's the visible Object for End User.

Currently there are implemented 4 different databases

1. Database - a database to use in single process/thread environment
2. DatabaseTreadSafe - a database to use with threads, readers don't
   block writers etc. GeventDatabase is 1:1 copy of that database.
3. DatabaseSuperThreadSafe - a database to also use with threads, but
   database operations are limited to only one in given time.
4. CodernityDB-HTTP - a HTTP server version of database, for multi
   thread and multi process environments. |CodernityDB-HTTP-link|.

.. attention::

    Database to work requires at least one Index. That required index
    must be named **id**. It will store **all** objects that are saved
    in database.

.. hint::

    Do you know that you can use CodernityDB as simple key-value on
    disk storage? Just define only one index (the **id** one).



.. _database_design_index:

Index
^^^^^

.. attention::

    If you want to search / query database for given parameters /
    values etc., you have to specify the index that will return
    key/value from it. Please see :ref:`examples` and :ref:`database_indexes`.

By index we call the class in Python language that was added to the
Database. It can be compared to SQL table (read only), to update
you always need to pass full object to database, our Indexes can be
compared also with CouchDB views mechanizm. (you would like probably to see :ref:`simple_index`). You can have as much indexes as you want and single record in database can "exists" in more than one index.

Index itself does not store any information except it's
*metadata*. You don't have to copy full data every time in indexes,
because all indexes different than *id* one, are bound with it by
``_id`` value, and you can easily get content from that *id* index by
adding ``with_doc=True`` to your get queries (please refer to
:py:meth:`CodernityDB.database.Database.get` for method documentation)

Don't worry it's not hard, to write index, that's an example of hash index
(more in :ref:`examples`)

.. note::

    Remember that adding new index when database exists you have to
    perform reindex on that new index.



.. _example_md5_hash_based_index:

.. code-block:: python

    class Md5Index(HashIndex):

        def __init__(self, *args, **kwargs):
            kwargs['key_format'] = '16s'
            super(Md5Index, self).__init__(*args, **kwargs)

        def make_key_value(self, data):
            return md5(data['name']).digest(), None

        def make_key(self, key):
            return md5(key).digest()

That's one of the simplest index class, it will allow you to query
database for specified `name`, for example:

.. code-block:: python

    [...]
    john = db.get('md5', 'John', with_doc=True)
    [...]



Currently *Hash* based index (`Hash Table`_ separate chaining version) and *B+Tree* based (`B Plus Tree`_) are available.

Both indexes makes huge use of `Sparse files`_.

For more information about indexes visit :ref:`database_indexes`

Also please remember that more indexes affects write performance.

.. warning::

    The **id** index should save whole object content, otherwise the options *with_doc* will not work as expected.



Storage
^^^^^^^
Storage is used by index to store values from it (look at the second return parameter in code example above).

If index returns ``None`` as value, no storage operation is
performed.

Storage needs to save python value to the disk and return the position
and size to allow Index to save that data. The default implementation
uses Python marshal_ to serialize and deserialize Python objects
passed as value into it. So you will be able to store those object
that are serializable by marshal_ module.




ACID
----

CodernityDB never overwrites existing data. The **id** index is
**always** consistent. And other indexes can be always restored,
refreshed (:py:meth:`CodernityDB.database.Database.reindex_index` operation) from it.

In given time, just one writer is allowed to write into single index
(update / delete actions). Readers are never blocked.

The write is first performed on storage, and then on
index metadata. After every write operation, the index does flush of the storage and
metadata files. It means that in worst case (power lost during write
operation) the previous metadata and storage information will be
valid.

Database doesn't allow multiple object operations, and has no support
for typical transaction mechanizm (like SQL databases have). But
*single object operation* is fully atomic.

To handle multiple updates to the same document we use ``_rev`` (like CouchDB_) field,
that informs us about document version. When ``rev`` is not matched
with one from Database, write operation is refused.

There is also nothing like *delayed write* in default CodernityDB
implementation. After each write, internals and file buffers are flushed, and then the write confirmation is returned to user.


.. warning::
    CodernityDB does no sync kernel buffers with disk itself. To be sure that data is written to disk please call :py:meth:`~CodernityDB.database.Database.fsync`, or use :py:meth:`CodernityDB.patch.patch_flush_fsync` to call fsync always when flush is called (after data modification).




.. _CouchDB: http://couchdb.apache.org


Disk usage
----------

**Indexes** tries to reuse as much space as possible, because
*metadata* size is fixed, during every write operation,
if index finds *metadata* marked as removed or so, it reuses it -
writes new data into that place.

Because of *never update* in **Storage**, a lot of space is wasted
there. To optimize the disk usage run
:py:meth:`CodernityDB.database.Database.compact()` or
:py:meth:`CodernityDB.index.Index.compact()` method.


.. _B Plus Tree: http://en.wikipedia.org/wiki/B%2B_tree
.. _Hash Table: http://en.wikipedia.org/wiki/Hash_table
.. _marshal: http://docs.python.org/library/marshal.html
.. _Sparse files: http://en.wikipedia.org/wiki/Sparse_file



.. _database_operations_description:

Database operations flow
------------------------

During insert into database, incoming data is passed to
``make_key_value`` functions in *all* indexes in order of adding or
changing them in database.
On query operations function ``make_key`` is called to get
valid key for the given index.
So having more indexes affects write speed, but does not affect read speed at all.

.. note::

   Interested in speed? Visit :ref:`speed` showcase.


Insert
^^^^^^

Incoming data is at first processed in *id* index. Then it goes
through ``make_key_value`` method, in next stage the value is stored in
*storage*, and at last the metadata is stored in *index*.
Then the procedure is repeated for other indexes.

.. note::
   Please see :py:meth:`~CodernityDB.database.Database.insert` docs
   for details.


Update
^^^^^^

Works in the same way as *insert* operation. But you have to specify
``_rev`` and ``_id`` fields. The ``_rev`` field is compared with
currently stored in database. If they match, the operation continues, in
other situation :py:exc:`.DatabaseConflict` is raised.

Also there is no possibility to update single attribute of object in
database. You have to always do full update. So even for updating a single
attribute you have to perform ``get`` + ``update`` on whole object from database.


.. note::
   Please see :py:meth:`~CodernityDB.database.Database.update` docs for details.



Delete
^^^^^^

During delete phase at first the data is deleted from *all* indexes
but *id*, then if succeeded at last phase from *id* index. Delete operation is in
general just a bit changed update one. In fact the *delete* means
*mark as deleted*. No direct delete is performed. The place used by
*metadata* will be reused in first possible situation (ie. will not
iterate further if element marked as *deleted* is found).

To real delete data from database you have to first delete it, then run
:py:meth:`CodernityDB.database.Database.compact` or :py:meth:`CodernityDB.database.Database.reindex`.


.. note::
   Please see :py:meth:`~CodernityDB.database.Database.delete` docs for details.

.. note::
    Please see :ref:`database_indexes` for index documentation and description.


Using that order, we can be sure that even in case of index failure,
in any case we have fully working *id* index, and it can be used to
rebuild other index structure
(:py:meth:`CodernityDB.database.Database.reindex` and :py:meth:`CodernityDB.database.Database.compact`)
