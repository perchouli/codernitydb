.. _api_database:

API docs
========

Here you will find API docs. If you're python user you will probably
understand it. In other case you should visit:

- :ref:`database_operations_description`
- :ref:`design`
- :ref:`quick_tutorial`

And you probably want to use |CodernityDB-HTTP-link| instead this embedded version.

Database
--------

.. note::
    Please refer to :ref:`database_operations_description` for general description


Standard
^^^^^^^^

.. automodule:: CodernityDB.database
    :members:
    :undoc-members:
    :show-inheritance:


Thread Safe Database
^^^^^^^^^^^^^^^^^^^^

.. autoclass:: CodernityDB.database_thread_safe.ThreadSafeDatabase
    :members:
    :undoc-members:
    :show-inheritance:


Super Thread Safe Database
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: CodernityDB.database_super_thread_safe
    :members:
    :undoc-members:
    :show-inheritance:


Gevent Database
^^^^^^^^^^^^^^^

.. automodule:: CodernityDB.database_gevent
    :members:
    :undoc-members:
    :show-inheritance:


Indexes
-------

.. note::
   If you're **not interested in CodernityDB development / extending** you don't need to read this section, 
   please then refer to :ref:`database_indexes`, **otherwise** please remember that index methods are called from
   ``Database`` object.

   

General Index
^^^^^^^^^^^^^

.. automodule:: CodernityDB.index
    :members:
    :undoc-members:
    :show-inheritance:


Hash Index specific
^^^^^^^^^^^^^^^^^^^

.. note::
    Please refer to :ref:`internal_hash_index` for description

.. automodule:: CodernityDB.hash_index
    :members:
    :undoc-members:
    :show-inheritance:


B+Tree Index specific
^^^^^^^^^^^^^^^^^^^^^

.. note::
    Please refer to :ref:`internal_tree_index` for description

.. automodule:: CodernityDB.tree_index
    :members:
    :undoc-members:
    :show-inheritance:



Storage
-------

.. automodule:: CodernityDB.storage
    :members:
    :undoc-members:
    :show-inheritance:


Patches
-------

.. automodule:: CodernityDB.patch
    :members:
    :show-inheritance:
    :undoc-members:
