CodernityDB pure python, fast, NoSQL database
=============================================

CodernityDB is opensource, pure Python (no 3rd party dependency), fast (even 100 000 insert and more than 100 000 get operations per second, check :ref:`speed` if you don't believe in words), multi platform, schema-less, NoSQL_ database.

.. image:: CodernityDB.png
    :align: center


You can also call it a more advanced key-value database, with multiple key-values indexes in the same engine (for sure it's not "simple key/value store"). Also CodernityDB supports functions that are executed inside database. It has optional support for HTTP server version (|CodernityDB-HTTP-link|), and also Python client library (|CodernityDB-PyClient-link|) that aims to be 100% compatible with embedded version.

**And it's** `Apache 2.0`_ **licensed !**


Key features
~~~~~~~~~~~~

* Native Python database
* Multiple indexes
* Fast (even 100 000 insert and more than 100 000 get operations per second see :ref:`speed` for details)
* Embedded mode (default) and :ref:`Server<server>`, with client library that aims to be 100% compatible with embedded one.
* Easy way to implement custom Storage (see :ref:`example_storage`)
* Collections / Tables support (see :ref:`tables_collections_q`)
* Sharding (see :ref:`sharding_in_indexes`)

Install
~~~~~~~

Because CodernityDB is pure Python you need to perform standard installation for Python applications::

   pip install CodernityDB

or using easy_install::

   easy_install CodernityDB

or from sources::

   hg clone ssh://hg@bitbucket.org/codernity/codernitydb
   cd codernitydb
   python setup.py install


Contribute & Bugs & Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CodernityDB is one of projects developed and released by Codernity_, so you can contact us directly in any case via db@codernity.com.

Do you want to contribute? Great! Then just fork our repository (|cdb_repo|) on Bitbucket and do a pull request. It can't be more easy!

To fill a bug please also use Bitbucket.


.. _codernity: http://codernity.com


Support
~~~~~~~

In case of any problems, feature request you can also contact us directly.

Do you want customized version of CodernityDB ? No problem, just contact us.


Documentation index
~~~~~~~~~~~~~~~~~~~

.. toctree::
    :maxdepth: 2

    design
    database_indexes

    how_its_tested
    speed

    server
    deployment

    quick
    examples

    faq

    api


Indices and tables
""""""""""""""""""

* :ref:`genindex`
* :ref:`search`
* :ref:`modindex`


.. _NoSQL: http://en.wikipedia.org/wiki/NoSQL
.. _KnockoutJS: http://knockoutjs.com/
.. _Apache 2.0: http://www.apache.org/licenses/LICENSE-2.0.html
