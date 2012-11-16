CodernityDB pure python, NoSQL, fast database
=============================================

CodernityDB is opensource, pure Python (no 3rd party dependency), fast (really fast check :ref:`speed` if you don't believe in words), multiplatform, schema-less, NoSQL_ database. It has optional support for HTTP server version (|CodernityDB-HTTP-link|), and also Python client library (|CodernityDB-PyClient-link|) that aims to be 100% compatible with embedded version.

.. image:: CodernityDB.png
    :align: center


You can call it a more advanced key-value database. With multiple key-values indexes in the same engine. Also CodernityDB supports functions that are executed inside database.


Key features
~~~~~~~~~~~~

* Native Python database
* Multiple indexes
* Fast (more than 50 000 insert operations per second see :ref:`speed` for details)
* Embedded mode (default) and :ref:`Server<server>`, with client library that aims to be 100% compatible with embedded one.
* Easy way to implement custom Storage (see :ref:`example_storage`)


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

CodernityDB is one of projects developed and released by Codernity_, so you can contact us directly in any case.

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
