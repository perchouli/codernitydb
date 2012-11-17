.. _how_its_tested:


How it's tested
===============

We use pytest_ with  pytest-xdist_ to distribute the tests to multiple
CPUs.

What's also worth to note, we tests CodernityDB on python 2.6.x,
2.7.x, pypy (1.9 and dev branch) using Tox_, and Jenkins_ takes care about continuous testing process.

All major parts of CodernityDB has their own test suite. We also test
CodernityDB single-thread, multiple-thread (thread-safe, and
super-thread-safe) and the gevent one.

There are some patch mechanizm / locks  so running the test suite in wrong
combination might report wrong results.

.. warning::
    Running default test suite on your hardware will cause a lot of IO
    operations and intense CPU work for several minutes (depends on
    your hardware). Really **do not** run tests suite on environments
    with less than 4 cpu cores without clock at least 2.5GHz, also if
    you run SSD for personal use, use ramcache instead. Test suite
    performs more than 24 000 000 system calls to write, read, lseek.

.. note::
    For more details visit or clone our |cdb_repo| and check ``tox.ini`` and
    ``pytest.ini`` files.

Whole test suite contains a lot of different tests, that covers
naturally all database functions, and probably most of use-cases of them.

.. _Tox: http://tox.testrun.org/
.. _Jenkins: http://jenkins-ci.org/
.. _pytest: http://pytest.org
.. _pytest-xdist: http://pypi.python.org/pypi/pytest-xdist
