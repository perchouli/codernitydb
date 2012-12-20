.. _speed:

Speed
====================

Average record size during that speed tests was **1524
bytes** or described separately.

Number of indexes does **not affect** get performance. It affect
insert performance because data must come through that index.

The platform used for that tests was:

* i5-2500K CPU
* 8 GB DDR3 1333 MHz of ram memory
* Hitachi HDS721010DLE630 as HDD
* Debian Wheezy 64bit


.. note::

    Please note that Operatig system and/or Python will optimize disk
    read calls, in "same ``_id`` get" test there were much less system calls to
    ``read`` function than 100 000.


Hash Index only
---------------

.. _hash_speed:

.. list-table::
   :header-rows: 1

   * - Operation
     - Number
     - Time (secs)
     - Total size
     - Total throughput [#f1]_
   * - Insert 1524b
     - 1 000 000
     - 22.79
     - 1577203 kB (~1.5 GB) [#f4]_
     - 67,58 MB/s
   * - Insert 44b
     - 1 000 000
     - 12.63
     - 94926 kB (~93 MB) [#f4]_
     - 7.34 MB/s
   * - Insert 13b
     - 1 000 000
     - 11.31
     - 68558 kB (~67 MB)
     - 5.92 MB/s
   * - Unique get  (~1524b each) [#f2]_
     - 100 000
     - 0.76
     - 148828 kB (~145 MB)
     - 191 MB/s
   * - Same ``_id`` get (1524b) [#f3]_
     - 100 000
     - 0.16
     - 148828 kB (~145 MB)
     - 906 MB/s


.. note::

    CodernityDB **never** caches disk read directly. Internal cache
    mechanizm only affects metadata lookup in database
    structure.

As you can see it's possible to reach near 100 000 per second insert operations per second (when single record has 13 bytes).


Hash + BPlusTree Index
----------------------

.. _tree_speed:

.. list-table::
   :header-rows: 1

   * - Operation
     - Number
     - Time (secs)
     - Total size
     - Total throughput [#f1]_
   * - Insert with empty storage [#f5]_
     - 100 000
     - 4.88
     - 166608 kB (~162 MB)
     - 33.34 MB/s
   * - Insert with full storage [#f6]_
     - 100 000
     - 5.75
     - 315436 kB (~308 MB)
     - 58.67 MB/s
   * - Same key get [#f7]_
     - 100 000
     - 0.16
     - 148828 kB (~145 MB)
     - 906 MB/s
   * - Random key get [#f8]_
     - 50 000
     - 2.11
     - 74414 kB (~72 MB)
     - 34.44 MB/s
   * - First 50000 records
     - 10 * 50000 [#f9]_
     - 1.46
     - 744140 kB (~726 MB)
     - 497.74 MB/s





Huge insert
-----------

By huge we mean ``10 000 000+``. Below you will find table that shows how
much it take to insert next ``100 000`` data. On the right side you
will find a hundreds of hundreds of thousands (so 195 means 19.5
milions) data that are already in database. Whole test was to insert ``40 000 000`` records. Test was performed with only **HashIndex** enabled.


::

   195 7.95013904572
   196 3.65534615517
   197 3.76729917526
   198 3.67554092407
   199 3.67811894417
   200 3.68342399597
   201 7.85825514793
   202 7.03050684929
   203 3.83275294304
   204 3.71660590172
   205 3.73785018921
   ...
   345 9.38193798065
   346 5.12198114395
   347 5.05182003975
   348 5.08870697021
   349 9.35661196709
   350 8.65566110611
   351 5.08686304092
   352 5.09454798698
   353 5.25117206573
   354 5.27701687813
   355 9.33711600304

As you can see CodernityDB performs pretty stable even on quite big number of records inside database.

We can easily compare those data to Kyoto Cabinet for example. Kyoto DB was: ``casket.kch#bnum=5000000#xmsiz=536870912``

::

    195 4.10116410255
    196 82.7376952171
    197 67.5338990688
    198 1.37760806084
    199 1.06519412994
    200 1.05488991737
    201 1.0541009903
    202 35.6051700115
    203 1.0752620697
    204 1.05934405327
    205 1.05623102188
    ...
    345 2.68368792534
    346 53.0082919598
    347 1.61376214027
    348 1.40895700455
    349 79.2329280376
    350 1.2841398716
    351 1.2099750042
    352 71.6390359402
    353 1.20401000977
    354 1.15575003624
    355 55.4045758247


*Surprised?*
^^^^^^^^^^^^

As you can see while Kyoto Cabinet is quite fast in most cases, it slowdowns **a lot** sometimes (do you know better setup that we should use? Contact us). ``autosync`` and ``autotran`` was disabled in Kyoto Cabinet. What's even more important in CodernityDB you can have more than one index (it affects performance though), so you don't have to copy your data all over single databases.

There you will find statistics for that tests:

.. list-table::
   :header-rows: 1

   * - Database
     - Min
     - Max
     - Mean
     - Std
     - Total
   * - Kyoto Cabinet
     - 0.629998922348
     - 146.798651934
     - 13.6441506329
     - 25.5503667156
     - 5471.30440378
   * - CodernityDB
     - 1.63907909393
     - 12.9895970821
     - 4.57873315585
     - 4.57873315585
     - 1836.0719955


CodernityDB slow downs on when there is a lot of records in database, but as you can see it performs pretty stable. And remember, Kyoto Cabinet is C++ database while CodernityDB is pure Python.



.. rubric:: Footnotes

.. [#f1] In get methods it doesn't mean disk I/O throughput
.. [#f2] Gets for unique records in database
.. [#f3] Gets for always the same record.
.. [#f4] That is the total size used by database
.. [#f5] Tree index doesn't store anything to storage
.. [#f6] Tree index stores the same data as *id* one (1524 B average size)
.. [#f7] Get for the same key in Tree index
.. [#f8] Gets for unique keys in Tree index. That's **worst** case for Tree Index
.. [#f9] The operation was repeated 10 times in a row

