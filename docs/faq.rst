.. _faq:

====================
FAQ
====================


Here you will find some questions and answers


What about JOINs (known from SQL databases) ?
    You can write database function. Please see :ref:`join_like1`. Just remember CodernityDB is *not* relational database.

Will it work on Jython ?
    On 2.5 no, on 2.7 it should but currently it will not because of bug in Jython IO implementation. But currently Jython 2.7 is in alpha stage, so it will change in future.

How fast is CodernityDB ?
    It's pretty fast. It can be said that you can insert even more than 55 000 records to Hash Index every second (see :ref:`speed` for more details)

Can I add index to existing DB ?
    Yes you can, but you will need to reindex that index to have in it data that were in database already before you add that index. (see :ref:`database_indexes` for details)

What about tables or collections ?
    Everything can be done through our Index mechanism see :ref:`tables_colections_q`.

How does it compare to MongoDB, CouchDB and other "big" NoSQL databases ?
    Different purposes + different design. CodernityDB doesn't have yet any replication engine (yet?). However we are sure that there is a place for CodernityDB. Nothing is impossible in CodernityDB, because Index IS a Python class where you can do anything (if you're not a Python user we created :ref:`simple_index`). Don't try make CodernityDB relational database, it will work but its not *that*. It can act as a simple key-value database or as a database with secondary indexes (ordering / selecting etc). You can optimize IO performance by moving indexes data on different partitions. Generally the CodernityDB index mechanism is really powerful, its much more than in other databases (it's more similar to CouchDB views).

Why Python 3 is not supported ?
    Python 3 introduced many incompatible changes. In case of CodernityDB having working version for 2.x and 3.x series in the same code base without ugly hacks (big monkey-patching etc.) is almost impossible. If you're interested Python 3 version of CodernityDB contact us. Porting CodernityDB to Python 3.x is not hard. Python 3.x support in fact was never needed. That's why there is no support for it (yet?).

I want to contribute, what can I do?
    Just fork and create pull request |bitbucket_link|.

I found a bug, where can I report it?
    Please report it on Bitbucket bugtracker in our repo |bitbucket_link|.

I want to have a bit customized CodernityDB
    No problem, just contact us to get more details about that.

Can I do prefix/infix/suffix search in CodernityDB ?
    Sure! Please refer to :ref:`multiple_keys_index`. By using such method you will get very fast prefix/infix/suffix search mechanism.

What If I want to implement my own Index ?
    At first you have to remember that implementing custom index shouldn't require changes in Database itself. Because of CodernityDB design, database object tries to perform operations on particular index as fast as it's possible. Good example of such method is :ref:`multiple_keys_index`.

I want to use CodernityDB in commercial project, do I have to pay for it?
    CodernityDB is released on `Apache 2.0 license`_, it allows you to freely use it even for commercial purposes without any need to pay for it. IT'S FREE FOR COMMERCIAL USE. 


.. _Apache 2.0 license: http://www.apache.org/licenses/LICENSE-2.0.html
