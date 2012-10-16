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

I want to contribute, what can I do?
    Just fork and create pull request on bitbucket.

I found a bug, where can I report it?
    Please report it on Bitbucket bugtracker in our repo|cdb_repo|.

I want to have a bit customized CodernityDB
    No problem, just contact us to get more details about that.