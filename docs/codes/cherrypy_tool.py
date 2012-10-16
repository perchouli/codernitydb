#!/usr/bin/env python

import cherrypy
from cherrypy.process import plugins
from CodernityDB.database_thread_safe import ThreadSafeDatabase as Database


class CodernityDBPlugin(plugins.SimplePlugin):
    def __init__(self, bus):
        plugins.SimplePlugin.__init__(self, bus)
        self.db = None

    def start(self):
        pass

    def setup_db(self, data, *args, **kwargs):
        if self.db is None:
            codernitydb = cherrypy.config.get('codernitydb')
            if codernitydb:
                path = codernitydb['dir']
            else:
                path = cherrypy.config['codernitydb.dir']
            db = Database(path)
            if db.exists():
                db.open()
            else:
                db.create()
                # add indexes etc...
            self.db = db

    def stop(self):
        if self.db is not None:
            self.db.close()
            self.db = None
