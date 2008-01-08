
from time import time
import sqlite  ## find this at http://pysqlite.sourceforge.net/
import os

class DBExcept(Exception):
    pass

class DB:
    """Database access for storing persistent data."""
    
    def __init__(self, db):
        self.db = db
        try:
            os.stat(db)
        except OSError:
            self._createNewDB(db)
        else:
            self._loadDB(db)
        
    def _loadDB(self, db):
        try:
            self.store = sqlite.connect(db=db)
            #self.store.autocommit = 0
        except:
            import traceback
            raise DBExcept, "Couldn't open DB", traceback.format_exc()
        
    def _createNewDB(self, db):
        self.store = sqlite.connect(db=db)
        s = """
            create table kv (key binary, value binary, time timestamp, primary key (key, value));
            create index kv_key on kv(key);
            create index kv_timestamp on kv(time);
            
            create table nodes (id binary primary key, host text, port number);
            
            create table self (num number primary key, id binary);
            """
        c = self.store.cursor()
        c.execute(s)
        self.store.commit()

    def getSelfNode(self):
        c = self.store.cursor()
        c.execute('select id from self where num = 0;')
        if c.rowcount > 0:
            return c.fetchone()[0]
        else:
            return None
        
    def saveSelfNode(self, id):
        c = self.store.cursor()
        c.execute('delete from self where num = 0;')
        c.execute("insert into self values (0, %s);", sqlite.encode(id))
        self.store.commit()
        
    def dumpRoutingTable(self, buckets):
        """
            save routing table nodes to the database
        """
        c = self.store.cursor()
        c.execute("delete from nodes where id not NULL;")
        for bucket in buckets:
            for node in bucket.l:
                c.execute("insert into nodes values (%s, %s, %s);", (sqlite.encode(node.id), node.host, node.port))
        self.store.commit()
        
    def getRoutingTable(self):
        """
            load routing table nodes from database
            it's usually a good idea to call refreshTable(force=1) after loading the table
        """
        c = self.store.cursor()
        c.execute("select * from nodes;")
        return c.fetchall()
            
    def retrieveValues(self, key):
        c = self.store.cursor()
        c.execute("select value from kv where key = %s;", sqlite.encode(key))
        t = c.fetchone()
        l = []
        while t:
            l.append(t['value'])
            t = c.fetchone()
        return l

    def storeValue(self, key, value):
        """Store or update a key and value."""
        t = "%0.6f" % time()
        c = self.store.cursor()
        try:
            c.execute("insert into kv values (%s, %s, %s);", (sqlite.encode(key), sqlite.encode(value), t))
        except sqlite.IntegrityError, reason:
            # update last insert time
            c.execute("update kv set time = %s where key = %s and value = %s;", (t, sqlite.encode(key), sqlite.encode(value)))
        self.store.commit()

    def expireValues(self, expireTime):
        """Expire older values than expireTime."""
        t = "%0.6f" % expireTime
        c = self.store.cursor()
        s = "delete from kv where time < '%s';" % t
        c.execute(s)
        
    def close(self):
        self.store.close()
