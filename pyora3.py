#!/usr/bin/env python
# coding: utf-8
# vim: tabstop=2 noexpandtab
"""
    Author: Danilo F. Chilene
    Email:    bicofino at gmail dot com
"""

import argparse
import cx_Oracle
import inspect
import json
import re
from os.path import getsize
import time

version = 0.2


class Checks(object):
        def show_tablespaces_used(self):
                sql = "select t.* \
                            from (SELECT D.TABLESPACE_NAME as space_name, \
                            SPACE - NVL(FREE_SPACE, 0) as used, \
                            D.SPACE as file_size, \
                            AUTOEXTENSIBLE as auto_ext, \
                            MAXBYTES as max_size \
                            FROM (SELECT TABLESPACE_NAME, \
                            AUTOEXTENSIBLE, \
                            ROUND(SUM(BYTES), 2) SPACE, \
                            ROUND(SUM(MAXBYTES),2) MAXBYTES \
                            FROM DBA_DATA_FILES \
                            GROUP BY TABLESPACE_NAME,AUTOEXTENSIBLE) D, \
                            (SELECT TABLESPACE_NAME, \
                            ROUND(SUM(BYTES), 2) FREE_SPACE \
                            FROM DBA_FREE_SPACE \
                            GROUP BY TABLESPACE_NAME) F \
                            WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME(+)) t"
                self.cur.execute(sql)
                res = self.cur.fetchall()
                key = ['{#TABLESPACE}','USED','{#FILE_SIZE}','AUTO_EXT','{#MAX_SIZE}']
                lst = []
                for i in res:
                        d = dict(zip(key, i))
                        lst.append(d)
                lst = self.tablespace_when_no(lst)
                jsObj = json.dumps(lst)
                fileObject = open(self.dr,'w')
                fileObject.write(jsObj)
                fileObject.close()
                print json.dumps({'data': lst})
                    
        def read_tablespaces(self,name,item):
		if getsize(self.dr) < 1:
                    #print getsize(self.dr)
                    time.sleep(5)
                with open(self.dr,'r') as f:
                    data = json.load(f)
                str1 = name.upper()
                str2 = item.upper()
                for i in range(len(data)):
                    if data[i]["{#TABLESPACE}"] == str1:
                        print data[i][str2]
                        break
                
        def tablespace_when_no(self, lst):
            newList = []
            for i in lst:
                if i['AUTO_EXT'] == 'NO':
                    i['{#MAX_SIZE}'] = i['{#FILE_SIZE}']
                newList.append(i)
            return newList
        
class Main(Checks):
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--username')
        parser.add_argument('--password')
        parser.add_argument('--address')
        parser.add_argument('--database')
        parser.add_argument('--port')

        subparsers = parser.add_subparsers()

        for name in dir(self):
            if not name.startswith("_"):
                p = subparsers.add_parser(name)
                method = getattr(self, name)
                argnames = inspect.getargspec(method).args[1:]
                for argname in argnames:
                    p.add_argument(argname)
                p.set_defaults(func=method, argnames=argnames)
        self.args = parser.parse_args()
        addr = self.args.address
        dbr = self.args.database
        self.dr = '/tmp/' + addr + '_' + dbr +'.json'

    def db_connect(self):
        a = self.args
        username = a.username
        password = a.password
        address = a.address
        database = a.database
        port = a.port
        self.db = cx_Oracle.connect("{0}/{1}@{2}:{3}/{4}".format(
            username, password, address, port, database))
        self.cur = self.db.cursor()
    
    def db_close(self):
        self.cur.close()
        self.db.close()

    def __call__(self):
        try:
            a = self.args
            callargs = [getattr(a, name) for name in a.argnames]
            if (a.username <> '0' and a.password <> '0' and a.address <> '0' and a.database <> '0' and a.port <> '0'):
                self.db_connect()
                con = 1
            return self.args.func(*callargs)
            if con == 1:
                self.db_close()
        except Exception, err:
            print 0
            print str(err)

if __name__ == "__main__":
    main = Main()
    main()
