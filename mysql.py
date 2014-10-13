#! /usr/bin/python2.6
import MySQLdb


# MySqldb class
# Author: leixu@google.com


class MySql:
    # connect to mysql using MySqlDB package

    def __init__(self, host, user, passwd, database):
        try:
            self.instance = MySQLdb.connect(
                host = host,
                user = user,
                passwd = passwd,
                db = database,
                charset="utf8")
        except:
            print "Error to get mysql instance"
        else:
            self.mysql = self.instance.cursor()

    def execute(self, sql):
        self.mysql.execute(sql)

    def insert(self, table, par):
        count = ",".join(map(lambda x: "%s", range(len(par))))
        sql = "insert into {1} values({0})".format(count, table)
        self.mysql.execute(sql, par)

    def select(self, table, par, filter_set=None, filter_value=None):
        if filter_set:
            sql = "select {0} from {1} where {2}=%s".format(
                par, table, filter_set)
            self.mysql.execute(sql, filter_value)
        else:
            sql = "select {0} from {1}".format(par, table)
            self.mysql.execute(sql)
        return [item[0] for item in self.mysql.fetchall()]

    def distinct(self, table, col):
        sql = "select distinct {0} from {1}".format(col, table)
        self.mysql.execute(sql)
        return [item[0] for item in self.mysql.fetchall()]

    def update(self, set, where, table, par):
        sql = "update {2} set {0}=%s where {1}=%s".format(set, where, table)
        self.mysql.execute(sql, par)

    def getMaxID(self, table):
        self.select(table, "*")
        return int(self.mysql.rowcount) + 1

    def removeData(self, table):
        sql = "delete from {}".format(table)
        self.mysql.execute(sql)

    def removeTable(self, table):
        sql = "drop table {}".format(table)
        self.mysql.execute(sql)

    def quit(self):
        self.instance.commit()
        self.mysql.close()
        self.instance.close()
        print "Operation sccuessfull"
