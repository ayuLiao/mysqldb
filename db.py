# coding=utf-8

import logging
import pymysql
import time
import datetime
import uuid
from .pool import PooledConnection
from .connection import SqlExcuteError


class MySQLdb(object):
    """mysql 的数据库操作类，支持连接池"""

    def __init__(self, cfg, log=None):
        self.config = cfg
        self._pool = PooledConnection(
            self.config,
            self.config.get("maxConnections"),
            self.config.get("minFreeConnections", 1),
            self.config.get("keepConnectionAlive", False),
            self.config.get("traceLogger", None),
            log,
        )

        self._logger = log if log is not None else logging.getLogger(__name__)

    def execute(self, sql, args=None):
        """执行 sql"""

        cursor = None
        conn = None
        try:
            try:
                conn = self._pool.get_connection()
                cursor = conn.execute(sql, args)
            except (pymysql.err.OperationalError, RuntimeError):
                self._logger.warning("execute error ready to retry", exc_info=True)
                conn and conn.drop()
                conn = self._pool.get_connection()
                cursor = conn.execute(sql, args)
        except:
            self._logger.error("MySQLdb execute error", exc_info=True)
            conn and conn.drop()
            conn = None
            raise
        finally:
            conn and conn.release()

        return cursor

    def insert(self, sql, args=None):
        """插入记录"""

        cursor = None
        try:
            cursor = self.execute(sql, args)
            if cursor:
                row_id = cursor.lastrowid
                return row_id
            else:
                raise SqlExcuteError("sql error: %s" % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def update(self, sql, args=None):
        """更新记录"""

        cursor = None
        try:
            cursor = self.execute(sql, args)
            if cursor:
                row_count = cursor.rowcount
                return row_count
            else:
                raise SqlExcuteError("sql error: %s" % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def delete(self, sql, args=None):
        """删除记录"""

        cursor = None
        try:
            cursor = self.execute(sql, args)
            if cursor:
                row_count = cursor.rowcount
                return row_count
            else:
                raise SqlExcuteError("sql error: %s" % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def query(self, sql, args=None):
        """查询"""

        cursor = None
        try:
            cursor = self.execute(sql, args)
            if cursor:
                return cursor.fetchall()
            else:
                raise SqlExcuteError("sql error: %s" % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def query_one(self, sql, args=None):
        """查询返回一条数据"""

        cursor = None
        try:
            cursor = self.execute(sql, args)
            if cursor:
                return cursor.fetchone()
            else:
                raise SqlExcuteError("sql error: %s" % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def begin(self):
        """开启并返回一个事务"""

        tran = Transaction(self._pool.get_connection())
        tran.begin()

        return tran

    def commit(self, tran):
        """提交事务"""
        return tran.commit()

    def rollback(self, tran):
        """回滚事务"""
        return tran.rollback()


class Transaction(object):
    """事务类"""

    def __init__(self, conn):
        self.__is_began = False
        self.conn = conn
        self.__old_autocommit = self.conn._conn.get_autocommit()
        self.conn._conn.autocommit(False)
        self.id = None

    def begin(self):
        """开启事务"""

        if not self.__is_began:
            self.conn._conn.begin()
            self.__is_began = True
            self.id = str(uuid.uuid1())
            self.conn.set_tran_id(self.id)

    def commit(self):
        """提交事务"""

        try:
            self.conn._conn.commit()
        except:
            self.conn._conn.rollback()
            raise
        finally:
            self._end()

    def rollback(self):
        """回滚事务"""

        try:
            self.conn._conn.rollback()
        finally:
            self._end()

    def _end(self):
        """结束事务"""
        self.__is_began = False
        self.__reset_autocommit()
        self.conn.set_tran_id(None)
        self.conn.release()

    def __reset_autocommit(self):
        """将连接的自动提交设置重置回原来的设置"""
        self.conn._conn.autocommit(self.__old_autocommit)

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
