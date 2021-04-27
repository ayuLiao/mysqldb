# coding=utf-8

import pymysql
import re
import uuid
import logging


class Connection(object):
    """连接类"""

    # PARAMERTS_REG = re.compile(r'\:([_0-9]*[_A-z]+[_0-9]*[_A-z]*)')
    PARAMERTS_REG = re.compile(r'[\'"\\]?:([_0-9]*[_A-z]+[_0-9]*[_A-z]*)[\'"\\]?')

    def __init__(self, pool, trace_logger_setting=None, log=None, *args, **kwargs):
        self._pool = pool
        self.id = uuid.uuid4()
        self._logger = log if log is not None else logging.getLogger(__name__)
        self._trace_logger = self.__create_trace_logger(trace_logger_setting) if trace_logger_setting else None
        self.tran_id = None   # 事务id

        # 连不上数据库时，自动重试
        try:
            self._conn = pymysql.connections.Connection(*args, **kwargs)
            self.__is_closed = False
        except pymysql.err.OperationalError:
            self._conn = pymysql.connections.Connection(*args, **kwargs)
            self.__is_closed = False

    def __del__(self):
        """销毁连接"""

        self.drop()

    def __create_trace_logger(self,trace_logger_setting):
        """创建一个 sql 跟踪 logger"""

        LOG_FILENAME = 'mysqldb.log'
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(process)d-%(threadName)s - '
                                      '%(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILENAME, maxBytes=10485760, backupCount=5, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def set_tran_id(self, tran_id):
        """设置事务id"""
        self.tran_id = tran_id

    def execute(self, sql, args=None):
        """执行 sql"""

        cursor = self._conn.cursor()
        # sql_text = self.PARAMERTS_REG.sub(r'%(\1)s', sql)
        # model_attrs = []
        # result = self.PARAMERTS_REG.finditer(sql)
        # for match in result:
        #     model_attrs.append(match.group(1))
        #
        # if self._trace_logger:
        #     # 记录sql语句，后续做执行性能跟踪
        #     self._trace_logger.debug({'sql':sql,'args':args,'tran_id':self.tran_id})
        #
        # def filter_args(attrs, model):
        #     """过滤参数"""
        #     if model is None:
        #         return None
        #     return {a: model[a] for a in attrs}
        # if args and isinstance(args, list):
        #     cursor.executemany(sql_text, [filter_args(model_attrs, a) for a in args])
        # else:
        #     cursor.execute(sql_text, filter_args(model_attrs, args))
        cursor.execute(sql)
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
                raise SqlExcuteError('sql error: %s' % sql)
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

                if not row_count:
                    self._logger.debug(cursor._last_executed)

                return row_count
            else:
                raise SqlExcuteError('sql error: %s' % sql)
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
                row_id = cursor.rowcount
                return row_id
            else:
                raise SqlExcuteError('sql error: %s' % sql)
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
                raise SqlExcuteError('sql error: %s' % sql)
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
                raise SqlExcuteError('sql error: %s' % sql)
        except:
            raise
        finally:
            cursor and cursor.close()

    def release(self):
        """释放连接，将连接放回连接池"""

        self._pool.release_connection(self)

    def close(self):
        """释放连接，将连接放回连接池"""

        self.release()

    def drop(self):
        """丢弃连接"""

        self._pool._close_connection(self)

    def _close(self):
        """真正关闭"""

        if self.__is_closed:
            return False
        try:
            self.__is_closed = True
            self._conn.close()
        except:
            self._logger.error('mysql connection close error', exc_info=True)

        return True

class SqlExcuteError(Exception):
    """sql执行异常类"""
    pass
