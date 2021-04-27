# coding=utf-8

import threading
from queue import Queue,Empty
import time
import datetime
import pymysql
import logging
from .connection import Connection


class PooledConnection(object):
    """连接池"""

    def __init__(
        self, connection_strings, max_count=10, min_free_count=1, keep_conn_alive=False, trace_sql=False, log=None
    ):
        self._max_count = max_count
        self._min_free_count = min_free_count
        self._connection_strings = connection_strings
        self._count = 0
        self._queue = Queue(max_count)
        self._lock = threading.Lock()
        self.trace = trace_sql

        self._logger = log if log is not None else logging.getLogger(__name__)
        
        if keep_conn_alive:
            self._ping_interval = 300
            self._run_ping()

    def __del__(self):

        while self._queue._qsize() > 0:
            self._lock.acquire()
            try:
                conn_info = self._queue.get(block=False)
                conn = conn_info.get("connection") if conn_info else None
            except Empty:
                conn = None
            finally:
                self._lock.release()
                
            if conn:
                self._close_connection(conn)
            else:
                break

    def _run_ping(self):
        """开启一个后台线程定时 ping 连接池里的连接，保证池子里的连接可用"""
        
        def ping_conn(pool_queue, pool_lock, per_seconds,log):
            # 每5分钟检测池子里未操作过的连接进行ping操作，移除失效的连接
            pre_time = time.time()
            while True:
                if pre_time <= time.time() - per_seconds:
                    log.debug("pool connection count:(%s,%s)" % (pool_queue._qsize(), self._count))

                    # 使用 queue 的 _qsize 方法，防止queue里的lock与pool_lock造成死锁
                    while pool_queue._qsize() > 0:
                        conn = None
                        usable = True
                        pool_lock.acquire()
                        try:
                            conn_info = pool_queue.get(block=False)
                            if conn_info:
                                if conn_info.get("active_time") <= time.time() - per_seconds:
                                    conn = conn_info.get("connection")
                                    try:
                                        conn._conn.ping()
                                    except:
                                        usable = False
                                else:
                                    # 只要遇到连接的激活时间未到 ping 时间就结束检测后面的连接【Queue的特性决定了后面的连接都不需要检测】
                                    break
                        except:
                            pass
                        finally:
                            pool_lock.release()

                        # 必须放在 lock 的外面，避免在做drop和release的时候死锁
                        if conn:
                            if not usable:
                                conn.drop()
                            else:
                                conn.release()

                    pre_time = time.time()
                else:
                    time.sleep(5)

        thread = threading.Thread(target=ping_conn, args=(self._queue, self._lock, self._ping_interval,self._logger), daemon=True)
        thread.start()

    def _create_connection(self, auto_commit=True):
        if self._count >= self._max_count:
            raise PoolError("Maximum number of connections exceeded!")

        # self._logger.info("开始创建mysql连接")
        conn = Connection(
            self,
            self.trace,
            self._logger,
            host=self._connection_strings.get("host"),
            port=self._connection_strings.get("port"),
            user=self._connection_strings.get("user"),
            password=self._connection_strings.get("password"),
            db=self._connection_strings.get("database"),
            charset=self._connection_strings.get("charset", "utf8"),
            autocommit=auto_commit,
            cursorclass=pymysql.cursors.DictCursor,
        )
        # self._logger.info("完成创建mysql连接")
        self._count += 1
        return conn

    def release_connection(self, connection):
        """释放连接"""

        self._lock.acquire()
        try:
            if self._queue._qsize() >= self._min_free_count:
                self._close_connection(connection)
            else:
                self._queue.put({"connection": connection, "active_time": time.time()})
        except:
            pass
        finally:
            self._lock.release()

    def get_connection(self, timeout=15):
        """获取一个连接"""
        begin_time = time.time()

        def get_conn():
            """获取连接"""
            self._lock.acquire()
            try:
                if self._queue._qsize() > 0:
                    try:
                        conn_info = self._queue.get(block=False)
                        conn = conn_info.get("connection") if conn_info else None
                    except Empty:
                        conn = None
                elif self._count < self._max_count:
                    conn = self._create_connection()
                else:
                    conn = None
                return conn
            except:
                raise
            finally:
                self._lock.release()

        connection = get_conn()
        if connection:
            return connection
        else:
            if timeout:
                while (time.time() - begin_time) < timeout:
                    connection = get_conn()
                    if connection:
                        break
                    time.sleep(0.2)
            if not connection:
                raise PoolError(
                    "mysql pool: get connection timeout, not enough connections" +
                    " are available!(modify the maxConnections value maybe can fix it)"
                )
            return connection

    def _close_connection(self, connection):
        """关闭连接"""
        try:
            if connection._close():
                self._count -= 1
        except:
            pass


class PoolError(Exception):
    """连接异常类"""

    pass
