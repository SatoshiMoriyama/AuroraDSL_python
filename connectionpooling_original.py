import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import time
from datetime import datetime
import threading
from typing import Optional
import random
import queue

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(thread)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DSQLAuthenticator:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.dsql_client = None
        
    def _get_dsql_client(self):
        if self.dsql_client is None:
            try:
                self.dsql_client = boto3.client('dsql', region_name=self.region)
            except Exception as e:
                logger.error(f"DSQLクライアントの作成に失敗しました: {e}")
                raise
        return self.dsql_client
    
    def get_auth_token(self, cluster_identifier, expires):
        try:
            client = self._get_dsql_client()
            
            token = client.generate_db_connect_admin_auth_token(
                Hostname=cluster_identifier,
                ExpiresIn=expires
            )
            
            if not token:
                raise ValueError("認証トークンの取得に失敗しました")

            return token
            
        except NoCredentialsError:
            logger.error("AWS認証情報が設定されていません")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS API エラー: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"予期しないエラーが発生しました: {e}")
            raise

class DSQLConnectionWrapper:
    """DSQL接続のラッパークラス"""
    
    def __init__(self, connection):
        """
        Args:
            connection: psycopg2接続オブジェクト
        """
        self.connection = connection
        self.created_at = datetime.now()
        
    def is_healthy(self):
        try:
            if self.connection.closed:
                logger.warning(f"接続の健全性:closed")
                return False
                
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception as e:
            logger.warning(f"接続健全性チェック失敗: {e}")
            return False
    
    def close(self):
        """接続を閉じる"""
        try:
            if not self.connection.closed:
                self.connection.close()
        except Exception as e:
            logger.debug(f"接続クローズエラー: {e}")

class DSQLCustomConnectionPool:
    """DSQL専用のカスタムコネクションプール"""
    
    def __init__(self, authenticator, cluster_id, endpoint, minconn=1, maxconn=20, 
                 database='postgres', username='admin'):
        self.authenticator = authenticator
        self.cluster_id = cluster_id
        self.endpoint = endpoint
        self.minconn = minconn
        self.maxconn = maxconn
        self.database = database
        self.username = username
        
        self._pool = queue.Queue(maxsize=maxconn)
        self._pool_lock = threading.Lock()
        self._current_connections = 0
        
        self._init_pool()
        
    def _init_pool(self):
        """プールを初期接続で満たす"""
        logger.info(f"コネクションプールを初期化中 (minconn={self.minconn})")
        for i in range(self.minconn):
            try:
                conn_wrapper = self._create_connection()
                self._pool.put(conn_wrapper, block=False)
                self._current_connections += 1
            except Exception as e:
                logger.error(f"初期接続作成エラー {i+1}/{self.minconn}: {e}")
                
    def _create_connection(self):
        """新しい接続を作成してラッパーで包む"""
        token = self.authenticator.get_auth_token(self.cluster_id, 5)
        
        connection = psycopg2.connect(
            host=self.endpoint,
            database=self.database,
            user=self.username,
            password=token,
            port=5432,
            sslmode='require'
        )
        
        logger.debug("新しいDSQLコネクションを作成しました")
        return DSQLConnectionWrapper(connection)
    
    def get_connection(self):
        """プールから健全な接続を取得、必要に応じて新規作成"""
        with self._pool_lock:
            while True:
                try:
                    conn_wrapper = self._pool.get(block=False)
                    
                    if conn_wrapper.is_healthy():
                        connection_id = f"conn_{id(conn_wrapper.connection)}"
                        logger.info(f"プールから健全な接続を取得: {connection_id}")
                        return conn_wrapper
                    else:
                        logger.warning("不健全な接続を検出、破棄して新しい接続を作成")
                        conn_wrapper.close()
                        new_conn_wrapper = self._create_connection()
                        connection_id = f"conn_{id(new_conn_wrapper.connection)}"
                        logger.info(f"新しい接続を作成して返却: {connection_id}")
                        return new_conn_wrapper
                        
                except queue.Empty:
                    if self._current_connections < self.maxconn:
                        logger.info("プールが空、新しい接続を作成")
                        conn_wrapper = self._create_connection()
                        self._current_connections += 1
                        connection_id = f"conn_{id(conn_wrapper.connection)}"
                        logger.info(f"プール空で新しい接続を返却: {connection_id}")
                        return conn_wrapper
                    else:
                        logger.warning("接続上限に達しました。少し待ってから再試行してください")
                        raise Exception("コネクションプール上限に達しました")
    
    def put_connection(self, conn_wrapper):
        """接続をプールに戻す"""
        try:
            self._pool.put(conn_wrapper, block=False)
            logger.debug("接続をプールに戻しました")
        except queue.Full:
            logger.warning("プールが満杯、接続を破棄")
            conn_wrapper.close()
            with self._pool_lock:
                self._current_connections -= 1
    
    @contextmanager
    def get_db_connection(self):
        """コンテキストマネージャーとして接続を提供"""
        conn_wrapper = None
        try:
            conn_wrapper = self.get_connection()
            yield conn_wrapper.connection
        except Exception as e:
            logger.error(f"接続取得エラー: {e}")
            raise
        finally:
            if conn_wrapper:
                self.put_connection(conn_wrapper)
    
    def get_pool_status(self):
        """プールの状態を取得"""
        try:
            available_connections = self._pool.qsize()
            used_connections = self._current_connections - available_connections
            return f"総接続数:{self._current_connections}, 利用可能:{available_connections}, 使用中:{used_connections}"
        except Exception as e:
            return f"プール状態取得エラー: {e}"
    
    def close_all(self):
        """すべての接続を閉じる"""
        logger.info("全接続を閉じます")
        while not self._pool.empty():
            try:
                conn_wrapper = self._pool.get(block=False)
                conn_wrapper.close()
            except queue.Empty:
                break
        with self._pool_lock:
            self._current_connections = 0

class DSQLConnector:
    @contextmanager
    def connect(self, cluster_identifier, token, database='postgres', username='admin'):
        connection = None
        try:
            connection = psycopg2.connect(
                host=cluster_identifier,
                database=database,
                user=username,
                password=token,
                port=5432,
                sslmode='require'
            )
            yield connection
            
        except psycopg2.Error as e:
            logger.error(f"データベース接続エラー: {e}")
            raise
        finally:
            if connection:
                connection.close()


def test_connection_pool():
    CLUSTER_ID = "g4abult6r6rrzachxrubfgunia"
    ENDPOINT = "g4abult6r6rrzachxrubfgunia.dsql.ap-northeast-1.on.aws"

    try:
        authenticator = DSQLAuthenticator(region='ap-northeast-1')
        pool_manager = DSQLCustomConnectionPool(
            authenticator=authenticator,
            cluster_id=CLUSTER_ID,
            endpoint=ENDPOINT,
            minconn=3,
            maxconn=5
        )
        
        # 停止フラグ
        stop_flag = threading.Event()
        
        def worker_thread(thread_id):
            while not stop_flag.is_set():
                try:
                    with pool_manager.get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT CURRENT_TIMESTAMP, 'thread_' || %s;", (thread_id,))
                            result = cur.fetchone()
                            timestamp, thread_msg = result
                            logger.info(f"✓ スレッド{thread_id}: {thread_msg} at {timestamp}")
                except Exception as e:
                    logger.error(f"✗ スレッド{thread_id} エラー: {e}")
                finally:
                    # プール状態をログに出力
                    pool_status = pool_manager.get_pool_status()
                    logger.info(f"プール状態 (スレッド{thread_id}): {pool_status}")
                
                time.sleep(120)
        
        threads = []
        for i in range(1):
            thread = threading.Thread(target=worker_thread, args=(i+1,))
            thread.daemon = True
            threads.append(thread)
            thread.start()
                
        try:
            # メインスレッドを維持
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_flag.set()
                    
        # プールのクリーンアップ
        pool_manager.close_all()
        
    except Exception as e:
        logger.error(f"テストエラー: {e}")

if __name__ == "__main__":
    test_connection_pool()
