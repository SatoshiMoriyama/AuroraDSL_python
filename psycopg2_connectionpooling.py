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

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(thread)d - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
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

class DSQLPoolManager:
    def __init__(self, authenticator, cluster_id, endpoint, minconn=1, maxconn=20, 
                 database='postgres', username='admin'):
        self.authenticator = authenticator
        self.cluster_id = cluster_id
        self.endpoint = endpoint
        self.minconn = minconn
        self.maxconn = maxconn
        self.database = database
        self.username = username
        self.pool = None
        self._lock = threading.Lock()
        
        # 初期プール作成
        self._init_pool()
        
    def _init_pool(self):
        token = self.authenticator.get_auth_token(self.cluster_id, 120)                 
        try:
            if self.pool:
                self.pool.closeall()
                
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                self.minconn,
                self.maxconn,
                host=self.endpoint,
                database=self.database,
                user=self.username,
                password=token,
                port=5432,
                sslmode='require'
            )
            
        except Exception as e:
            raise
    
    def _create_single_connection(self):
        token = self.authenticator.get_auth_token(self.cluster_id, 120)
        
        connection = psycopg2.connect(
            host=self.endpoint,
            database=self.database,
            user=self.username,
            password=token,
            port=5432,
            sslmode='require'
        )
        
        logger.info("新しい接続の個別作成が完了しました")
        return connection
    
    @contextmanager
    def get_db_connection(self):
        connection = None
        try:
            if not self.pool:
                raise Exception("コネクションプールが初期化されていません")
                
            connection = self.pool.getconn()
            if connection.closed:
                logger.warning("閉じられた接続を検出、個別で再作成します")
                self.pool.putconn(connection, close=True)
                connection = self._create_single_connection()
                
            try:
                with connection.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            except Exception as e:
                logger.warning(f"接続健全性チェック失敗: {e}")
                self.pool.putconn(connection, close=True)
                connection = self._create_single_connection()
                
            yield connection
            
        except Exception as e:
            logger.error(f"接続取得エラー: {e}")
            raise
        finally:
            if connection:
                try:
                    self.pool.putconn(connection)
                except Exception as e:
                    logger.warning(f"putconn失敗: {e}")
                    connection.close()
    
    def get_pool_status(self):
        if not self.pool:
            return "プールなし"
        try:
            # ThreadedConnectionPoolの内部状態を取得
            total_connections = len(self.pool._pool) + len(self.pool._used)
            available_connections = len(self.pool._pool)
            used_connections = len(self.pool._used)
            return f"総接続数:{total_connections}, 利用可能:{available_connections}, 使用中:{used_connections}"
        except Exception as e:
            return f"プール状態取得エラー: {e}"
    
    def close_all(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

def test_connection_pool():
    CLUSTER_ID = "g4abult6r6rrzachxrubfgunia"
    ENDPOINT = "g4abult6r6rrzachxrubfgunia.dsql.ap-northeast-1.on.aws"

    try:
        authenticator = DSQLAuthenticator(region='ap-northeast-1')
        pool_manager = DSQLPoolManager(
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
                
                time.sleep(600)
        
        threads = []
        for i in range(3):
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
