import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
import psycopg2
import threading
import time
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

from sqlalchemy import create_engine, text, event, pool
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
import sqlalchemy.pool.events

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

class DSQLSQLAlchemyPool:
    """SQLAlchemyを使用したDSQLコネクションプール"""
    
    def __init__(self, authenticator, cluster_id, endpoint, database='postgres', 
                 username='admin', pool_size=3, max_overflow=2, pool_timeout=30,
                 pool_recycle=3600):
        self.authenticator = authenticator
        self.cluster_id = cluster_id
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        
        self.engine = None
        self._init_engine()
        
    def _get_connection_url(self):
        """接続URL生成（トークンは実際の接続時に動的取得）"""
        # パスワードは後で動的に設定するためダミーを使用
        return f"postgresql://{self.username}:dummy_password@{self.endpoint}:{5432}/{self.database}"
    
    def _init_engine(self):
        """SQLAlchemy Engineを初期化"""
        connection_url = self._get_connection_url()
        
        self.engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=True,
            connect_args={
                'sslmode': 'require'
            },
            echo=False
        )
        
        # 接続作成時にトークンを動的に取得するイベントハンドラー
        @event.listens_for(self.engine, "do_connect")
        def receive_do_connect(dialect, conn_rec, cargs, cparams):
            # 新しい認証トークンを取得
            token = self.authenticator.get_auth_token(self.cluster_id, 5)
            
            # 接続パラメータを更新
            cparams['password'] = token
            
            connection_id = f"conn_{id(conn_rec)}"
            logger.info(f"新しい接続を作成中: {connection_id}")
            
            # psycopg2.connect()を直接呼び出し
            return psycopg2.connect(*cargs, **cparams)
        
        # プールイベントのログ出力
        @event.listens_for(self.engine.pool, "connect")
        def receive_connect(dbapi_conn, connection_record):
            connection_id = f"conn_{id(dbapi_conn)}"
            logger.info(f"プールに接続を追加: {connection_id}")
        
        @event.listens_for(self.engine.pool, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            connection_id = f"conn_{id(dbapi_conn)}"
            logger.info(f"プールから接続を取得: {connection_id}")
        
        @event.listens_for(self.engine.pool, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            connection_id = f"conn_{id(dbapi_conn)}"
            logger.info(f"プールに接続を返却: {connection_id}")
            
        @event.listens_for(self.engine.pool, "close")
        def receive_close(dbapi_conn, connection_record):
            connection_id = f"conn_{id(dbapi_conn)}"
            logger.info(f"接続をクローズ: {connection_id}")
    
    @contextmanager
    def get_connection(self):
        """接続を取得するコンテキストマネージャー"""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            logger.error(f"接続エラー: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    
    def get_pool_status(self):
        """プールの状態を取得"""
        try:
            pool_obj = self.engine.pool
            return f"プールサイズ:{pool_obj.size()}, チェックアウト中:{pool_obj.checkedout()}, オーバーフロー:{pool_obj.overflow()}"
        except Exception as e:
            return f"プール状態取得エラー: {e}"
    
    def close_all(self):
        """すべての接続を閉じる"""
        if self.engine:
            logger.info("全接続を閉じます")
            self.engine.dispose()

def test_sqlalchemy_pool():
    """SQLAlchemyプールのテスト"""
    CLUSTER_ID = "g4abult6r6rrzachxrubfgunia"
    ENDPOINT = "g4abult6r6rrzachxrubfgunia.dsql.ap-northeast-1.on.aws"
    
    try:
        authenticator = DSQLAuthenticator(region='ap-northeast-1')
        pool_manager = DSQLSQLAlchemyPool(
            authenticator=authenticator,
            cluster_id=CLUSTER_ID,
            endpoint=ENDPOINT,
            pool_size=3,
            max_overflow=2
        )
        
        # 停止フラグ
        stop_flag = threading.Event()
        
        def worker_thread(thread_id):
            while not stop_flag.is_set():
                try:
                    with pool_manager.get_connection() as conn:
                        result = conn.execute(
                            text("SELECT CURRENT_TIMESTAMP, :thread_msg"),
                            {"thread_msg": f"thread_{thread_id}"}
                        )
                        row = result.fetchone()
                        timestamp, thread_msg = row
                        logger.info(f"✓ スレッド{thread_id}: {thread_msg} at {timestamp}")
                        
                except Exception as e:
                    logger.error(f"✗ スレッド{thread_id} エラー: {e}")
                finally:
                    # プール状態をログに出力
                    pool_status = pool_manager.get_pool_status()
                    logger.info(f"プール状態 (スレッド{thread_id}): {pool_status}")
                
                time.sleep(120)
        
        threads = []
        for i in range(2):
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
    test_sqlalchemy_pool()