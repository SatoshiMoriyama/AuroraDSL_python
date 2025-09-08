import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
import psycopg2
from contextlib import contextmanager
import time
from datetime import datetime

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

def main():
    CLUSTER_ID = "g4abult6r6rrzachxrubfgunia"
    ENDPOINT = "g4abult6r6rrzachxrubfgunia.dsql.ap-northeast-1.on.aws"

    try:
        authenticator = DSQLAuthenticator(region='ap-northeast-1')
        token = authenticator.get_auth_token(CLUSTER_ID, 120)

        connector = DSQLConnector()
        
        try:
            with connector.connect(ENDPOINT, token) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    logger.info(f"✓ 開始")
                    cur.execute("BEGIN;")
                    cur.execute("SELECT pg_sleep(600);")
                    cur.execute("COMMIT;")
                    logger.info(f"✓ 終了")
        except Exception as e:
            logger.error(f"✗ DB接続エラー: {e}")
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()