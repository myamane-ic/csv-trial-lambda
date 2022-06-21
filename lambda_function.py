import pyodbc
import json
import logging
import rds_config
import s3_config
import sys
import boto3

#log settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#rds settings
driver = '{ODBC Driver 17 for SQL Server}'
sqlServer = rds_config.db_host
sqlDatabase = rds_config.db_name
sqlPort = rds_config.db_port
sqlUsername = rds_config.db_username
sqlPassword = rds_config.db_password

#create rds connection
print(pyodbc.drivers())
print('Attempting Connection...')
try:
    conn = pyodbc.connect(f"DRIVER={driver};SERVER={sqlServer};PORT={sqlPort};DATABASE={sqlDatabase};UID={sqlUsername};PWD={sqlPassword}");
except pyodbc.Error as e:
    logger.error("ERROR: Unexpected error: Could not connect to SQLServer instance.")
    logger.error(e)
    sys.exit()
logger.info("SUCCESS: Connection to RDS SQLServer instance succeeded")

#s3 setting
s3 = boto3.resource('s3')
src_file_encoding=s3_config.src_file_encoding

def lambda_handler(event, context):
    # APIからのRequestBodyを出力
    print(event['body'])
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    # 1.taskIdを基に親レコード検索
    # 2.file_idカラムからファイル名取得
    # 3.S3からファイルDL
    # 4.ファイル内容をクラスに置き換え
    # 5.以下ループ
    # 5.1 子テーブルに登録
