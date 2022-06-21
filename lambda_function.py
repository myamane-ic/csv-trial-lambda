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
db_host  = rds_config.db_host
db_port = rds_config.port
db_name = rds_config.db_name
db_user = rds_config.db_username
db_password = rds_config.db_password

#create rds connection
try:
    constr=pyodbc.connect("DRIVER={{/msodbcsql17/lib64/libmsodbcsql-17.3.so.1.1}};SERVER={0};PORT={1};DATABASE={2};UID={3};PWD={4}".format(db_host,db_port,db_name,db_user,db_password))
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
