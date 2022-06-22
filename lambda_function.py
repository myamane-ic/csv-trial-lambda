import pyodbc
import json
import logging
import rds_config
import s3_config
import sys
import boto3
import datetime

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
src_file_encoding=s3_config.src_file_encoding
bucket_name = s3_config.bucket_name
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)

def lambda_handler(event, context):

    #APIからのRequest内容を出力
    print(event)
    taskId = event['taskId']
    print(taskId)

    #SQL開始
    dt_now = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    logger.info("SQL Start: "+dt_now)
    with conn.cursor() as cur:
        # 1.taskIdを基に親レコード検索
        sql = 'select * from task where id = \'' + taskId + '\''
        print(sql)
        cur.execute(sql)
        result = cur.fetchone()
        print(result)
        
    # 2.file_idカラムからファイル名取得
    fileName = result.file_id
    print(fileName)

    # 3.S3からファイルDL
    object = bucket.Object(fileName)
    response = object.get()
    body = response['Body'].read().decode(src_file_encoding)
    
    print(body)
    # 4.ファイル内容をクラスに置き換え
    # 5.以下ループ
    # 5.1 子テーブルに登録
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
