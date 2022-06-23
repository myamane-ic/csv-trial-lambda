import pyodbc
import json
import logging
import rds_config
import s3_config
import sys
import boto3
import io
import csv
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
    st = io.StringIO()
    st.write(body)
    st.seek(0)
    csv_f =csv.reader(st)
    
    taskDatas=[] #空List
    rowNumber = -1 #Csv行数カウント
    for row in csv_f: #rowはList
        rowNumber += 1
        if(rowNumber == 0):
            continue #最初の行はスキップ
        #クラス化
        taskDatas.append(TaskDetailCsvModel(row[0]))
    #csv行数確認
    print(rowNumber)
    
    # 5.以下ループ
    with conn.cursor() as cur:
        for taskData in taskDatas:
            # 5.1 子テーブルに登録
            sql = 'insert into task_detail (task_id, content) values (' + str(taskId) + ', \'' + taskData.content + '\')'
            print(sql)
            cur.execute(sql)
        conn.commit()
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Process Finished!')
    }

#csvデータクラス
class TaskDetailCsvModel:
    def __init__(self, content):
        self.content = content