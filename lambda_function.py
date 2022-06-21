import json
import logging

#log settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#rds settings
rds_host  = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

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
