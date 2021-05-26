import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

question_table = 'question'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(question_table)
cognito = boto3.client('cognito-idp')

region = 'us-east-1'
service = 'es'
host = 'search-ccfinalsearcht-jdyfz3ale3zufejmvivdts3lea.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

def lambda_handler(event, context):
    access_token = event['headers']['access_token']
    try:
        resp = cognito.get_user(
            AccessToken=access_token,
        )
    except:
        return {
            'statusCode': 500,
            'body': json.dumps('Error in your login'),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
    
    headers = event['headers']
    
    question_keys = headers.get('question_keys')
    
    if question_keys == None:
        return  {
            'statusCode': 500,
            'body': json.dumps('request should have designited headers'),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
    question_id, created_at = question_keys.split(' ')
    
    response = table.get_item(Key={'question_id': question_id,'created_at':int(created_at)})
    response = response.get("Item")
    if not response:
        return {
            'statusCode':400,
            'body':json.dumps("Could not find this question"),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
    
    res = table.delete_item(TableName=question_table,\
            Key={
                'question_id':question_id,
                'created_at':int(created_at)
            })
    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )
    try:
        es_res = es.delete(
            index="questions",
            doc_type="_doc",
            id=question_id
        )
    except:
        pass
                
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully Canceled'),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
