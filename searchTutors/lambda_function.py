import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key


region = 'us-east-1'
service = 'es'
host = 'search-ccfinalsearcht-jdyfz3ale3zufejmvivdts3lea.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

TABLE_NAME = 'user-profile'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
cognito = boto3.client('cognito-idp')


def lambda_handler(event, context):
    access_token = event['headers']['access_token']
    try:
        resp = cognito.get_user(
            AccessToken=access_token,
        )
    except:
        return {
            'statusCode': 502,
            'body': json.dumps('Error in your login')
        }
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_id = user['email']

    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

    keywords = event['headers']['keywords'].replace(","," ")
    
    print(keywords)
    query_body = {
        "query": {
        "multi_match" : {
        "query": keywords,
        }
    }
    }

    res = es.search(index="tutors", body=query_body)
    
    if res['hits']['total']['value'] == 0:
        return {
        'statusCode': 200,
        'body':  {'data': []},
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
    
    ids = list(set([r['_id'] for r in res.get('hits').get('hits') if r['_id'] != user_id]))
    
    if ids == []:
        return {
        'statusCode': 200,
        'body': {'data': []},
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
    
    response = dynamodb.batch_get_item(
        RequestItems={
                TABLE_NAME: {
                    'Keys': [{'user_id': d} for d in ids],            
                    'ConsistentRead': True            
                }
            },
            ReturnConsumedCapacity='TOTAL'
        )
    
    result = []
    response = response.get('Responses')
    if response != None:
        result = response.get(TABLE_NAME)
    
    print(result)
    return {
        'statusCode': 200,
        'body': json.dumps({'data': result}),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
