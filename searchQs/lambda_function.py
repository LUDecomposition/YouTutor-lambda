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

TABLE_NAME = 'question'
user_table = 'user-profile'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
cognito = boto3.client('cognito-idp')

def lambda_handler(event, context):
    # TODO implement
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
    theuser = user_id

    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

    keywords = event['headers']['keywords'].replace(","," ")
    query_body = {
        "query": {
        "multi_match" : {
          "query": keywords,
        }
      }
    }
    res = es.search(index="questions", body=query_body)
    
    if res['hits']['total']['value'] == 0:
        return {
        'statusCode': 200,
        'body': {"data":[]},
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
    
    ids = list(set([r['_id'] for r in res.get('hits').get('hits') if r['_id'] != user_id]))
    created_ats = list(set([r['_source']['created_at'] for r in res.get('hits').get('hits') if r['_id'] != user_id]))
    response = dynamodb.batch_get_item(
    RequestItems={
            TABLE_NAME: {
                'Keys': [{'question_id': d, 'created_at': c} for d,c in zip(ids, created_ats)],            
                'ConsistentRead': True            
            }
        },
        ReturnConsumedCapacity='TOTAL'
    )
        
    
    data = []
    response = response.get('Responses')
    if response != None:
        data = response.get(TABLE_NAME)
    
    if len(data)!=0:
        user_ids = [i['user_id']for i in data] + [i['tutor_id']for i in data if i['tutor_id'] !='Null']
        response = dynamodb.batch_get_item(
            RequestItems={
                user_table:{
                    "Keys": [{'user_id': user_id} for user_id in set(user_ids)],
                    'AttributesToGet': ['user_id', 'picture', 'first_name', 'last_name']
                }
            }
        )['Responses'][user_table]
        result = {i['user_id']: i for i in response}
    for d in data:
        d['created_at'] = int(d['created_at'])
        d['user_picture'] = result[d['user_id']]['picture']
        d['user'] = result[d['user_id']]['first_name'] + ' ' + result[d['user_id']]['last_name']
        d['status'] = d['question_status']
        d['tutor_id'] = None
        d['tutor_picture'] = None
        d['tutor'] = None
        d.pop('question_status', None)
        if 'tags' not in d:
            d['tags'] = []
    
    data = [d for d in data if d['user_id'] != theuser]
    
    
    return {
        'statusCode': 200,
        'body': json.dumps({'data': data}),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
