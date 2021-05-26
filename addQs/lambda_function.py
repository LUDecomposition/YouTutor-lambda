import json
import boto3
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import uuid

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
    # TODO implement
    access_token = event['headers']['access_token']
    try:
        resp = cognito.get_user(
            AccessToken=access_token,
        )
    except:
        return {
            'statusCode': 502,
            'body': json.dumps('Error in your login'),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_name = user['given_name'] + ' ' + user['family_name']
    user_id = user['email']
    requested_body = json.loads(event['body'])
    

    info = {
        'question_id': str(uuid.uuid1()),
        'user_id': user_id,
        'question_status': 'asked',
        'created_at': int(time.time())
    }
    if requested_body['tutor_id'] == 'Null':
        info['question_status'] = 'posted'

    for k in requested_body.keys():
        if k == 'send_email' or k == 'tutor_name':
            continue
        info[k] = requested_body[k]
        
    if requested_body['send_email']:
        recipient_name = requested_body['tutor_name']
        recipient_email = requested_body['tutor_id']
        email_title = 'YouTutor: You have a new question'
        email_body = f'''
Hello, {recipient_name}
You have a new question from {user_name}
with Title: {info['title']}
    Detail: {info['detail']}
Requested Time From: {info['start_time']}
                To: {info['end_time']}
Please login to YouTutor to Confirm or Cancel the Question Request.

Thank You!

Best,
YouTutor
'''
    

    res = table.put_item(Item=info)
    if info['question_status'] == 'posted':
        es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
        )
        # index in elastic search
        es.index(index="questions", doc_type="_doc", id=info['question_id'], body={"title":info["title"],"tags":info["tags"],"created_at":info["created_at"]})
    return {
        'statusCode': 200,
        'body': json.dumps(info),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
