import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key


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
    tutor_name = user['given_name'] + ' ' + user['family_name']
    tutor_id = user['email']
    requested_body = json.loads(event['body'])
    
    info = {
        'tutor_id': tutor_id,
        'start_time': requested_body['start_time'],
        'end_time': requested_body['end_time'],
        'question_id': requested_body['question_id'],
        'created_at': requested_body['created_at'],
        'status': requested_body['status']
    }
    update_att = {
        'tutor_id': tutor_id,
        'start_time': requested_body['start_time'],
        'end_time': requested_body['end_time'],
        'question_status': requested_body['status']
    }
    update_expression = 'SET'
    expression_dict = {}
    c = 0
    for k in update_att:
        expression_dict_k = f':var{c}'
        update_expression += ' ' + k + '= '+expression_dict_k + ','
        expression_dict[expression_dict_k] = update_att[k]
        c += 1
    update_expression = update_expression[:-1]
    
    response = table.update_item(
        Key={
            'created_at': info['created_at'],
            'question_id': info['question_id']
        },
        UpdateExpression = update_expression,
        ExpressionAttributeValues = expression_dict,
        ReturnValues="UPDATED_NEW"
    )
    if requested_body['send_email']:
        recipient_name = requested_body['user_name']
        recipient_email = requested_body['user_id']
        email_title = 'YouTutor: Your question has an update'
        email_body = f'''
Hello, {recipient_name}
Your question has been accepted by {tutor_name}

Requested Time From: {info['start_time']}
                To: {info['end_time']}
Please login to YouTutor to Confirm or Cancel the Question Request.

Thank You!

Best,
YouTutor
'''            
        print(email_body) 
        
        
    # delete the accepted Q in es
    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )
    es.delete(index="questions", doc_type="_doc", id=info['question_id'])
    
    return {
        'statusCode': 200,
        'body': json.dumps('success'),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
