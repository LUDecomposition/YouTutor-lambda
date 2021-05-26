import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key

user_table = 'user-profile'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(user_table)
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
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
        }
        }
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_id = user['email']
    update_expression = 'set '
    expression_dict = {}
    
    event['body'] = json.loads(event['body'])
    
    if event['body']['isRegister']:
        info = {}
        for k in event['body']:
            if k != 'isRegister':
                info[k] = event['body'][k]
        table.put_item(Item = info)
        
    else:
        for i in enumerate(event['body'].items()):
            idx = i[0]
            k = i[1][0]
            v = i[1][1]
    
            if k == 'user_id' or k=='isRegister':
                continue
    
            update = k+'=:val'+str(idx)+", "
            update_expression += update
            expression_dict[":val"+str(idx)] = v
        update_expression = update_expression[:-2]  # delete the last ", " in the expression
        response = table.update_item(
            Key={
                'user_id': user_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_dict,
            ReturnValues="UPDATED_NEW"
        )
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
        )
    if event['body']["tutor"]:
        
        if es.exists(index="tutors",id=user_id):
            es.update(index='tutors',doc_type='_doc',id=user_id,
                    body={"doc": {"degree":event['body']["degree"],
                    "first_name": event['body']['first_name'], "last_name": event['body']['last_name'],
                    "tags": event['body']['tags'],"school":event['body']["school"],"major":event['body']["major"]}})
        else:
            es.index(index="tutors",doc_type="_doc",id=user_id,body={
                "degree":event['body']["degree"],
                "tags": event['body']['tags'],
                "school":event['body']["school"],
                "major":event['body']["major"],
                "last_name": event['body']['last_name'],
                "first_name": event['body']['first_name']
            })
    else:
        if es.exists(index="tutors",id=user_id):
            es.delete(index="tutors", id=user_id)  
    return {
        'statusCode': 200,
        'body': json.dumps("successfully update/register your account"),
        "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
    }
    }
