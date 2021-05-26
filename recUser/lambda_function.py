import json
import boto3
from boto3.dynamodb.conditions import Key, Attr, And, Not

rec_table = boto3.resource('dynamodb').Table('rec-table')
user_table = boto3.resource('dynamodb').Table('user-profile')
dynamodb = boto3.resource("dynamodb")
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
    if 'last_key' in event['headers']:
        resp = rec_table.query(
        Limit=50,
        ExclusiveStartKey=json.loads(event['headers']['last_key']),
        KeyConditionExpression=Key('user_id').eq(user_id)
        )
    else:
        resp = rec_table.query(
            Limit=50,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    data = resp.get('Items')
    if data == None:
        data = []
    random_flag = False
    
    data = []
    
    if len(data) == 0:
        resp = rec_table.scan(Limit=50)
        data = resp.get('Items')[:50]
        random_flag = True
    tutor_ids = list(set([d['tutor_id'] for d in sorted(data, key=lambda x: x['pred'], reverse=True)]))
    response = dynamodb.batch_get_item(
        RequestItems={
            user_table.name: {
                'Keys': [{'user_id': d} for d in tutor_ids],            
                'ConsistentRead': True            
            }
        },
        ReturnConsumedCapacity='TOTAL'
    )
    response = response.get('Responses')
    if response != None:
        data = response.get(user_table.name)
    else:
        data = []
        
    if 'LastEvaluatedKey' in resp and not random_flag:
        body = {
            'data': data,
            'LastEvaluatedKey': resp['LastEvaluatedKey']
        }
    else:
        body = {
            'data': data
        }
    return {
        'statusCode': 200,
        'body': json.dumps(body),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }