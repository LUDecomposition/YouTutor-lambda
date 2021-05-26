import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr, And, Not,In, Contains

dynamodb = boto3.resource('dynamodb')
cognito = boto3.client('cognito-idp')
question_table = dynamodb.Table('question')
rec_table = boto3.resource('dynamodb').Table('rec-table')
user_table = 'user-profile'

def lambda_handler(event, context):
    access_token = event['headers']['access_token']
    try:
        resp = cognito.get_user(
            AccessToken=access_token,
        )
    except:
        return {
            'statusCode': 500,
            'body': json.dumps('Error in your login')
        }
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_id = user['email']

    if event['headers']['last_key'] != "null":
        resp = rec_table.query(
            Limit=100,
            ExclusiveStartKey=json.loads(event['headers']['last_key']),
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    else:
        resp = rec_table.query(
            Limit=100,
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    data = resp.get('Items')
    tutor_ids = []
    if data != None:
        tutor_ids = list(set([d['tutor_id'] for d in sorted(data, key=lambda x: x['pred'], reverse=True)]))
        if user_id in tutor_ids:
            tutor_ids.remove(user_id)
    
    
    
    qs=[]
    response = {} 
    if tutor_ids != []:
        response = question_table.scan(
                    Limit=100,
                    FilterExpression=And(Attr('question_status').eq('posted'), In(Attr('user_id'), tutor_ids))
                )
    if response.get('Items') != None:
        qs = response.get('Items')
    
    #for random scan now
    if qs == []:
        response = question_table.scan(
                    Limit=100,
                    FilterExpression=And(Attr('question_status').eq('posted'), Not(Attr('user_id').eq(user_id)))
                )
        if response.get('Items') != None:
            qs.extend(response.get('Items'))
    
        
    if len(qs)!=0:
        user_ids = list(set([i['user_id']for i in qs] + [i['tutor_id']for i in qs if i['tutor_id'] !='Null']))[:50]
        response = dynamodb.batch_get_item(
            RequestItems={
                user_table:{
                    "Keys": [{'user_id': user_id} for user_id in set(user_ids)],
                    'AttributesToGet': ['user_id', 'picture', 'first_name', 'last_name']
                }
            }
        )['Responses'][user_table]
        result = {i['user_id']: i for i in response}
    for d in qs:
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
    
    if 'LastEvaluatedKey' in resp:
        body = {
            'data': qs,
            'LastEvaluatedKey': resp['LastEvaluatedKey']
        }
    else:
        body = {
            'data': qs
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