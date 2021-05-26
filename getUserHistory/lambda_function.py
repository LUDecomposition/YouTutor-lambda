import json
import boto3
from decimal import Decimal
import datetime
from boto3.dynamodb.conditions import Key, Attr, And, Not, Or
import dateutil.tz
eastern = dateutil.tz.gettz('US/Eastern')

dynamodb = boto3.resource('dynamodb')
user_table = 'user-profile'
question_table = dynamodb.Table('question')
cognito = boto3.client('cognito-idp')
def lambda_handler(event, context):
    is_ask = event['headers']['is_ask']
    last_key = None
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
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_id = user['email']


    if  event['headers']['last_key'] != 'null':
        last_key = json.loads(event['headers']['last_key'])
        last_key['created_at'] = Decimal(last_key['created_at'])
    
    
    if is_ask == 'true':
        if last_key:
            query = question_table.query(
                    ExclusiveStartKey=last_key,
                    IndexName="user_id",
                    KeyConditionExpression=Key('user_id').eq(user_id)
                )
        else:
            query = question_table.query(
                    IndexName="user_id",
                    KeyConditionExpression=Key('user_id').eq(user_id)
                )
    elif is_ask=='false':
        if last_key:
            query = question_table.query(
                    ExclusiveStartKey=last_key,
                    IndexName="tutor_id",
                    KeyConditionExpression=Key('tutor_id').eq(user_id)
                )
        else:
            query = question_table.query(
                    IndexName="tutor_id",
                    KeyConditionExpression=Key('tutor_id').eq(user_id)
                )
    data = query.get('Items')
    result = dict()
    if len(data) != 0 :
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
        if d['tutor_id'] != 'Null':
            d['tutor_picture'] = result[d['tutor_id']]['picture']
            d['tutor'] = result[d['tutor_id']]['first_name'] + ' ' + result[d['tutor_id']]['last_name']
            start = datetime.datetime.fromisoformat(d['start_time'])
            end = datetime.datetime.fromisoformat(d['end_time'])
            if datetime.datetime.now(tz=eastern) > end.astimezone(eastern):
                if d['status'] == 'confirmed':
                    d['status'] = 'finished'
                else:
                    d['status'] = 'expired'
            
            d['time'] = start.strftime("%H:%M") + '-' + end.strftime("%H:%M") + ' ' + end.strftime('%D')
        else:
            d['tutor_id'] = None
            d['tutor_picture'] = None
            d['tutor'] = None
        d.pop('question_status', None)
        if 'tags' not in d:
            d['tags'] = []
    
    e, p = [], []
    for i in data:
        if i['status'] in ['finished', 'expired']:
            e.append(i)
        elif i['status'] == 'canceled':
            continue
        else:
            p.append(i)
    e = sorted(e, key= lambda x: x['created_at'], reverse=True)
    p = sorted(p, key= lambda x: x['created_at'], reverse=True)
    data = p+e

    print(data)
    if 'LastEvaluatedKey' in query:
        last_key = query['LastEvaluatedKey']
        last_key['created_at'] = int(last_key['created_at'])
        body = {
            'data': data,
            'LastEvaluatedKey': last_key
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