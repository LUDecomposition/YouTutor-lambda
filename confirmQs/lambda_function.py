import json
import boto3
from boto3.dynamodb.conditions import Key


question_table = 'question'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(question_table)
cognito = boto3.client('cognito-idp')

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
    update_att = {
        'question_status': 'confirmed'
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
            'created_at': int(created_at),
            'question_id': question_id
        },
        UpdateExpression = update_expression,
        ExpressionAttributeValues = expression_dict,
        ReturnValues="UPDATED_NEW"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('question request confirmed'),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
