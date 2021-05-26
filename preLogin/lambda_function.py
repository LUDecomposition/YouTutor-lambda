import json
import boto3
user_table = 'user-profile'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(user_table)
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
    user = {i['Name']:i['Value'] for i in resp['UserAttributes']}
    user_id = user['email']
    resp = table.get_item(Key={'user_id': user_id})
    the_user = resp.get('Item')
    if the_user:
        return {
            'statusCode': 200,
            'body': json.dumps(True),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps(False),
            "headers": {
            'Content-Type':  'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*"
            }
        }
