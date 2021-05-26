import json
import boto3
user_table = boto3.resource('dynamodb').Table('user-profile')
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
    
    user_id = event['headers']['user_id']
    resp = user_table.get_item(Key = {'user_id': user_id})
    data = resp.get('Item')
    
    print(data)
    
    return {
        'statusCode': 200,
        'body': json.dumps(data),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
