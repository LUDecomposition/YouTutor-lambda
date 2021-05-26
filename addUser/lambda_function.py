import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

user_table = "user-profile"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(user_table)

region = 'us-east-1'
service = 'es'
host = 'search-ccfinalsearcht-jdyfz3ale3zufejmvivdts3lea.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def lambda_handler(event, context):
    info = {}

    for k in event.keys():
        info[k] = event[k]

    try:
        if not info['user_id']:
            return {
            'statusCode': 400,
            'body': "invalid user_id: "+info['user_id']
            }
    except:
        return {
            'statusCode': 400,
            'body': "bad request no user_id."
            }

    res = table.put_item(Item=info)
    
    if info['tutor']:
        es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
        )
        # index in elastic search
        es.index(index="tutors", doc_type="_doc", id=info['user_id'], body={"degree":info["degree"],
                                "tags":info["tags"],"first_name":info["first_name"], "last_name":info["last_name"],
                                "school":info["school"],"major":info["major"]})

    return {
        'statusCode': 200,
        'body': json.dumps(info),
        "headers": {
        'Content-Type':  'application/json',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*"
        }
    }
