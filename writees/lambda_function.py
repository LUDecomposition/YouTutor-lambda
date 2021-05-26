import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


region = 'us-east-1'
service = 'es'
host = 'search-ccfinalsearcht-jdyfz3ale3zufejmvivdts3lea.us-east-1.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def lambda_handler(event, context):
    # TODO implement
    
    TABLE_NAME = 'user-profile'
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table(TABLE_NAME)
    
    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

    first_page = True
    # gather data
    while True:
        # Scan DynamoDB table
        if first_page:
            response = table.scan()
            first_page = False
        else:
            response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])
        for item in response['Items']:
            user_id = item['user_id']
            tutor = item['tutor']
            degree = item['degree']
            major = item['major']
            school = item['school']
            tags = item['tags']

            if tutor :
                es.index(index="tutors", doc_type="_doc", id=user_id, body={"degree":degree,"major":major,"school":school,"tags":tags})
        if 'LastEvaluatedKey' not in response:
            break


    TABLE_NAME = 'question'
    table = dynamodb_resource.Table(TABLE_NAME)
    first_page = True
    # gather data
    while True:
        # Scan DynamoDB table
        if first_page:
            response = table.scan()
            first_page = False
        else:
            response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])
        for item in response['Items']:
            q_id = item['question_id']
            title = item['title']
            tags = item['tags']
            created_at = item['created_at']
            es.index(index="questions", doc_type="_doc", id=q_id, body={"title":title,"tags":tags,"created_at":created_at})
        if 'LastEvaluatedKey' not in response:
            break

    
    return {
        'statusCode': 200,
        'body': tags
    }
