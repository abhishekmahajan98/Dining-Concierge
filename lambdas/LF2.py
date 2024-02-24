import datetime
import boto3
from boto3.dynamodb.conditions import Key
import json
from botocore.exceptions import ClientError
import requests
import decimal
from aws_requests_auth.aws_auth import AWSRequestsAuth
from opensearchpy import OpenSearch, RequestsHttpConnection
import os
import random


def es_search(host, query):
    awsauth = AWSRequestsAuth(aws_access_key='API KEY',
                      aws_secret_access_key='API-KEY',
                      aws_host=host,
                      aws_region='us-east-1',
                      aws_service='es')
    
    # # use the requests connection_class and pass in our custom auth class
    esClient = OpenSearch(
        hosts=[{'host': host,'port':443}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        http_auth=awsauth,
        timeout=30,
        max_retries=10,
        retry_on_timeout=True
    )
    
    es_result=esClient.search(index="restaurants", body=query)    # response=es.get()
    return es_result

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(0,len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj
        
def get_dynamo_data(dynno, table, key):
    response = table.get_item(Key={'id':str(key)})
    '''response = table.query(
        KeyConditionExpression=Key('id').eq(key)
    )'''
    response = replace_decimals(response)
    name = response['Item']['name']
    address_list = response['Item']['address']
    return 'Name:{}.\nAddress:{}\n'.format(name, address_list)

def get_sqs_data(queue_URL):
    sqs = boto3.client('sqs')
    queue_url = queue_URL
    
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'time', 'cuisine', 'city', 'num_people', 'email'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        messages = response['Messages'] if 'Messages' in response.keys() else []
        for message in messages:
            receiptHandle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_URL, ReceiptHandle=receiptHandle)
        return messages
    except ClientError as e:
        return []
        
def send_email(res_list,email):
    ses = boto3.client('ses')
    subject = 'Restaurant recommendations from Dining Concierge bot'
    body='Your Restaurant Suggestions are:\n'
    for i,res in enumerate(res_list):
        body= body+ str(i+1)+". "+res
    message = {
    'Subject': {
        'Data': subject,
        'Charset': 'UTF-8'
    },
    'Body': {
        'Text': {
            'Data': body,
            'Charset': 'UTF-8'
            }
        }
    }
    
    # Send the email
    ses.send_email(
        Source='codemahajan@gmail.com',
        Destination={
            'ToAddresses': [email]
        },
        Message=message
    )
    
def generate_random_integers(n):
    return [random.randint(0, n) for _ in range(5)]
    
def get_elements_at_indexes(indexes, my_list):
    return [my_list[i] for i in indexes]
    
def lambda_handler(event, context):

    messages = get_sqs_data('https://sqs.us-east-1.amazonaws.com/654654501011/Dining-Concierge-Q1')
    table_name = 'yelp-restaurants'
    db = boto3.resource('dynamodb')
    table=db.Table('yelp-restaurants-1')
    es_host = 'search-restaurants-dining-concierge-belqbiq3k2ouavwqcinuqjb33y.us-east-1.es.amazonaws.com'

    for message in messages:
        msg_attributes=message['MessageAttributes']
        query = {
            "query": {
                "match": {
                    "cuisine": msg_attributes["cuisine"]["StringValue"]
                }
            },
            "size":1000
        }
        es_search_result = es_search(es_host, query)
        number_of_records_found = int(es_search_result["hits"]["total"]["value"])
        hits = es_search_result['hits']['hits']
        suggested_restaurants = []
        idx = generate_random_integers(number_of_records_found)
        hits_res = get_elements_at_indexes(idx,hits)
        for hit in hits_res:
            id = hit['_source']['id']
            #print(id)
            suggested_restaurant = get_dynamo_data(db, table, id)
            suggested_restaurants.append(suggested_restaurant)
        print(msg_attributes["email"]["StringValue"])
        send_email(suggested_restaurants,msg_attributes["email"]["StringValue"])
