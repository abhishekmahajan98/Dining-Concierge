import json
import boto3

print('Loading function')

def lambda_handler(event, context):

    client = boto3.client('lexv2-runtime')
    
    try:
        body_json = json.loads(event['body'])
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': 'Invalid JSON format in the request body'
        }
    
    try:
        text_value = body_json['messages'][0]['unstructured']['text']
    except (KeyError, IndexError):
        return {
            'statusCode': 400,
            'body': 'Invalid JSON structure or missing expected fields'
        }
    
    try:
        response = client.recognize_text(
            botId='DL3BR2HLED',
            botAliasId='4QWJRRSKOR',
            localeId='en_US',
            sessionId='101',
            text= text_value)
            
    except client.exceptions.AccessDeniedException:
        return {
            "statusCode": 403,
            "body": {
                "code": 403,
                "message": "Unauthorized: Access to the Lex service is denied"
            }
        }
    except client.exceptions.InternalServerException:
        return {
            "statusCode": 500,
            "body": {
                "code": 500,
                "message": "Internal Server Error: Lex service encountered an internal server error"
            }
        }
    
    response_data = {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "example_id",
                    "text": response['messages'][0]['content'],
                    "timestamp": "2024-02-18T12:00:00Z"
                }
            }
        ]
    }
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        },
        "body": json.dumps(response_data)
    }
