import math
import dateutil.parser
import datetime
import time
import os
import logging
import json
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def push_to_sqs(QueueURL, msg_body):
    """
    :param QueueURL: url of existing SQS queue
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """
    
    sqs = boto3.client('sqs')

    queue_url = QueueURL
    try:
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'cuisine': {
                    'DataType': 'String',
                    'StringValue': msg_body['cuisine']
                },
                'city': {
                    'DataType': 'String',
                    'StringValue': msg_body['city']
                },
                'email': {
                    'DataType': 'String',
                    'StringValue': msg_body['email']
                },
                'time': {
                    'DataType': 'String',
                    'StringValue': msg_body['time']
                },
                'num_people': {
                    'DataType': 'Number',
                    'StringValue': msg_body['num_people']
                }
            },
            MessageBody=(
                'Information about the diner'
            )
        )
        print(response)
    
    except ClientError as e:
        #logging.error(e) 
        print(e)
        return None
    
    return response


def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionState':{
            'dialogAction':{
                'slotToElicit':slot_to_elicit,
                'type':"ElicitSlot",
            },
            'intent': {
                'name':intent_name,
                'slots':slots
            },
        },
        "messages":[message]
        
            
    }
    
def build_validation_result(is_valid, violated_slot, message_content):
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {
            'contentType': 'PlainText',
            'content': message_content
        }
    }

def validate_parameters(_time_, cuisine_type, city, num_people, email,date):
    
    city_types = ['manhattan', 'new york', 'ny', 'nyc']
    if not city:
        return build_validation_result(False, 'city', 'Which city do you wanna eat at?')
    
    elif city['value']['originalValue'].lower() not in city_types:
        return build_validation_result(False, 'city', 'We do not have any restaurant serving there, please enter another location')
    
    
    cuisine_types = ['chinese', 'indian', 'middle eastern', 'italian', 'mexican']
    if not cuisine_type:
        return build_validation_result(False, 'cuisine', 'What type of cuisine do you prefer to have?')
        
    elif cuisine_type['value']['originalValue'].lower() not in cuisine_types:
        return build_validation_result(False, 'cuisine', 'We do not have any restaurant that serves {}, would you like a different cuisine'.format(cuisine_type['value']['originalValue']))
    
    if not date:
        return build_validation_result(False, 'date', 'What day do you want to dine at')

    if not _time_:
        return build_validation_result(False, 'DiningTime', 'What time do you prefer?')
    
    
    if not num_people:
        return build_validation_result(False, 'NumberOfPeople', 'How many people (including you) are going?')
    
    if not email:
        return build_validation_result(False, 'email', 'Please share your email')
    
    return build_validation_result(True, None, None)


def get_restaurants(intent_request):
    """
    Performs dialog management and fulfillment for asking details to get restaurant recommendations.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """
    
    source = intent_request['invocationSource']
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        time_ = slots["DiningTime"]
        cuisine = slots["cuisine"]
        city = slots["city"]
        num_people = slots["NumberOfPeople"]
        email = slots["email"]
        date = slots["date"]
        
        
        
        validation_result = validate_parameters(time_, cuisine, city, num_people, email,date)
        if not validation_result['isValid']:
            
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionState']['sessionAttributes'],
                              intent_request['sessionState']['intent']['name'],
                              slots,
                              validation_result['violatedSlot'],
                              validation_result['message'])
        slot_dict = {
            'time': time_['value']['originalValue'],
            'cuisine': cuisine['value']['originalValue'],
            'city': city['value']['originalValue'],
            'num_people': num_people['value']['originalValue'],
            'email': email['value']['originalValue'],
            'date' : date['value']['originalValue']
        }

    res = push_to_sqs('https://sqs.us-east-1.amazonaws.com/654654501011/Dining-Concierge-Q1', slot_dict)

    #res = True
    if res:
        response = {
            'sessionState':{
                "dialogAction":
                    {
                     "fulfillmentState":"Fulfilled",
                     "type":"Close",
                     
                    },
                "intent":{
                    "name":intent_request['sessionState']['intent']['name'],
                    "slots":slots,
                    "state":"Fulfilled",
                }
            },
            "messages":
                [{
                  "contentType":"PlainText",
                  "content": "We have received your request for {} cuisine. You will recieve recommendations to your email {}. Have a great day with your group of {} on date {} from {} in the city of {}!".format(
                      cuisine['value']['originalValue'], email['value']['originalValue'], num_people['value']['originalValue'], date['value']['originalValue'], time_['value']['originalValue'], city['value']['originalValue']),
                }]
        }
    else:
        response = {
            'sessionState':{
                "dialogAction":
                    {
                     "fulfillmentState":"Fulfilled",
                     "type":"Close",
                     
                    },
                "intent":{
                    "name":intent_request['sessionState']['intent']['name'],
                    "slots":slots,
                    "state":"Close",
                }
                
            },
            "messages":
                [{
                  "contentType":"PlainText",
                  "content": "Sorry, come back after some time!",
                }]
        }

    return response

def dispatch(event):
    #logger.debug('dispatch userId={}, intentName={}'.format(event['userId'], event['currentIntent']['name']))
    intent_name = event['sessionState']['intent']["name"]
    if intent_name == 'DiningSuggestionsIntent':
        return get_restaurants(event)
    raise Exception('Intent with name ' + intent_name + ' not supported')

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    #logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)