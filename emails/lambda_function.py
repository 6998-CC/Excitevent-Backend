import json
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Read the input parameters
    message = event['Records'][0]['Sns']['Message']
    message = json.loads(message)
    email_list = message['email_list']
    event_name = message['event_name']
    event_location = message['event_location']
    event_date = message['event_date']
    event_time = message['event_time']
    event_description = message['event_description']
    user = message['user']
    
    print(email_list)
    
    # send out invitation through ses
    ses = boto3.client('ses')
    for email in email_list:
        try:
            print('start send out invitation')
            title = f'Excitevent: Invitation from {user}'
            message = 'Hello from Excitevent!' + '\n\n' + f'Your friend {user} invites you to {event_name} ' \
                                                        f'({event_location}) on {event_date} {event_time}.' + '\n' \
                      + f'Event Description: {event_description}' + '\n' \
                      + 'Ready to be a part of something amazing? Register now on Excitevent and be a part of this incredible event: http://excitevent-frontend.s3-website-us-east-1.amazonaws.com'
            response = ses.send_email(
                Destination={
                    'ToAddresses': [email],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': "UTF-8",
                            'Data': message,
                        },
                    },
                    'Subject': {
                        'Charset': "UTF-8",
                        'Data': title,
                    },
                },
                Source='excitevent.invitation@gmail.com'
            )
    
            print(f"the response is {response}")
        except ClientError as e:
            print(e.response['Error']['Message'])
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps('Error! unable to send invitation email')
            }
        
    return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Sent successfully.')
        }
        
        
