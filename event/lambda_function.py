import json
import sys
import logging
import pymysql
import boto3
import base64
import time

# rds settings
rds_host  = "mysqlforlambda.c6cg9rtsaerr.us-east-1.rds.amazonaws.com"
user_name = "admin"
password = "Cloud6998!"
db_name = "ExampleDB"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

client=boto3.client('s3')


def lambda_handler(event, context):
    print(f"The event message is : {event}")
    httpMethod = event['httpMethod']
    path = event['path']
    resource = event['resource']


    if httpMethod == 'GET':
        # if path == '/event/findByStatus':
        #     # search by status attribute
        #     status = event["queryStringParameters"]["status"]
        #     print(f"status is {status}")
        #     with conn.cursor() as cur:
        #         sql_string = f"select * from Event where status = '{status}'"
        #         cur.execute(sql_string)
        #         res = cur.fetchall()

        #         return {
        #             'statusCode': 200,
        #             'headers': {
        #                 'Content-Type': 'application/json',
        #                 'Access-Control-Allow-Headers': '*',
        #                 'Access-Control-Allow-Origin': '*',
        #                 'Access-Control-Allow-Methods': '*',
        #             },
        #             'body': json.dumps(res)
        #         }
                
        if path == '/event/findByTags':
            # search by tag attribute
            tags = event["queryStringParameters"]["tags"]
            res = set()
            tag_list = tags.split(",")
            for tag in tag_list:
                sql_tag = f"%{tag}%"
                print(f"tag is {tag}")
                cur = conn.cursor()
                sql_string = f"select * from Eventt where tag like '{sql_tag}'"
                cur.execute(sql_string)
                r = cur.fetchall()
                for c in r:
                    res.add(c)

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps(list(res))
            }
                
        elif resource == '/event/{eventId}':
            # search by eventid attribute
            eventid = int(event["pathParameters"]["eventId"])
            print(f"eventid is {eventid}")
            with conn.cursor() as cur:
                sql_string = f"select * from Eventt where eventid = {eventid}"
                cur.execute(sql_string)
                res = cur.fetchall()

                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps(res)
                }
            
    
    elif httpMethod == 'POST':
        if path == '/event/createEvent':
            # create an event
            # load event attributes
            data = json.loads(event["headers"]["x-amz-meta-name"])
            print(data)
            print(type(data))
            event_name = data["name"]
            # event_status = data["status"]
            event_tag = json.dumps(data["tag"])
            event_location = data["location"]
            event_date = data["date"]
            event_time = data["time"]
            event_capacity = data["capacity"]
            event_description = data["description"]
            event_userid = data["userid"]
            
             # upload image to S3 and obtain url
            key = str(time.time()).replace('.', '') + ".PNG"
            ms = base64.b64decode(event['body'])
            response = client.put_object(
                Body=ms,
                Bucket='excitevent-event-image',
                Key=key,
                ContentType='image/png',
            )

            event_image_url = "https://excitevent-event-image.s3.amazonaws.com/" + key

            # load the event to table
            cur = conn.cursor()
            sql_create_string = "create table if not exists Eventt (" \
                                "eventid int NOT NULL AUTO_INCREMENT, " \
                                "name varchar(255) NOT NULL, " \
                                "tag varchar(255)," \
                                "location varchar(255)," \
                                "date varchar(255)," \
                                "time varchar(255)," \
                                "capacity int," \
                                "description varchar(3000)," \
                                "event_image_url varchar(255)," \
                                "userid VARCHAR(10), FOREIGN KEY (userid) REFERENCES User(user_uni) ON DELETE CASCADE, PRIMARY KEY (eventid))"
            sql_string = f"insert into Eventt (name, tag, location, date, time, capacity, description, event_image_url, userid) values('{event_name}', '{event_tag}', '{event_location}', '{event_date}', '{event_time}', {event_capacity}, '{event_description}', '{event_image_url}', '{event_userid}')"
            # sql_string = "select * from Eventt"
            cur.execute(sql_create_string)
            conn.commit()
            cur.execute(sql_string)
            res = cur.fetchall()


            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps("Created successfully!")
            }
        
        elif resource == '/event/{eventId}':
            # update an event 
            eventid = int(event["pathParameters"]["eventId"])
            data = json.loads(event["headers"]["x-amz-meta-name"])
            event_name = data["name"]
            # event_status = data["status"]
            event_tag = json.dumps(data["tag"])
            event_location = data["location"] 
            event_date = data["date"]
            event_time = data["time"]
            event_capacity = data["capacity"]
            event_description = data["description"]
            event_userid = data["userid"]
    
            # upload image to S3 and obtain url
            key = str(time.time()).replace('.', '') + ".PNG"
            ms = base64.b64decode(event['body'])
            response = client.put_object(
                Body=ms,
                Bucket='excitevent-event-image',
                Key=key,
                ContentType='image/png',
            )
    
            event_image_url = "https://excitevent-event-image.s3.amazonaws.com/" + key
    
            # load the event to table
            cur = conn.cursor()
            sql_string = f"update Eventt " \
                         f"set name = '{event_name}', " \
                         f"userid = '{event_userid}', " \
                         f"tag = '{event_tag}', " \
                         f"location = '{event_location}', " \
                         f"date = '{event_date}', " \
                         f"time = '{event_time}', " \
                         f"capacity = {event_capacity}, " \
                         f"description = '{event_description}', " \
                         f"event_image_url = '{event_image_url}' " \
                         f"where eventid = {eventid}"
            cur.execute(sql_string)
            conn.commit()
    
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps("Updated successfully!")
            }
            
    elif httpMethod == 'DELETE':
        if resource == '/event/{eventId}':
            # delete event
            eventid = int(event["pathParameters"]["eventId"])
            print(f"eventid is {eventid}")
            cur = conn.cursor()
            sql_string = f"delete from Eventt where eventid = {eventid}"
            cur.execute(sql_string)
            conn.commit()

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps("Deleted successfully!")
            }


