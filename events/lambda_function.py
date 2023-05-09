import json
import sys
import logging
import pymysql
import boto3
from botocore.exceptions import ClientError
import base64
import time
import smtplib

# rds settings
rds_host = "mysqlforlambda.c6cg9rtsaerr.us-east-1.rds.amazonaws.com"
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

client = boto3.client('s3')


def lambda_handler(event, context):
    print(f"The event message is : {event}")
    httpMethod = event['httpMethod']
    path = event['path']
    resource = event['resource']

    if httpMethod == 'GET':
        if resource == '/event/{eventId}/attendee':
            eventID = event["pathParameters"]["eventId"]
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT userId FROM Tickets WHERE eventID={eventID}")
                peers = [user[0] for user in cur.fetchall()]
                results = list()
                for peer in peers:
                    cur.execute(f"SELECT user_name FROM User WHERE user_uni='{peer}'")
                    name = cur.fetchone()
                    if name:
                        results.append(name[0])
                results.sort()

                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps(results)
                }
            except:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps('Error!')
                }

        elif resource == '/event/{eventId}/invite':
            # send out invitation through ses
            emails = event["queryStringParameters"]["emails"]
            userid = event["queryStringParameters"]["userid"]
            eventid = int(event["pathParameters"]["eventId"])

            emails = emails.split(",")
            email_list = []
            for i in range(0, len(emails)):
                email = emails[i]
                if email[0] == ' ':
                    email_list.append(email[1:])
                else:
                    email_list.append(email)

            # check whether email formate & columbia email or not
            for email in email_list:
                email = email.split("@")
                if len(email) < 2:
                    return {
                        'statusCode': 404,
                        'headers': {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Methods': '*',
                        },
                        'body': json.dumps('Error! wrong email format')
                    }
                elif email[1] != 'columbia.edu':
                    return {
                        'statusCode': 404,
                        'headers': {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Methods': '*',
                        },
                        'body': json.dumps('Error! only columbia emails allowed')
                    }

            # find out user and event information
            cur = conn.cursor()
            sql_string = f"select * from Eventt where eventid = {eventid}"
            cur.execute(sql_string)
            res = cur.fetchall()
            event_name = res[0][1]
            event_location = res[0][3]
            event_date = res[0][4]
            event_time = res[0][5]
            event_description = res[0][7]

            sql_string = f"select * from User where user_uni = '{userid}'"
            cur.execute(sql_string)
            res = cur.fetchall()
            user = res[0][1]

            print(email_list)

            # send out invitation through sns -> email lambda
            sns = boto3.client('sns')
            inputParams = {
                "email_list": email_list,
                "event_name": event_name,
                "event_location": event_location,
                "event_date": event_date,
                "event_time": event_time,
                "event_description": event_description,
                "user": user
            }
            inputParams = json.dumps(inputParams)
            response = sns.publish(TopicArn='arn:aws:sns:us-east-1:216308205566:emails',
                                   Message=inputParams)

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

        elif path == '/event/myevents':
            userid = event["queryStringParameters"]["userid"]
            cur = conn.cursor()
            try:
                params = (
                    'eventId', 'name', 'tags', 'location', 'date', 'time', 'capacity', 'description', 'image_url',
                    'hostid')
                cur.execute(f"SELECT * FROM Eventt WHERE userid='{userid}'")
                results = [dict(zip(params, ev)) for ev in cur.fetchall()]

                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps({'events': results})
                }
            except:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps('Error!')
                }

        elif path == '/event/findByTags':
            # search by tag attribute
            tags = event["queryStringParameters"]["tags"]
            res_return = set()
            tag_list = tags.split(",")
            for tag in tag_list:
                sql_tag = f"%{tag}%"
                print(f"tag is {tag}")
                cur = conn.cursor()
                sql_string = f"select * from Eventt where tag like '{sql_tag}'"
                cur.execute(sql_string)
                res = cur.fetchall()
                for r in res:
                    res_dict = dict()
                    res_dict['eventId'] = r[0]
                    res_dict['name'] = r[1]
                    print(f"r2 is {r[2]}")
                    tag_list_r = json.loads(r[2])
                    tag_str = ""
                    for t in tag_list_r:
                        tag_str = tag_str + t + ", "
                    tag_str = tag_str[:-2]
                    res_dict['tags'] = tag_str
                    print(res_dict['tags'])
                    print(type(res_dict['tags']))
                    res_dict['location'] = r[3]
                    res_dict['date'] = r[4]
                    res_dict['time'] = r[5]
                    res_dict['capacity'] = r[6]
                    res_dict['description'] = r[7]
                    res_dict['image_url'] = r[8]
                    res_dict['hostid'] = r[9]
                    res_return.add(json.dumps(res_dict))

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps(list(res_return))
            }

        elif resource == '/event/{eventId}':
            # search by eventid attribute
            eventid = int(event["pathParameters"]["eventId"])
            print(f"eventid is {eventid}")
            with conn.cursor() as cur:
                sql_string = f"select * from Eventt where eventid = {eventid}"
                cur.execute(sql_string)
                res = cur.fetchall()
                res_return = []
                for r in res:
                    res_dict = dict()
                    res_dict['eventId'] = r[0]
                    res_dict['name'] = r[1]
                    tag_list_r = json.loads(r[2])
                    tag_str = ""
                    for t in tag_list_r:
                        tag_str = tag_str + t + ", "
                    tag_str = tag_str[:-2]
                    res_dict['tags'] = tag_str
                    res_dict['location'] = r[3]
                    res_dict['date'] = r[4]
                    res_dict['time'] = r[5]
                    res_dict['capacity'] = r[6]
                    res_dict['description'] = r[7]
                    res_dict['image_url'] = r[8]
                    res_dict['hostid'] = r[9]
                    res_return.append(res_dict)

                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Headers': '*',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': '*',
                    },
                    'body': json.dumps(list(res_return))
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

            event_tag = "["
            for w in data["tag"].split(", "):
                event_tag += '"'
                event_tag += w
                event_tag += '"'
                event_tag += ", "
            event_tag = event_tag[:-2]
            event_tag += "]"
            print(event_tag)
            print(type(event_tag))

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
            conn.commit()

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
            event_tag = "["
            for w in data["tag"].split(", "):
                event_tag += '"'
                event_tag += w
                event_tag += '"'
                event_tag += ", "
            event_tag = event_tag[:-2]
            event_tag += "]"
            print(event_tag)
            print(type(event_tag))
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

            eventid = event["pathParameters"]["eventId"]
            cur = conn.cursor()
            sql_string = f"delete from Tickets where eventId = {eventid}"
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
                'body': json.dumps("Deleted event successfully!")
            }
