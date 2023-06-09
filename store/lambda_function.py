import json
import sys
import logging
import pymysql
import boto3

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


def getAttendee(eventId):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT userId FROM Tickets WHERE eventID={eventId}")
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


def getInventory(eventId):  # given certain eventId return remaining spots
    cur = conn.cursor()

    query = "SELECT e.capacity " \
            "FROM Eventt e " \
            f"WHERE e.eventid={eventId}"
    cur.execute(query)
    capacity = cur.fetchone()
    if capacity is None:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Invalid eventId!')
        }
    capacity = capacity[0]
    # print("cap: ", capacity)

    query = "SELECT Count(*) " \
            "FROM Tickets t " \
            "GROUP BY t.eventId " \
            f"HAVING t.eventId={eventId}"
    cur.execute(query)
    registered = cur.fetchone()
    if registered is None:
        registered = 0
    else:
        registered = registered[0]
    # print("registered: ", registered)
    remaining = capacity - registered
    # print("res from sql: ",remaining)
    res = {}
    res["remaining"] = remaining
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


def getUserEvents(userId):
    cur = conn.cursor()

    try:
        query = "SELECT e.eventid, e.name, e.date, e.time, e.userid " \
                "FROM Tickets t, Eventt e " \
                f"WHERE t.eventId=e.eventid AND t.userId='{userId}' " 
        cur.execute(query)
        registered = cur.fetchall()
        # print(registered)
        res = {}
        res['events'] = []
        for r in registered:
            data = {}
            data['eventId'] = r[0]
            data['name'] = r[1]
            data['date'] = r[2]
            data['time'] = r[3]
            data['editable'] = r[4] == userId
            res['events'].append(data)
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


def postOrder(eventId, userId):  # given eventId, userId, order a ticket and return ticketId
    cur = conn.cursor()

    if getInventory(eventId)['statusCode'] == 404:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Invalid eventId!')
        }
    elif json.loads(getInventory(eventId)['body'])['remaining'] == 0:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('The Event is Full!')
        }
    query = f"SELECT * FROM Tickets WHERE eventId={eventId} AND userId='{userId}'"
    cur.execute(query)
    isRegistered = cur.fetchone()
    if isRegistered is not None:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Already registered')
        }
    try:
        query = f"INSERT INTO Tickets (eventId, userId) VALUES ({eventId}, '{userId}')"
        cur.execute(query)
        conn.commit()
    except:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Internal Server Error! Failed to register the event!')
        }

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps('Successfully register the event!')
    }


def deleteOrder(orderId):  # given orderId, delete it from database
    cur = conn.cursor()
    try:
        query = f"DELETE FROM Tickets WHERE ticketId = {orderId}"
        cur.execute(query)
        conn.commit()

    except:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Internal Server Error! Failed to delete!')
        }

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps('Delete successfully!')
    }


def getOrderDetails(orderId):  # given orderId, return event and user
    cur = conn.cursor()
    try:
        query = f"SELECT t.eventId, t.userId FROM Tickets t WHERE ticketId = {orderId}"
        cur.execute(query)
        res = cur.fetchone()
        if res is None:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps('Invalid orderId')
            }
        else:
            info = {}
            info['eventId'] = res[0]
            info['userId'] = res[1]
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': json.dumps(info)
            }
    except:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Internal Server Error! Failed to retrieve!')
        }
        
def deleteOrder(eventId,userId):
    cur = conn.cursor()
    try:
        query = f"DELETE FROM Tickets WHERE userId='{userId}' AND eventId={eventId}"
        cur.execute(query)
        conn.commit()

    except:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Internal Server Error! Failed to delete!')
        }

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps('Delete successfully!')
    }


def lambda_handler(event, context):
    print(event)

    # create_query = "CREATE TABLE IF NOT EXISTS Tickets (" \
    #                     "ticketId INT NOT NULL AUTO_INCREMENT, " \
    #                     "eventId INT, userId VARCHAR(10), " \
    #                     "FOREIGN KEY (eventId) REFERENCES Eventt(eventid) ON DELETE CASCADE, " \
    #                     "FOREIGN KEY (userId) REFERENCES User(user_uni) ON DELETE CASCADE, " \
    #                     "PRIMARY KEY (ticketId));"

    q = "select * from Eventt"
    

    # q = "UPDATE Eventt SET location='30 Morningside Dr, New York, NY 10025' WHERE eventid>100"
    
    # q= "SELECT e.eventid, e.name, e.date, e.time, e.userid " \
    #             "FROM Tickets t, Eventt e " \
    #             f"WHERE t.eventId=e.eventid AND t.userId='qw1234' " \
    #             "UNION " \
    #             "SELECT e.eventid, e.name, e.date, e.time, e.userid " \
    #             "FROM Eventt e " \
    #             f"WHERE e.userid='qw1234'"

    cur = conn.cursor()
    cur.execute(q)
    # conn.commit()
    res = cur.fetchall()
    print("=======", res)

    # return

    if event['path'] == '/store/hasticket':
        eventId = event['queryStringParameters']['eventId']
        userId = event['queryStringParameters']['userId']
        if eventId is None or userId is None:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                },
                'body': 'Invalid eventId or userId!'
            }
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM Tickets WHERE eventId={eventId} AND userId='{userId}'")
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps(cur.fetchone()[0]>0)
        }
    elif event['path'] == '/store/attendee' and event['httpMethod'] == 'GET':
        eventId = event['queryStringParameters']['eventId']
        return getAttendee(eventId)
    elif event['path'] == '/store/deleteorder' and event['httpMethod'] == 'DELETE':
        eventId = event['queryStringParameters']['eventId']
        userId = event['queryStringParameters']['userId']
        res = deleteOrder(eventId,userId)
    elif event['path'] == '/store/inventory' and event['httpMethod'] == 'GET':
        eventId = event['queryStringParameters']['eventId']
        res = getInventory(eventId)
    elif event['path'] == '/store/mytickets' and event['httpMethod'] == 'GET':
        userId = event['queryStringParameters']['userId']
        res = getUserEvents(userId)
        print('triggered')
        print(res)
        # print('2',eventId,userId)
    elif event['path'] == '/store/order' and event['httpMethod'] == 'POST':
        eventId = event['queryStringParameters']['eventId']
        userId = event['queryStringParameters']['userId']
        res = postOrder(eventId, userId)
        # print('2',eventId,userId)
    elif event['httpMethod'] == 'DELETE':
        orderId = event['path'].split('/')[-1]
        res = deleteOrder(orderId)
        # print('3',orderId)
    elif event['resource'] == '/store/order/{orderId}' and event['httpMethod'] == 'GET':
        orderId = event['path'].split('/')[-1]
        res = getOrderDetails(orderId)
        # print('4',orderId)
    else:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps('Internal Server Error! Failed!')
        }

    return res


