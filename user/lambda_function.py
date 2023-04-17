import json
import logging
import pymysql
import re
import sys

GET = 'GET'
PUT = 'PUT'
POST = 'POST'
DELETE = 'DELETE'


def connect_to_db():
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
    else:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS User (user_uni VARCHAR(10) NOT NULL, user_name VARCHAR(22) NOT NULL, user_password VARCHAR(16) NOT NULL, user_interests VARCHAR(255), user_bio VARCHAR(255), PRIMARY KEY (user_uni))")
            conn.commit()
        logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        return conn


def lambda_handler(event, context):
    conn = connect_to_db()

    headers = {
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*'
        }
    }

    print(f"The event message is : {event}")
    httpMethod = event['httpMethod']
    resource = str(event['resource'])

    if resource == '/user/create':
        body = json.loads(event['body'])
        user_uni = body["user_uni"]
        user_name = body["user_name"]
        user_password = body["user_password"]
        user_interests = body["user_interests"]
        user_bio = body["user_bio"]
        if(user_name is None or user_uni is None or user_password is None or user_interests is None):
            return headers | {
                'statusCode': 400,
                'body': json.dumps('Bad Request!')
            }
        if not re.match(r'^[a-z]{2,3}[0-9]{1,5}$', user_uni):
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! UNI incorrect!')
            }
        if len(user_password)<6 or len(user_password)>=16:
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! Password length must be 6-15!')
            }
        if len(user_name)>=22:
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! Name length must be less than 22!')
            }
        if len(user_interests)>=255:
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! Interests length must be less than 255!')
            }
        if len(user_bio)>=255:
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! Bio length must be less than 255!')
            }
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM User WHERE user_uni = '{user_uni}'")
            res = cur.fetchone()
            if res[0] > 0:
                return headers | {
                    'statusCode': 409,
                    'body': json.dumps('User already exists!')
                }
            try:
                sql_string = f"INSERT INTO User (user_uni, user_name, user_password, user_interests, user_bio) VALUES ('{user_uni}', '{user_name}', '{user_password}', '{user_interests}', '{user_bio}')"
                cur.execute(sql_string)
                conn.commit()
            except:
                return headers | {
                    'statusCode': 500,
                    'body': json.dumps('Internal Server Error! Failed to create user!')
                }
            return headers | {
                'statusCode': 200,
                'body': json.dumps('User created successfully!')
            }
    
    elif resource == '/user/login':
        user_uni = event["queryStringParameters"]["user_uni"]
        user_password = event["queryStringParameters"]["user_password"]
        if(user_uni is None or user_password is None):
            return headers | {
                'statusCode': 400,
                'body': json.dumps('Bad Request!')
            }
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM User WHERE user_uni = '{user_uni}'")
            res = cur.fetchone()
            if res[0] == 0:
                return headers | {
                    'statusCode': 403,
                    'body': json.dumps('Forbidden!')
                }
            cur.execute(f"SELECT user_password FROM User WHERE user_uni = '{user_uni}'")
            res = cur.fetchone()
            if res[0] != user_password:
                return headers | {
                    'statusCode': 403,
                    'body': json.dumps('Forbidden! Uni/Password is incorrect!')
                }
            return headers | {
                'statusCode': 200,
                'body': json.dumps('User logged in successfully!')
            }

    elif resource == '/user/{uni}':
        user_uni = event["pathParameters"]["uni"]
        if not re.match(r'^[a-z]{2,3}[0-9]{1,5}$', user_uni):
            return headers | {
                'statusCode': 400,
                'body': json.dumps(f'Bad Request! UNI format incorrect!')
            }
        elif httpMethod == DELETE: # delete user
            user_password = event["queryStringParameters"]["user_password"]
            if(user_password is None):
                return headers | {
                    'statusCode': 400,
                    'body': json.dumps('Bad Request!')
                }
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM User WHERE user_uni = '{user_uni}'")
                res = cur.fetchone()
                if res[0] == 0:
                    return headers | {
                        'statusCode': 404,
                        'body': json.dumps('User Not Found!')
                    }
                cur.execute(f"SELECT user_password FROM User WHERE user_uni = '{user_uni}'")
                res = cur.fetchone()
                if res[0] != user_password:
                    return headers | {
                        'statusCode': 403,
                        'body': json.dumps('Forbidden! Password is incorrect!')
                    }
                try:
                    cur.execute(f"DELETE FROM User WHERE user_uni = '{user_uni}'")
                    conn.commit()
                except:
                    return headers | {
                        'statusCode': 500,
                        'body': json.dumps('Internal Server Error! Filed to create user!')
                    }
                return headers | {
                    'statusCode': 200,
                    'body': json.dumps('User deleted successfully!')
                }

        elif httpMethod == GET: # read user data
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM User WHERE user_uni = '{user_uni}'")
                res = cur.fetchone()
                if res[0] == 0:
                    return headers | {
                        'statusCode': 404,
                        'body': json.dumps('User not exists!')
                    }
                elif res[0] > 1:
                    return headers | {
                        'statusCode': 500,
                        'body': json.dumps('Internal Server Error! More than one user!')
                    }
                userinfo = dict()
                userinfo['user_uni'] = user_uni
                items = ['user_name', 'user_password', 'user_interests', 'user_bio']
                for item in items:
                    cur.execute(f"SELECT {item} FROM User WHERE user_uni = '{user_uni}'")
                    res = cur.fetchone()
                    userinfo[item] = res[0]
                return headers | {
                    'statusCode': 200,
                    'body': json.dumps(userinfo)
                }

        elif httpMethod == PUT: # update user
            body = json.loads(event['body'])
            user_name = body["user_name"]
            user_password = body["user_password"]
            user_interests = body["user_interests"]
            user_bio = body["user_bio"]
            if(user_name is None or user_password is None or user_interests is None):
                return headers | {
                    'statusCode': 400,
                    'body': json.dumps('Bad Request!')
                }
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM User WHERE user_uni = '{user_uni}'")
                res = cur.fetchone()
                if res[0] == 0:
                    return headers | {
                        'statusCode': 404,
                        'body': json.dumps('User not exists!')
                    }
                try:
                    sql_string = f"UPDATE User SET user_name = '{user_name}', user_password = '{user_password}', user_interests = '{user_interests}', user_bio = '{user_bio}' WHERE user_uni = '{user_uni}'"
                    cur.execute(sql_string)
                    conn.commit()
                except:
                    return headers | {
                        'statusCode': 500,
                        'body': json.dumps('Internal Server Error! Failed to update user!')
                    }
                return headers | {
                    'statusCode': 200,
                    'body': json.dumps('User updated successfully!')
                }
        else:
            return headers | {
                'statusCode': 501,
                'body': json.dumps('Method Not Implemented!')
            }
    else:
        return headers | {
            'statusCode': 404,
            'body': json.dumps('Resource unavailable!')
        }
