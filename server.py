import mindsdb
import sqlalchemy
import pymysql
import json
import functions_framework
from sqlalchemy import create_engine

#initializing the db connection
def get_connection(user, password, host, port, database):
        return create_engine(
                url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)
        )

@functions_framework.http
def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Credentials': 'true' 
        }

        return (json_result, 204, headers)
        
    #mindsdb is a mysql db so these are the credentials
    user = 'aswin.kumar@metaschool.so' 
    password = 'Qnzxc91227#' 
    host = 'cloud.mindsdb.com'
    port = 3306
    database = 'mindsdb'

    try:
        engine = get_connection(user, password, host, port, database)
        print(f"Connection to the {host} for user {user} created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)

    #running the query here 
    request_json = request.get_json()
    with engine.connect() as eng:
        query = eng.execute(f"SELECT response from mindsdb.kanye_west2 WHERE text= '{request_json['message']}';")
        results = []
        for row in query:
            row_dict = dict(row)
            results.append(row_dict)

        # Create a dictionary to store the results
        result_dict = {'results': results}

        # Convert the dictionary to a JSON format
        json_result = json.dumps(result_dict, ensure_ascii=False)

        # Set CORS headers for the main request
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': 'true'
        }

        return (json_result, 200, headers)