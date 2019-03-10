import boto3
import json
from botocore.vendored import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Q, Search
import json

region = 'us-east-1' # For example, us-west-1
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service, session_token=credentials.token)

host = 'vpc-otaku-stats-ysjtk7v43e66adxsqmpqabxttm.us-east-1.es.amazonaws.com' # For example, search-mydomain-id.us-west-1.es.amazonaws.com
index = 'series'


def convertEventToESQuery(event):


    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth, 
        use_ssl=True, 
        verify_certs=True, 
        connection_class=RequestsHttpConnection)

    body = json.loads(event["body"])


    expressions = body["expressions"]
    print ("Expressions are: {}".format(expressions))

    operator = body["operator"]
    print ("Operator is: {}".format(operator))


    s = Search(using=es, index=index)
    q = None

    if (len(expressions) < 1):
        q = Q("match_all")
    else:
        for expression in expressions:
            formatted = {expression["l_operand"]:expression["r_operand"]}
            if q is None:
                q = Q("match", **formatted)
            elif (operator == "OR"):
                q = q | Q("match", **formatted)
            else:
                q = q & Q("match", **formatted)

    if "slice" in body and len(body["slice"]) > 1:
        sli = body["slice"]
        s = s[sli[0]:sli[1]]

    return s.query(q)


# Lambda execution starts here
def handler(event, context):

    search = convertEventToESQuery(event)

    #Send the query to ES
    results = search.execute()

    print ("Got {} hits".format(results["hits"]["total"]))


    # Add the search results to the response
    resultsString = json.dumps(results.to_dict())

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
            "Content-Type":"application/json",
            "Content-Length": str(len(resultsString))
        },
        "isBase64Encoded": False
        "body" : resultsString
    }


    return response