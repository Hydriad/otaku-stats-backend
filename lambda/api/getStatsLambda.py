import boto3
import json
from botocore.vendored import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

region = 'us-east-1' # For example, us-west-1
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service, session_token=credentials.token)

host = 'vpc-otaku-stats-ysjtk7v43e66adxsqmpqabxttm.us-east-1.es.amazonaws.com' # For example, search-mydomain-id.us-west-1.es.amazonaws.com
port = "443"
index = 'movies'

# Lambda execution starts here
def handler(event, context):

    print (credentials.access_key)
    print (credentials.secret_key)
    print (credentials.token)
    print (session.region_name)

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth, 
        use_ssl=True, 
        verify_certs=True, 
        connection_class=RequestsHttpConnection)

    print (es.info());

    # # ES 6.x requires an explicit Content-Type header
    # headers = { "Content-Type": "application/json" }

    # print ("Making request with url {}, auth {}, and headers {}".format(url, awsauth, headers))
    # # Make the signed HTTP request
    # r = requests.get(url, auth=awsauth, headers=headers)

    # # Create the response and add some extra content to support CORS
    # response = {
    #     "statusCode": 200,
    #     "headers": {
    #         "Access-Control-Allow-Origin": '*'
    #     },
    #     "isBase64Encoded": False
    # }
    # print ("Response received, text is {}".format(r.text))

    # # Add the search results to the response
    # response['body'] = r.text
    # return response