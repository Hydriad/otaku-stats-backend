import boto3
import json
from botocore.vendored import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Q, A, Search, Range
import json
import datetime

region = 'us-east-1' # For example, us-west-1
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service, session_token=credentials.token)

host = 'vpc-otaku-stats-ysjtk7v43e66adxsqmpqabxttm.us-east-1.es.amazonaws.com' # For example, search-mydomain-id.us-west-1.es.amazonaws.com
index = 'series'


def buildStatsQuery(event):


    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth, 
        use_ssl=True, 
        verify_certs=True, 
        connection_class=RequestsHttpConnection)

    queryParams = event["queryStringParameters"]
    print("queryParams are: {}".format(queryParams))

    #initialize the search
    s = Search(using=es, index=index)

    #Set up the range query
    s = s.filter("range", airing_start={'from': datetime.datetime.fromtimestamp(int(queryParams["startDate"])), 'to' : datetime.datetime.fromtimestamp(int(queryParams["endDate"])) })

    s = s.filter("term", type=queryParams["type"])

    #Don't return any actual fields   
    s = s[0:0]

    #Create the genre over time histogram
    s.aggs.bucket('season_buckets', 'date_histogram', field='airing_start', interval='quarter')\
        .metric('genres', 'terms', field='genres.mal_id')\
        .metric('episode_count', 'avg', field='episodes')\
        .metric('rating', 'avg', field='score')

    s.aggs.bucket('genres_total', 'terms', field='genres.mal_id')


    return s


# Lambda execution starts here
def handler(event, context):

    print ("event is {}".format(event))
    print ("context is {}".format(context))

    search = buildStatsQuery(event)

    #Send the query to ES
    results = search.execute()

    # Add the search results to the response
    resultsString = json.dumps(results.to_dict())

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*',
            "Content-Type":"application/json",
            "Content-Length": str(len(resultsString))
        },
        "isBase64Encoded": False,
        "body" : resultsString
    }


    return response