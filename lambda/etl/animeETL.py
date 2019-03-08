import boto3
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