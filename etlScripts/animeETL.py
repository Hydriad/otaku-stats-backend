import boto3
import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from time import sleep
from jikanpy import Jikan
import logging
import argparse


region = 'us-east-1' # For example, us-west-1
service = 'es'
session = boto3.session.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, session.region_name, service, session_token=credentials.token)

host = 'vpc-otaku-stats-ysjtk7v43e66adxsqmpqabxttm.us-east-1.es.amazonaws.com' # For example, search-mydomain-id.us-west-1.es.amazonaws.com
port = 443
index = 'movies'

parser = argparse.ArgumentParser(
    description="An ETL script meant to load the entirety of the MAL db from Jikan and load it into our ElasticSearch database"
)

parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
parser.add_argument("-s", "--startyear", help="The year to start checking Jikan for anime", required=True)
parser.add_argument("-e", "--endyear", help="The year to stop checking Jikan for anime", required=True)

logging.basicConfig(level=logging.INFO)

jikan = Jikan()

es = Elasticsearch(
    hosts=[{'host': host, 'port': port}],
    use_ssl=True
)

#Main function
def main():

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if es.ping():
        logging.info("Elasticsearch was able to connect. Proceeding.")
    else:
        logging.error("Elasticsearch couldn't connect. Quitting")
        return

    startyear = int(args.startyear)
    endyear = int(args.endyear)

    logging.info("Startyear: {}, Endyear: {}".format(startyear, endyear))

    while startyear <= endyear:
        logging.info("Running loop for {}".format(startyear))
        batch = getAnimeForYear(startyear)

        sendToES(batch)
        startyear += 1




# build the anime array by subsequently querying the server
def getAnimeForYear(year):

    yearOfAnime = []

    seasons = ["summer", "spring", "fall", "winter"]

    for season in seasons:
        logging.info("Getting anime for season: {}, and year: {}".format(season, year))
        results = jikan.season(year=year, season=season)
        logging.info("Results found of length {}".format(len(results["anime"])))
        logging.debug("Resulst found are: {}".format(results))
        yearOfAnime = yearOfAnime + processData(results["anime"])
        sleep(3)

    return yearOfAnime


#process the response into something more compatible with ES
def processData(data):
    for id, anime in enumerate(data):
        data[id] = {
            "_index":"series",
            "_type":"anime",
            "_id":anime["mal_id"],
            "_source":anime
        }

    logging.debug("data is now stored in format: {}".format(data))

    return data


def sendToES(batch):

    logging.info("Writing batch to Elasticsearch")
    logging.debug("Batch is in format: {}".format(batch))

    logging.info(helpers.bulk(es, iter(batch)))

    # print (es.info());


main()