import configparser
import logging
import json
import argparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Row
from sqlalchemy.exc import OperationalError

from google.cloud.pubsub_v1 import PublisherClient

# config
config = configparser.ConfigParser()
config.read("submission_published.ini")

# arg parser
parser = argparse.ArgumentParser(
    description='Push submission publication data to GCP pub/sub',
)
parser.add_argument('-n', '--dry-run', 
                    action='store_true',
                    help='Log sql output without actually sending it')
args = parser.parse_args()

# logging
logger = logging.getLogger("publish_submission_event")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(config["log_path"])
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
logger.addHandler(fh)
logger.addHandler(ch)

# event publisher
publisher = PublisherClient()
topic_path = publisher.topic_path(config['project_id'], config['topic_id'])

# db connection
try:
    engine = create_engine(
            f"mysql+pymysql://{config['db_username']}:{config['db_password']} \
            @{config['db_host']}/{config['db_database_name']}")
except OperationalError as e:
    raise Exception(f"Failed to connect to db {config['db_host']}/{config['db_database_name']}") from e

# get rows
query = 'select submission_id, document_id, paper_id, version, type \
         from arXiv_next_mail \
         where mail_id = (select max(mail_id) \
                          from arXiv_next_mail)'

with engine.connect() as conn:
    rows = conn.execute(query).fetchall()

# send events
def _format_payload (row: Row) -> str: 
    return json.dumps({k: row[k] for k in row._mapping.keys()}).encode('utf-8')

if args.dry_run:
    logger.info('***** DRY RUN OUTPUT *****')
    for row in rows:
        logger.info(_format_payload(row))
else:
    for row in rows:
        future = publisher.publish(topic_path, _format_payload(row))
        logger.info(future.result())
