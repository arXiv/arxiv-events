import configparser
import logging
import json
import argparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Row
from sqlalchemy.exc import OperationalError

from google.cloud.pubsub_v1 import PublisherClient
from google.oauth2 import service_account

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
parser.add_argument('-m', '--example-events',
                    action='store_true',
                    help='Publish hard coded example values for testing')
parser.add_argument('-c', '--credential-file')
args = parser.parse_args()

# logging
logger = logging.getLogger("publish_submission_event")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(config['DEFAULT']["log_path"])
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
logger.addHandler(fh)
logger.addHandler(ch)

# event publisher
if args.credential_file:
    credentials = service_account.Credentials.from_service_account_file(args.credential_file)
else:
    credentials = None
publisher = PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(config['DEFAULT']['project_id'], config['DEFAULT']['topic_id'])

# db connection
if not args.example_events:
    try:
        engine = create_engine(config['DEFAULT']['db_connection_string'])
    except OperationalError as e:
        raise Exception(f"Failed to connect to db") from e

    # get rows
    query = 'select submission_id, document_id, paper_id, version, type \
            from arXiv_next_mail \
            where mail_id = (select max(mail_id) \
                            from arXiv_next_mail)'

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
else:
    rows = [
        {
            "submission_id": 3966840,
            "document_id": 3249864,
            "paper_id": "test_paper1",
            "version": 1,
            "type": "new"
        }
    ]

# send events
def _format_payload (row: Row) -> str: 
    return json.dumps({k: row[k] for k in row._mapping.keys()}).encode('utf-8')

if args.dry_run:
    logger.info('***** PRINTING DRY RUN OUTPUT *****')
    for row in rows:
        logger.info(_format_payload(row))
elif args.example_events:
    logger.info('***** PUBLISHING EXAMPLE EVENTS *****')
    for row in rows:
        future = publisher.publish(topic_path, json.dumps(row).encode('utf-8'))
        logger.info(future.result())
else:
    for row in rows:
        future = publisher.publish(topic_path, _format_payload(row))
        logger.info(future.result())
