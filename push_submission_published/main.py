import configparser
import logging
import json

from sqlalchemy import create_engine
from sqlalchemy.engine import Row
from sqlalchemy.exc import OperationalError

from google.cloud.pubsub_v1 import PublisherClient

# config
config = configparser.ConfigParser()
config.read("datacite.ini")

# logging
logger = logging.getLogger("publish_submission_event")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(config["DEFAULT"]["log_path"])
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
logger.addHandler(fh)
logger.addHandler(ch)

# event publisher
publisher = PublisherClient()
topic_path = publisher.topic_path(config['DEFAULT']['project_id'], config['DEFAULT']['topic_id'])

# db connection
try:
    engine = create_engine(
            f"mysql+pymysql://{config['DEFAULT']['db_username']}:{config['DEFAULT']['db_password']} \
            @{config['DEFAULT']['db_host']}/{config['DEFAULT']['db_database_name']}")
except OperationalError as e:
    logger.error(f"Failed to connect to DB: {e}")
    exit()

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

for row in rows:
    future = publisher.publish(topic_path, _format_payload(row))
    logger.info(future.result())
