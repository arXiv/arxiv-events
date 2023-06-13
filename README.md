# Submission Publication Event
This is a Python script that pushes submission publication data to Google Cloud Platform (GCP) pub/sub. It retrieves data from a MySQL database and publishes it as events using the Google Cloud Pub/Sub service.

## Prerequisites
Before running the script, make sure you have the following requirements:

Python 3.x installed
configparser, logging, json, argparse, sqlalchemy, google-cloud-pubsub, and pymysql Python packages installed
Access to a MySQL database
Access to a GCP project with Pub/Sub enabled

## Setup
Clone this repository

Install the required Python packages by running the following command:

poetry install

Create a configuration file named submission_published.ini modeled after
our example, submission_published-example.ini

## Usage
Run the script using the following command:

python script.py [-n | --dry-run]
-n or --dry-run: Optional flag to perform a dry run without actually sending the data to Pub/Sub. The script will log the SQL output instead.

The script performs the following steps:

Reads the configuration from the submission_published.ini file.

Parses the command-line arguments.

Configures logging to write log messages to a file specified in the configuration and to the console for error messages.

Initializes a Pub/Sub PublisherClient and sets the topic path based on the configuration.

Attempts to establish a connection to the MySQL database using the provided credentials.

Executes a SQL query to retrieve rows of data from the arXiv_next_mail table.

Formats each row as a JSON payload and publishes it as an event to the Pub/Sub topic.

If the dry run flag is set, the script logs the formatted payload without publishing it.

## Logging
The script logs messages using the logging module. It creates a logger named "publish_submission_event" and configures two handlers:

A FileHandler that writes log messages to the file specified in the configuration.
A StreamHandler that writes log messages to the console for error messages.
The log levels are as follows:

DEBUG: Detailed information for debugging purposes.
INFO: Confirmation that things are working as expected.
ERROR: Indicates errors or unexpected conditions.
Troubleshooting
If the script fails to connect to the MySQL database, make sure the provided credentials and database details are correct.

If the script fails to publish events to Pub/Sub, ensure that the GCP project ID and Pub/Sub topic ID are correct and that you have the necessary permissions to access the Pub/Sub service.

Check the log file specified in the configuration for any error messages or additional information.

## License
This script is licensed under the MIT License.