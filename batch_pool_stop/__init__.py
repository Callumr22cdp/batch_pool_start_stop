from __future__ import print_function
import datetime
import io
import os
import sys
import time

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.functions as func

import json
import logging

# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

def main(req: func.HttpRequest) -> func.HttpResponse:

    start_time = datetime.datetime.now().replace(microsecond=0)
    logging.info('Pool Deletion start time: {}'.format(start_time))

    # Create a Batch service client to interact with the Bach service

    credentials = SharedKeyCredentials(os.environ["_BATCH_ACCOUNT_NAME"], os.environ["_BATCH_ACCOUNT_KEY"])

    batch_client = BatchServiceClient(credentials, batch_url=os.environ["_BATCH_ACCOUNT_URL"])

    # Delete batch pool
     
    batch_client.pool.delete( os.environ["_POOL_ID"])

    end_time = datetime.datetime.now().replace(microsecond=0)
    logging.info('Pool Deletion end time: {}'.format(end_time))
    logging.info('Pool Deletion elapsed time: {}'.format(end_time - start_time))

    pool_id=os.environ["_POOL_ID"]

    return func.HttpResponse(
        body=json.dumps(pool_id),
        status_code=200
    )


