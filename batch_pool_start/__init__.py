from __future__ import print_function
import datetime
import io
import os
import sys
import time

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
import azure.functions as func

import json
import logging

# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    logging.info('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="microsoft-dsvm",
                offer="dsvm-win-2019",
                sku="winserver-2019",
                version="latest"
            ),
            node_agent_sku_id="batch.node.windows amd64"),

        # set physical node properties
        vm_size=os.environ["_POOL_VM_SIZE"],
        target_dedicated_nodes=os.environ["_POOL_NODE_COUNT"],


        # add StartTask object to execute on each node as that node joins the pool, and each time a node is restarted.
        # e.g. run In this example, the StartTask runs Bash shell commands to install package dependencies on the nodes.

        start_task=batchmodels.StartTask(
        command_line="cmd /c \"pip install azure-storage-blob pandas\"",
        wait_for_success=True,
        user_identity=batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                scope=batchmodels.AutoUserScope.pool,
                elevation_level=batchmodels.ElevationLevel.admin)),
                )

    )

    batch_service_client.pool.add(new_pool)


def main(req: func.HttpRequest) -> func.HttpResponse:

    start_time = datetime.datetime.now().replace(microsecond=0)
    logging.info('Pool Creation start time: {}'.format(start_time))

    # Create a Batch service client to interact with the Batch service
    credentials = SharedKeyCredentials(os.environ["_BATCH_ACCOUNT_NAME"], os.environ["_BATCH_ACCOUNT_KEY"])

    batch_client = BatchServiceClient(credentials, batch_url=os.environ["_BATCH_ACCOUNT_URL"])

    try:
        
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, os.environ["_POOL_ID"])
        
    except batchmodels.BatchErrorException as err:
       # print_batch_exception(err)
        print(err)
        raise

    end_time = datetime.datetime.now().replace(microsecond=0)
    logging.info('Pool Creation end time: {}'.format(end_time))
    logging.info('Pool Creation elapsed time: {}'.format(end_time - start_time))

    pool_id=os.environ["_POOL_ID"]

    return func.HttpResponse(
        body=json.dumps(pool_id),
        status_code=200
    )


