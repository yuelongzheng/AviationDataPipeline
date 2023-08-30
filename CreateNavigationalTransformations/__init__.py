import logging
import azure.functions as func
import os
import sys

from . NavigationalTransform import get_navigational_transform
from azure.storage.blob import ContainerClient

def upload_to_transformed_container(output, filename): 
    container_client = ContainerClient.from_connection_string(
            conn_str=os.environ['s12023_storage_key'],
            container_name="transformed-data-navigational-database"
    )

    container_client.upload_blob(
            name=filename,
            data=output,
            overwrite=True
        )
    
def main(myblob: func.InputStream):
    if myblob.name == "transformed-data-navigational-database/otp.csv":
        logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
        try :
            nav_output = get_navigational_transform()
            upload_to_transformed_container(nav_output, 
            "NavigationalDelaysTransformations.csv")
        except Exception as e :
            logging.info(e)
