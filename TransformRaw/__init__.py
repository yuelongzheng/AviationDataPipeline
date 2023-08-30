import pandas as pd
import azure.functions as func
import os

from azure.storage.blob import ContainerClient
from . AirportStats import generate_airport_stats
from . otp import generate_otp

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
    if "traffic" in myblob.name: 
        output = generate_airport_stats(myblob.read())
        upload_to_transformed_container(output, "AirportStats.csv")

    if "monthly" in  myblob.name: 
        otp_output, route_output, airport_output  = generate_otp(myblob.read())
        upload_to_transformed_container(otp_output, "otp.csv")
        upload_to_transformed_container(route_output, "route.csv")
        upload_to_transformed_container(airport_output, "airport.csv")
