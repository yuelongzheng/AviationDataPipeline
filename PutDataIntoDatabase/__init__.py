import logging
import azure.functions as func
import pymysql
import pathlib
import os

from .createTablesHelper import createTables
from .dropTablesHelper import dropTables
from .insertDataHelper import insertData

def get_ssl_cert(): 
    current_path = pathlib.Path(__file__).parent.parent
    return str(current_path/ 'DigiCertGlobalRootCA.crt.pem')


def main(myblob: func.InputStream):
    if myblob.name == "transformed-data-navigational-database/NavigationalDelaysTransformations.csv":
        config = {"user" : 'PaceS12023Admin',
              "password" : os.environ['databasepassword'],
              "database" : 'test',
              "host" : 'pace-s1-2023-pub-access.mysql.database.azure.com', 
              "ssl" : {'ca': get_ssl_cert()}}
        conn = pymysql.connect(**config)
        try: 
            dropTables(conn)
            createTables(conn)
            insertData(config)
            conn.close()
        except Exception as e: 
            logging.info(e)
