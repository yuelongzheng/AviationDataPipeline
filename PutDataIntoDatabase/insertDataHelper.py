import logging
import pymysql
import pandas  as pd
from sqlalchemy import create_engine

def dataframe_to_sql_table(dataframe, engine, table_name): 
    with engine.connect() as con:
        dataframe.to_sql(
            name = table_name.lower(),
            con = con,
            if_exists = "append",
            index = False
        )

def insertData(config): 
    try : 
        eng_str = "mysql+pymysql://" + config['user'] +\
              ":" + config['password'] + "@" +\
                  config['host'] + "/" + config['database']
        engine = create_engine(eng_str, connect_args = { 
                                   "ssl" : 
                                       config['ssl']
                                   })
        logging.info(eng_str)

        airport_url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/airport.csv"
        airport_df = pd.read_csv(airport_url)
        dataframe_to_sql_table(airport_df, engine, "airport")

        airport_stats_url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/AirportStats.csv"
        airport_stats_df = pd.read_csv(airport_stats_url)
        dataframe_to_sql_table(airport_stats_df, engine, "airportstats")
        
        nav_delays_transformed_url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/NavigationalDelaysTransformations.csv"
        nav_delays_df = pd.read_csv(nav_delays_transformed_url)
        dataframe_to_sql_table(nav_delays_df, 
                            engine,
                            "navigationaldelaystransformations")
        
        route_url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/route.csv"
        route_df = pd.read_csv(route_url)
        dataframe_to_sql_table(route_df, engine, "route")

        otp_url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/otp.csv"
        otp_df = pd.read_csv(otp_url)
        dataframe_to_sql_table(otp_df, engine, "otp")
        
    except Exception as e: 
        logging.info("insertDataHelper Exception occured: {}".format(e))
