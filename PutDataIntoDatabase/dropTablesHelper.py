import logging
import pymysql

def dropTables(conn):
    try: 
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS `AirportStats`")
        cursor.execute("DROP TABLE IF EXISTS `otp`")
        cursor.execute("DROP TABLE IF EXISTS `navigationaldelaystransformations`")
        cursor.execute("DROP TABLE IF EXISTS `route`")
        cursor.execute("DROP TABLE IF EXISTS `airport`")
    except Exception as e: 
        logging.info("dropTablesHelper Exception occured: {}".format(e))
    finally: 
        conn.commit()
        cursor.close()
