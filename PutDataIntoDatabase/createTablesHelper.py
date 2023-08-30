import logging
import pymysql

def createTables(conn):
  try : 
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS `Airport` (" +\
                        "`Airport` VARCHAR(250) NOT NULL," +\
                        "`Airport_C` VARCHAR(250) NULL," +\
                        "`Airport_IATA` VARCHAR(250) NULL," +\
                        "`Country` VARCHAR(250) NULL," +\
                        "PRIMARY KEY (`Airport`))")
    cursor.execute("CREATE TABLE IF NOT EXISTS `AirportStats` ("+\
                   "`Airport` VARCHAR(250) NOT NULL,"+\
                    "`Year` INT NOT NULL,"+\
                    "`Month` INT NOT NULL,"+\
                    "`Date` DATETIME NULL,"+\
                    "`DomesticInboundPassengers` INT NULL,"+\
                    "`DomesticOutboundPassengers` INT NULL,"+\
                    "`DomesticTotalPassengers` INT NULL,"+\
                    "`InternationalInboundPassengers` INT NULL,"+\
                    "`InternationalOutboundPassengers` INT NULL,"+\
                    "`InternationalTotalPassengers` INT NULL,"+\
                    "`TotalInboundPassengers` INT NULL,"+\
                    "`TotalOutboundPassengers` INT NULL,"+\
                    "`TotalPassengers` INT NULL,"+\
                    "`DomesticInboundAircraftMovement` INT NULL,"+\
                    "`DomesticOutboundAircraftMovement` INT NULL,"+\
                    "`DomesticTotalAircraftMovement` INT NULL,"+\
                    "`InternationalInboundAircraftMovement` INT NULL,"+\
                    "`InternationalOutboundAircraftMovement` INT NULL,"+\
                    "`InternationalTotalAircraftMovement` INT NULL,"+\
                    "`TotalInboundAircraftMovement` INT NULL,"+\
                    "`TotalOutboundAircraftMovement` INT NULL,"+\
                    "`TotalAircraftMovement` INT NULL,"+\
                    "PRIMARY KEY (`Airport`, `Year`, `Month`),"+\
                    "INDEX `fk_AirportStats_Airport1_idx` (`Airport` ASC) ,"+\
                    "CONSTRAINT `fk_AirportStats_Airport1`"+\
                    "FOREIGN KEY (`Airport`)"+\
                    "REFERENCES `Airport` (`Airport`)"+\
                    "ON DELETE NO ACTION "+\
                    "ON UPDATE NO ACTION)")
    cursor.execute("CREATE TABLE IF NOT EXISTS `Route` (" +\
                    "`DepartingPort` VARCHAR(250) NOT NULL," +\
                    "`ArrivingPort` VARCHAR(250) NOT NULL,"+\
                    "`Route` VARCHAR(250) NULL,"+\
                    "`Route_IATA` VARCHAR(250) NULL,"+\
                    "INDEX `fk_Route_Airport1_idx` (`DepartingPort` ASC) ,"+\
                    "INDEX `fk_Route_Airport2_idx` (`ArrivingPort` ASC) ,"+\
                    "PRIMARY KEY (`DepartingPort`, `ArrivingPort`),"+\
                    "CONSTRAINT `fk_Route_Airport1`"+\
                    "FOREIGN KEY (`DepartingPort`)"+\
                    "REFERENCES `Airport` (`Airport`)"+\
                    " ON DELETE NO ACTION "+\
                    "ON UPDATE NO ACTION,"+\
                    "CONSTRAINT `fk_Route_Airport2`"+\
                    "FOREIGN KEY (`ArrivingPort`)"+\
                    "REFERENCES `Airport` (`Airport`)"+\
                    "ON DELETE NO ACTION "+\
                    "ON UPDATE NO ACTION)")
    cursor.execute("CREATE TABLE IF NOT EXISTS `OTP` ("+\
                   "`Month` INT NOT NULL,"+\
                    "`Year` INT NOT NULL,"+\
                    "`Date` DATETIME NULL,"+\
                    "`Airline` VARCHAR(250) NOT NULL,"+\
                    "`DepartingPort` VARCHAR(250) NOT NULL,"+\
                    "`ArrivingPort` VARCHAR(250) NOT NULL,"+\
                    "`SectorsScheduled` INT NULL,"+\
                    "`SectorsFlown` INT NULL,"+\
                    "`Cancellations` INT NULL,"+\
                    "`DeparturesOnTime` INT NULL,"+\
                    "`ArrivalsOnTime` INT NULL,"+\
                    "`DeparturesDelayed` INT NULL,"+\
                    "`ArrivalsDelayed` INT NULL,"+\
                    "`OnTimeDeparture_percent` FLOAT NULL,"+\
                    "`OnTimeArrivals_percent` FLOAT NULL,"+\
                    "`Cancellations_percent` FLOAT NULL,"+\
                    "`DelayedDepartures_percent` FLOAT NULL,"+\
                    "`DelayedArrivals_percent` FLOAT NULL,"+\
                    "`DelayedTotals` FLOAT NULL,"+\
                    "PRIMARY KEY (`Month`, `Year`, `Airline`, `DepartingPort`, `ArrivingPort`),"+\
                    "INDEX `fk_OTP_Route1_idx` (`DepartingPort` ASC, `ArrivingPort` ASC) ,"+\
                    "CONSTRAINT `fk_OTP_Route1`"+\
                    "FOREIGN KEY (`DepartingPort` , `ArrivingPort`)"+\
                    "REFERENCES `Route` (`DepartingPort` , `ArrivingPort`)"+\
                    "ON DELETE NO ACTION "+\
                    "ON UPDATE NO ACTION)")
    cursor.execute("CREATE TABLE IF NOT EXISTS `NavigationalDelaysTransformations` ("+\
                    "`Airport` VARCHAR(250) NOT NULL,"+\
                    "`Year` INT NOT NULL,"+\
                    "`Month` INT NOT NULL,"+\
                    "`Airline` VARCHAR(250) NOT NULL,"+\
                    "`percent_ofTotalDeparturesDelayed` FLOAT NULL,"+\
                    "`DepartDelaysPeople` FLOAT NULL,"+\
                    "`DepartTotalPeople` FLOAT NULL,"+\
                    "`percent_ofTotalArrivalsDelayed` FLOAT NULL,"+\
                    "`ArrivalsDelaysPeople` FLOAT NULL,"+\
                    "`ArrivalsTotalPeople` FLOAT NULL,"+\
                    "`percent_ofTotalFlightsCancelled` FLOAT NULL,"+\
                    "`DepartPeopleCancelled` FLOAT NULL,"+\
                    "`PeopleWIthIntentToDepart` FLOAT NULL,"+\
                    "`PeopleTotals` FLOAT NULL,"+\
                    "`DelaysTotal` FLOAT NULL,"+\
                    "PRIMARY KEY (`Airport`, `Year`, `Month`, `Airline`),"+\
                    "INDEX `fk_NavigationalDelaysTransformations_Airport_idx` (`Airport` ASC) ,"+\
                    "CONSTRAINT `fk_NavigationalDelaysTransformations_Airport`"+\
                    "FOREIGN KEY (`Airport`)"+\
                    "REFERENCES `Airport` (`Airport`)"+\
                    "ON DELETE NO ACTION "+\
                    "ON UPDATE NO ACTION)")
  except Exception as e: 
    logging.info("createTablesHelper Exception occured: {}".format(e))
  finally: 
        conn.commit()
        cursor.close()
