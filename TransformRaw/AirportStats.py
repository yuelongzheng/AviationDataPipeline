import pandas as pd

def get_pass_move_dataframe(blob_data): 
    pass_move = pd.read_excel(blob_data, sheet_name= "Airport Passengers", header = 6)
    # Rename the airport column
    pass_move = pass_move.rename(columns = {"AIRPORT" : "Airport"})
    # Capitlize the first letter of every word 
    # otherwise make all other letters lower case
    pass_move['Airport'] = pass_move['Airport'].str.title()
    # Create a Date column, the day is the first of every month
    pass_move['Date'] = pd.to_datetime(
        dict(year = pass_move['Year'], month = pass_move['Month'], day = 1))
    # Rename columns to what they would be in the database
    pass_move = pass_move.rename(columns = {"INBOUND" : "DomesticInboundPassengers", 
                                            "OUTBOUND" : "DomesticOutboundPassengers", 
                                            "TOTAL" : "DomesticTotalPassengers",
                                            "INBOUND.1" : "InternationalInboundPassengers", 
                                            "OUTBOUND.1" : "InternationalOutboundPassengers", 
                                            "TOTAL.1" : "InternationalTotalPassengers", 
                                            "INBOUND.2" : "TotalInboundPassengers", 
                                            "OUTBOUND.2" : "TotalOutboundPassengers",
                                            "TOTAL.2" : "TotalPassengers"})
    lower_bound_year = 2018
    # Filter out all the rows below the lower_bound_year
    pass_move = pass_move[(pass_move['Year'] >= lower_bound_year)]
    return pass_move

def get_aircraft_move_dataframe(blob_data): 
    move_df = pd.read_excel(blob_data, sheet_name= "Aircraft Movements", header = 5) 
    # Rename the airport column
    move_df = move_df.rename(columns = {"AIRPORT" : "Airport"})
    # Capitlize the first letter of every word 
    # otherwise make all other letters lower case
    move_df['Airport'] = move_df['Airport'].str.title()
    # Create a Date column, the day is the first of every month
    move_df['Date'] = pd.to_datetime(
        dict(year = move_df['Year'], month = move_df['Month'], day = 1))
    # Rename columns to what they would be in the database
    move_df = move_df.rename(columns = {"INBOUND" : "DomesticInboundAircraftMovement", 
                                            "OUTBOUND" : "DomesticOutboundAircraftMovement", 
                                            "TOTAL" : "DomesticTotalAircraftMovement",
                                            "INBOUND.1" : "InternationalInboundAircraftMovement", 
                                            "OUTBOUND.1" : "InternationalOutboundAircraftMovement", 
                                            "TOTAL.1" : "InternationalTotalAircraftMovement", 
                                            "INBOUND.2" : "TotalInboundAircraftMovement", 
                                            "OUTBOUND.2" : "TotalOutboundAircraftMovement",
                                            "TOTAL.2" : "TotalAircraftMovement"})
    lower_bound_year = 2018
    # Filter out all the rows below the lower_bound_year
    move_df = move_df[(move_df['Year'] >= lower_bound_year)]
    return move_df

def generate_airport_stats(blob_data):
    air_move = get_aircraft_move_dataframe(blob_data)
    pass_move = get_pass_move_dataframe(blob_data) 
    # Merge the two above dataframes
    merge = pd.merge(air_move, pass_move, on = ['Airport', 'Year', 'Month', 'Date'])
    # Reorder the columns
    cols = ['Airport', 'Year', 'Month', 'Date', 
                'DomesticInboundPassengers', 'DomesticOutboundPassengers',
                'DomesticTotalPassengers', 'InternationalInboundPassengers',
                'InternationalOutboundPassengers', 'InternationalTotalPassengers',
                'TotalInboundPassengers', 'TotalOutboundPassengers', 'TotalPassengers',
                'DomesticInboundAircraftMovement',
                'DomesticOutboundAircraftMovement', 'DomesticTotalAircraftMovement',
                'InternationalInboundAircraftMovement',
                'InternationalOutboundAircraftMovement',
                'InternationalTotalAircraftMovement', 'TotalInboundAircraftMovement',
                'TotalOutboundAircraftMovement', 'TotalAircraftMovement']
    merge = merge[cols]
    output = merge.to_csv(index = False, encoding = 'utf-8')
    return output
