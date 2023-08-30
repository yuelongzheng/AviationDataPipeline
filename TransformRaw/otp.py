import pandas as pd
import numpy as np

def get_otp_dataframe(blob_data): 
    # Extract the relevant data
    curr = pd.read_excel(blob_data, sheet_name = '2020-21 OTP')
    df_2019 = pd.read_excel(blob_data, sheet_name = '2019')
    df_2018 = pd.read_excel(blob_data, sheet_name= "2018")
    # Stick all the data together in the same dataframe
    dataframe = pd.concat([df_2018, df_2019, curr], ignore_index=True)
    return dataframe

def append_Australia(df): 
    # If an entry is All Ports, do not change anything
    # Otherwise add Australia in front of the port. 
    df['Departing Port (C)'] = np.where(df['Departing Port'] == 'All Ports', 'All Ports', "Australia " + df['Departing Port'])
    df['Arriving Port (C)'] = np.where(df['Arriving Port'] == 'All Ports', 'All Ports', "Australia " + df['Arriving Port'])

def aus_iata_dataframe(): 
    url = "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv"
    iata = pd.read_csv(url)
    aus_iata = iata[iata.iso_country == "AU"]
    # Extract relevant columns
    cols = ['type', 'name', 'iata_code']
    notna_mask = aus_iata['iata_code'].notna()
    # Get a dataframe with all rows with an iata code
    aus_iata = aus_iata.loc[notna_mask, cols]
    return aus_iata

"""
    route of the form: port-port
    convert port into iata code
"""
def turn_route_iata(route, iata_code_dict):
    port_list = route.split("-")
    for index in range(0, len(port_list)):
        port = port_list[index]
        port_list[index] = iata_code_dict[port]
    return "-".join(port_list)

def create_iata_code_columns(otp_df):
    aus_iata = aus_iata_dataframe()
    # List of departing ports and list of arriving ports
    unique_departing = otp_df['Departing Port'].unique()
    unique_arriving = otp_df['Arriving Port'].unique()

    # Create a dictionary with port and iata code as the 
    # key value pair
    iata_code_dict = {}
    for port in unique_departing:
        temp = aus_iata[aus_iata['name'].str.contains(port)]
        if port == "All Ports":
            iata_code_dict[port] = "ALL"
        # Port name only shows up in one airport
        elif len(temp.index) == 1:
            iata_code_dict[port] = temp.iloc[0]['iata_code']
        # Port name shows up in multiple locations
        else:
            large_airport = temp[temp['type'].str.contains('large_airport')]
            # Assume a location only has one "large_airport"
            if len(large_airport.index) == 1:
                iata_code_dict[port] = large_airport.iloc[0]['iata_code']
            else:
                medium_airport = temp[temp['type'].str.contains('medium_airport')]
                # Places does not have a large_airport, again assume location
                # only has one "medium_airport"
                if len(medium_airport.index) == 1:
                    iata_code_dict[port] = medium_airport.iloc[0]['iata_code']

    otp_df['Departing Port (IATA)'] = otp_df['Departing Port'].apply(
        lambda port: port.replace(port, iata_code_dict[port])
    )
    otp_df['Arriving Port (IATA)'] = otp_df['Arriving Port'].apply(
        lambda port: port.replace(port, iata_code_dict[port])
    )
    otp_df['Route (IATA)'] = otp_df['Route'].apply(
    lambda route: turn_route_iata(route, iata_code_dict))

def create_year_month_cols(otp_df): 
    otp_df = otp_df.rename(columns = {"Month":"Date"})
    otp_df['Month'] = otp_df['Date'].dt.month
    otp_df['Year'] = otp_df['Date'].dt.year
    return otp_df

"""
    Turn na strings into 0
    otherwise divide the number by 100
"""
def clean_percent_col(val):
    if val == "na":
        return 0
    else:
        return float(val)/100
    
def create_percent_cols(otp_df): 
    otp_df = otp_df.rename(columns = {"OnTime Departures \n(%)" : "OnTime Departures (%)",
                                      "OnTime Arrivals \n(%)" : "OnTime Arrivals (%)", 
                                      "Cancellations \n\n(%)" : "Cancellations (%)"})
    otp_df['OnTime Departures (%)'] = otp_df['OnTime Departures (%)'].apply(
    clean_percent_col)
    otp_df['OnTime Arrivals (%)'] = otp_df['OnTime Arrivals (%)'].apply(
        clean_percent_col)
    otp_df['Cancellations (%)'] = otp_df['Cancellations (%)'].apply(
        clean_percent_col)
    otp_df['Delayed Departures (%)'] = 1 - otp_df['OnTime Departures (%)']
    otp_df['Delayed Arrivals (%)'] = 1 - otp_df['OnTime Arrivals (%)']
    
    otp_df['Delayed Totals'] = otp_df['Delayed Arrivals (%)'] + otp_df['Delayed Departures (%)']
    return otp_df

def generate_otp(blob_data): 
    otp_df = get_otp_dataframe(blob_data)
    otp_df = create_year_month_cols(otp_df)
    append_Australia(otp_df)
    create_iata_code_columns(otp_df)
    otp_df = create_percent_cols(otp_df)
    route_df = otp_df[['Departing Port', 'Arriving Port', 'Route', 'Route (IATA)']].drop_duplicates()
    departing_df = otp_df[['Departing Port', 'Departing Port (C)', 'Departing Port (IATA)']].drop_duplicates()
    arriving_df = otp_df[['Arriving Port', 'Arriving Port (C)', 'Arriving Port (IATA)']].drop_duplicates()
    arriving_df = arriving_df.rename(columns = {"Arriving Port" : "Departing Port",
                                                'Arriving Port (C)' : 'Departing Port (C)', 
                                                'Arriving Port (IATA)' : 'Departing Port (IATA)'})
    airport_df = pd.concat([departing_df, arriving_df]).drop_duplicates().reset_index(drop = True)
    airport_df['Country'] = 'Australia'
    airport_df = airport_df.rename(columns = {"Departing Port" : "Airport", 
                                              "Departing Port (C)" : "Airport_C",
                                              "Departing Port (IATA)" : "Airport_IATA"})
    cols = ['Month', 'Year', 'Date', 'Airline', 'Departing Port',
            'Arriving Port', 'Sectors Scheduled', 'Sectors Flown', 'Cancellations',
       'Departures On Time', 'Arrivals On Time', 'Departures Delayed',
       'Arrivals Delayed', 'OnTime Departures (%)', 'OnTime Arrivals (%)',
       'Cancellations (%)', 'Delayed Departures (%)', 'Delayed Arrivals (%)',
         'Delayed Totals']
    otp_df = otp_df[cols]
    otp_df = otp_df.rename(columns = {"Departing Port" : "DepartingPort", 
                                      "Arriving Port" : "ArrivingPort",
                                      "Sectors Scheduled" : "SectorsScheduled",
                                      "Sectors Flown" : "SectorsFlown",
                                      "Departures On Time" : "DeparturesOnTime",
                                      "Arrivals On Time" : "ArrivalsOnTime",
                                      "Departures Delayed" : "DeparturesDelayed",
                                      "Arrivals Delayed" : "ArrivalsDelayed", 
                                      "OnTime Departures (%)" : "OnTimeDeparture_percent",
                                      "OnTime Arrivals (%)" : "OnTimeArrivals_percent",
                                      "Cancellations (%)" : "Cancellations_percent",
                                      "Delayed Departures (%)" : "DelayedDepartures_percent",
                                      "Delayed Arrivals (%)" : "DelayedArrivals_percent",
                                      "Delayed Totals" : "DelayedTotals"})
    otp_df = otp_df.drop(otp_df[(otp_df.Year <= 2020) & (otp_df.Airline == "Rex Airlines")].index)
    otp_df = otp_df.drop(otp_df[(otp_df.Year == 2021) & (otp_df.Month <= 6) & (otp_df.Airline == "Rex Airlines")].index)
    # Regional Express was replaced by Rex Airlines on July 2021
    otp_df = otp_df.drop(otp_df[(otp_df.Year == 2021) & (otp_df.Month > 6) & (otp_df.Airline == "Regional Express")].index)
    otp_df = otp_df.drop(otp_df[(otp_df.Year > 2021) & (otp_df.Airline == "Regional Express")].index)
    otp_df = otp_df.drop(otp_df[(otp_df.Airline == "Tigerair Australia")].index)
    otp_df = otp_df.reset_index()
    # Remove index 
    otp_df = otp_df.drop(['index'], axis = 1)
    # Rename Regional Express to Rex Airlines, per business rule
    otp_df = otp_df.replace({'Regional Express' : "Rex Airlines"})
    otp_output = otp_df.to_csv(index=False, encoding='utf-8')
    route_df = route_df.rename(columns = {"Departing Port" : "DepartingPort", 
                                "Arriving Port" : "ArrivingPort",
                                "Route (IATA)" : "Route_IATA"})
    route_output = route_df.to_csv(index = False, encoding = 'utf-8')
    new_row = pd.DataFrame(data = {'Airport' : ["All Australian Airports"], 
                    'Airport_C': ["Australia All Australian Airports"],
                     "Airport_IATA" : ["ALL"],
                     "Country" : ["Australia"]})
    airport_df = pd.concat([airport_df, new_row]).reset_index(drop = True)
    airport_output = airport_df.to_csv(index = False, encoding = 'utf-8') 
    return otp_output, route_output, airport_output