import pandas as pd
import numpy as np 

def get_on_time_performance_dataframe(): 
    url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/otp.csv"
    df = pd.read_csv(url)
    return df

def get_pass_move_dataframe(): 
    url = "https://s12023.blob.core.windows.net/transformed-data-navigational-database/AirportStats.csv"
    df = pd.read_csv(url)
    return df

def get_airline_dataframe(otp): 
    airline_list = otp['Airline'].unique().tolist()
    airline_list.remove('All Airlines')
    df = pd.DataFrame(airline_list, columns = ['Airline'])
    return df
'''
    Creating the transformations from the transformation page
    Can be found at 
    https://docs.google.com/spreadsheets/d/1QhjJBTYiu-ACB05q10SWkZKKssWLJHfR/edit?rtpof=true#gid=1405467807
'''
def get_hard_columns(otp, pass_move): 
    # Column G from the google sheet link
    # Also column S
    Depart_Flights_Flown_From = otp.groupby(['Airline',
                                            'Date',
                                            'DepartingPort'],
                                            as_index = False)[
                                                'SectorsFlown'].sum()
    # Rename columns so they can be distinguished 
    Depart_Flights_Flown_From = Depart_Flights_Flown_From.rename(columns = {
        "SectorsFlown":"Depart_Flights_Flown_From"
    })
    pass_move = pd.merge(pass_move, Depart_Flights_Flown_From,
                          how = "left",
                          left_on = ['Airport', 'Airline', 'Date'],
                          right_on = ['DepartingPort', 'Airline', 'Date'])
    # Column H 
    Depart_Flights_From_Delayed = otp.groupby(['Airline',
                                                'Date',
                                                'DepartingPort'],
                                    as_index = False)[
                                'DeparturesDelayed'].sum()
    Depart_Flights_From_Delayed = Depart_Flights_From_Delayed.rename(
        columns = { "DeparturesDelayed":"Depart_Flights_From_Delayed"
    })
    pass_move = pd.merge(pass_move, Depart_Flights_From_Delayed,
                          how = "left",
                          left_on = ['Airport', 'Airline', 'Date'],
                          right_on = ['DepartingPort', 'Airline', 'Date'])
    # Column I
    Depart_Flights_From_All = Depart_Flights_Flown_From[
        Depart_Flights_Flown_From['Airline'] == 'All Airlines'] 
    Depart_Flights_From_All = Depart_Flights_From_All.rename(
        columns = {"Depart_Flights_Flown_From":"Depart_Flights_From_All"
    })
    pass_move = pd.merge(pass_move, Depart_Flights_From_All,
                          how = "left",
                          left_on = ['Airport', 'Date'],
                          right_on = ['DepartingPort', 'Date'])
    # Column M
    Arrive_Flights_Flown_From = otp.groupby(['Airline',
                                            'Date',
                                            'ArrivingPort'],
                                as_index = False)[
                                'SectorsFlown'].sum()
    Arrive_Flights_Flown_From = Arrive_Flights_Flown_From.rename(
        columns = {"SectorsFlown" : "Arrive_Flights_Flown_From"}
    )
    pass_move = pd.merge(pass_move, Arrive_Flights_Flown_From,
                          how = "left",
                          left_on = ['Airport', 'Airline_x', 'Date'],
                          right_on = ['ArrivingPort', 'Airline', 'Date'])
    # Column N
    Arrive_Flights_From_Delayed = otp.groupby(['Airline',
                                                'Date',
                                                'ArrivingPort'],
                                    as_index = False)[
        'ArrivalsDelayed'].sum()
    # Rename Airline to TempAirline to avoid merge errors, apart from 
    # that the name does not matter
    Arrive_Flights_From_Delayed = Arrive_Flights_From_Delayed.rename(
        columns = {"ArrivalsDelayed" : "Arrive_Flights_From_Delayed",
                   "Airline" : "TempAirline"}
    )
    pass_move = pd.merge(pass_move, Arrive_Flights_From_Delayed,
                          how = "left",
                          left_on = ['Airport', 'Airline_x', 'Date'],
                          right_on = ['ArrivingPort', 'TempAirline', 'Date'])
    # Column O
    Arrive_Flights_From_All = Arrive_Flights_Flown_From[
        Arrive_Flights_Flown_From['Airline'] == 'All Airlines']
    Arrive_Flights_From_All = Arrive_Flights_From_All.rename(
        columns = {"Arrive_Flights_Flown_From" : "Arrive_Flights_From_All",
                   "Airline" : "TempAirline1",
                   "ArrivingPort" : "ArrivingPort1"}
    )
    pass_move = pd.merge(pass_move, Arrive_Flights_From_All,
                          how = "left",
                          left_on = ['Airport', 'Date'],
                          right_on = ['ArrivingPort1', 'Date'])
    # Column T
    Cancelled_Flights_From = otp.groupby(['Airline',
                                        'Date',
                                        'DepartingPort'],
                            as_index = False)[
        'Cancellations'].sum() 
    Cancelled_Flights_From = Cancelled_Flights_From.rename(
        columns = { "Airline" : "TempAirline2",
                    "DepartingPort" : "DepartingPort1"}
    )
    pass_move = pd.merge(pass_move, Cancelled_Flights_From,
                          how = "left",
                          left_on = ['Airport', 'Airline_x', 'Date'],
                          right_on = ['DepartingPort1', 'TempAirline2', 'Date'])
    # Column V
    Scheduled_Flights_From = otp.groupby(['Airline',
                                        "Date",
                                        "DepartingPort"],
                            as_index = False)[
        'SectorsScheduled'].sum()
    Scheduled_Flights_From = Scheduled_Flights_From[
        Scheduled_Flights_From['Airline'] == 'All Airlines'
    ] 
    Scheduled_Flights_From = Scheduled_Flights_From.rename(
        columns = {"DepartingPort" : "DepartingPort2",
                   "SectorsScheduled" : "Scheduled_Flights_From",
                   "Airline" : "Airline1"}
    )
    pass_move = pd.merge(pass_move, Scheduled_Flights_From,
                          how = "left",
                          left_on = ['Airport', 'Date'],
                          right_on = ['DepartingPort2', 'Date'])
    # Obtain relevants columns from all the merges
    columns = ["Airport", "Year", "Month", "Airline_x", "Date",
               "DomesticInboundPassengers", "DomesticOutboundPassengers",
               "Depart_Flights_Flown_From", "Depart_Flights_From_Delayed",
               "Depart_Flights_From_All", "Arrive_Flights_Flown_From", 
               "Arrive_Flights_From_Delayed", "Arrive_Flights_From_All",
               "Cancellations", "Scheduled_Flights_From"]
    pass_move = pass_move[columns]

    # Create required columns
    # Column J
    pass_move['percent_ofTotalDeparturesDelayed'] = np.where(pass_move['Depart_Flights_From_All'] == 'na',
                                 0, 
                                pass_move['Depart_Flights_From_Delayed']/\
                                pass_move['Depart_Flights_From_All'])
    # Column P
    pass_move['percent_ofTotalArrivalsDelayed'] = np.where(pass_move['Arrive_Flights_From_All'] == 'na',
                                 0,
                                 pass_move['Arrive_Flights_From_Delayed']/\
                                 pass_move['Arrive_Flights_From_All'])
    # Column W 
    pass_move['Cancelled_Ratio'] = np.where(pass_move['Depart_Flights_From_All'] == 'na',
                                 0,
                                 pass_move['Cancellations']/\
                                 pass_move['Depart_Flights_From_All'])
    # Column X
    pass_move['percent_ofTotalFlightsCancelled'] = np.where(pass_move['Scheduled_Flights_From'] == 'na',
                                 0,
                                 pass_move['Cancellations']/\
                                 pass_move['Scheduled_Flights_From'])
    # Column L 
    pass_move['DepartTotalPeople'] = np.where(pass_move['Depart_Flights_From_All'] == 'na',
                                 0,
                                 pass_move['Depart_Flights_Flown_From']/\
                                 pass_move['Depart_Flights_From_All'] *\
                                 pass_move['DomesticInboundPassengers'])
    # Column R 
    pass_move['ArrivalsTotalPeople'] = np.where(pass_move['Arrive_Flights_From_All'] == 'na', 
                                 0, 
                                 pass_move['Arrive_Flights_Flown_From'] /\
                                 pass_move['Arrive_Flights_From_All'] *\
                                 pass_move['DomesticOutboundPassengers'])
    # Column K 
    pass_move['DepartDelaysPeople'] = pass_move['percent_ofTotalDeparturesDelayed'] * pass_move['DomesticInboundPassengers']
    # Column Q
    pass_move['ArrivalsDelaysPeople'] = pass_move['percent_ofTotalArrivalsDelayed'] * pass_move['DomesticInboundPassengers']
    # Column Y
    pass_move['DepartPeopleCancelled'] = pass_move['Cancelled_Ratio'] * pass_move['DomesticInboundPassengers']
    # Column Z
    pass_move['PeopleWithIntentToDepart'] = pass_move['DepartTotalPeople'] + pass_move['DepartPeopleCancelled']
    # Column AA 
    pass_move['PeopleTotals'] = pass_move['DepartTotalPeople'] + pass_move['ArrivalsTotalPeople']
    # Column AB 
    pass_move['DelaysTotal'] = pass_move['DepartDelaysPeople'] + pass_move['ArrivalsDelaysPeople']
    return pass_move


def get_navigational_transform():
    otp = get_on_time_performance_dataframe()
    airline_df = get_airline_dataframe(otp)
    pass_move = get_pass_move_dataframe()
    pass_move = pass_move.merge(airline_df, how = 'cross')
    cols = ["Airport","Year","Month", "Airline","Date", "DomesticInboundPassengers", "DomesticOutboundPassengers"]	
    pass_move = pass_move[cols]
    pass_move = pass_move.sort_values(by = ['Airline', 'Airport', 'Year', 'Month'])
    pass_move = get_hard_columns(otp, pass_move)
    cols = ["Airport", "Year",	"Month", 
            "Airline_x", "percent_ofTotalDeparturesDelayed","DepartDelaysPeople", 
            "DepartTotalPeople", "percent_ofTotalArrivalsDelayed", "ArrivalsDelaysPeople",
            "ArrivalsTotalPeople", "percent_ofTotalFlightsCancelled","DepartPeopleCancelled",
            "PeopleWithIntentToDepart", "PeopleTotals", "DelaysTotal"]	
    pass_move = pass_move[cols]
    pass_move = pass_move.rename(columns = {"Airline_x" : "Airline"})
    pass_move = pass_move.sort_values(by =['Airport', 'Airline', 'Year', 'Month'])
    pass_move = pass_move.fillna(0)
    output = pass_move.to_csv(index=False, encoding='utf-8')
    return output