import numpy as np
from datetime import datetime

def str_to_datetime(x):
    if x is np.nan:
        return np.nan
    return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
def timedif_to_seconds(x):
    if x is np.nan:
        return np.nan
    return x.days*24*60*60 + x.seconds
def datetime_to_seconds(x):
    if x is np.nan:
        return np.nan
    return x.hour*60*60 + x.minute*60 + x.second

def event_generator(df):
    """
    Generates events from the database, 
    the events that it process can be found in event_types list
    """
    
    event_types = ['enter_date', 'summon_date', 'departure_date', 'cancel_date']
    events = [sorted([(str_to_datetime(row[e_type]), index) 
                      for index, row in df[[e_type]].dropna().iterrows()]) 
              for e_type in event_types]
    event_lens = [len(event) for event in events]
    event_indexes = [0 for x in event_types]
    
    while (np.array(event_indexes) < np.array(event_lens)).any(): # while there are still events left
        
        min_i = 0
        min_e_index = event_indexes[min_i]
        min_date = datetime.strptime("9999-01-01 0:0:0", '%Y-%m-%d %H:%M:%S')
        
        for i in range(len(event_types)):
            e_index = event_indexes[i]
            
            if e_index >= event_lens[i]:
                continue
            current_date, current_index = events[i][e_index]
                
            if (current_date, current_index, i) < (min_date, min_e_index, min_i):
                min_date, min_e_index, min_i = current_date, current_index, i
                
        event_indexes[min_i] += 1
        row = df[df.index == min_e_index]
        yield min_e_index, row['consumer_id'].iloc[0], event_types[min_i], min_date, row['employee_id'].iloc[0]

