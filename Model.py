import pandas as pd
import numpy as np
from datetime import datetime
from ConsumerQueue import ConsumerQueue
from EventGenerator import event_generator
from EventGenerator import str_to_datetime
from EventGenerator import datetime_to_seconds
from sklearn.linear_model import LinearRegression

def get_model(df):
    cq = ConsumerQueue()
    prev_date = datetime.strptime("9999-01-01 0:0:0", '%Y-%m-%d %H:%M:%S')
    
    X = []
    y = []
    
    for index, consumer_id, e_type, date, employee_id in event_generator(df):
        if prev_date != date.date():
            cq.clear()
            prev_date = date.date()
        seconds = datetime_to_seconds(date)
        if e_type == 'enter_date':
            summon_date = str_to_datetime(df[df.index == index]['summon_date'].iloc[0])
            
            if summon_date is not np.nan:
                X.append([len(cq.wait_queue)])
                y.append(datetime_to_seconds(summon_date) - datetime_to_seconds(date))
            
            cq.enter_consumer(consumer_id, seconds)
        elif e_type == 'summon_date':
            cq.summon_consumer(consumer_id, employee_id, seconds)
        elif e_type == 'departure_date' or e_type == 'cancel_date':
            cq.mark_departed(consumer_id)
            
    X = np.array(X)
    y = np.array(y)
    reg = LinearRegression().fit(X, y)
    return reg, X, y