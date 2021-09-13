import pandas as pd
import numpy as np
from datetime import datetime
from ConsumerQueue import ConsumerQueue
from EventGenerator import event_generator
from EventGenerator import str_to_datetime
from EventGenerator import datetime_to_seconds
from EventGenerator import timedif_to_seconds
from Model import get_model
import matplotlib.pyplot as plt
def get_dif(df, col1, col2):
    dif = []
    for index, row in df.iterrows():
        obj1 = str_to_datetime(row[col1])
        obj2 = str_to_datetime(row[col2])
        if obj1 is np.nan or obj2 is np.nan:
            dif.append(np.nan)
        else:
            dif.append(timedif_to_seconds(obj2-obj1))
    return dif

def get_serve_times(df):

    serve_times_column = pd.DataFrame({'serve_times': get_dif(df, 'arrival_date', 'departure_date')})
    serve_times_dict = {}
    
    employee_serves = pd.concat([df[['employee_id']], serve_times_column], axis = 1)
    
    for index, row in employee_serves.dropna().groupby(['employee_id']).mean().iterrows():
        serve_times_dict[index] = row['serve_times']
    return serve_times_dict

train = pd.read_csv('C:\\Users\\Admin\\Documents\\ProjectsFolder\\Qless_v2\\train1.csv')
train = train.drop(['merchant_id','location_id','service_id'], axis=1)
test = pd.read_csv('C:\\Users\\Admin\\Documents\\ProjectsFolder\\Qless_v2\\test1.csv')
test = test.drop(['merchant_id','location_id','service_id'], axis=1)

reg, X, y = get_model(train)

cq = ConsumerQueue(reg)
forecasted = []
prev_date = datetime.strptime("9999-01-01 0:0:0", '%Y-%m-%d %H:%M:%S')

X_test = []
y_test = []

for index, consumer_id, e_type, date, employee_id, forecast_seconds in event_generator(test):
    if prev_date != date:
        cq.clear()
        prev_date = date
    seconds = datetime_to_seconds(date)
    if e_type == 'enter_date':
        cq.enter_consumer(consumer_id, seconds)
        forecasted.append([consumer_id, cq.forecast_wait_time(consumer_id,forecast_seconds,  date), seconds, None, forecast_seconds])
    elif e_type == 'summon_date':
        cq.summon_consumer(consumer_id, employee_id, seconds)
        for i in range(len(forecasted)):
            if consumer_id == forecasted[i][0]:
                forecasted[i][3] = seconds - forecasted[i][2]
                break
    elif e_type == 'departure_date' or  e_type == 'cancel_date' :
        cq.mark_departed(consumer_id)
        
loss = np.array([x[3] - x[1] for x in forecasted if x[3] is not None and x[1] is not None])
print("test squared error:  ", np.sqrt(sum(loss**2) / len(loss)))
print("test absolute error: ", sum(abs(loss) / len(loss)))
print("test max loss:       ", max(abs(loss)))
plt.scatter(X[:, 0], y)