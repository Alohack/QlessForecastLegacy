import numpy as np
from datetime import datetime
from EventGenerator import datetime_to_seconds

class ConsumerTime:
    def __init__(self, consumer_id, time):
        self.consumer_id = consumer_id
        self.time = time

class ConsumerQueue:
    def __init__(self, reg = None):
        self.time = None
        self.wait_queue = []
        self.serve_queue = []
        self.reg = reg
            
    def enter_consumer(self, consumer_id, time):
        self.wait_queue.append(ConsumerTime(consumer_id, time))
        
    def summon_consumer(self, consumer_id, employee_id, time):
        self.wait_queue = [item for item in self.wait_queue if (item.consumer_id!=consumer_id)]
        self.serve_queue.append(ConsumerTime(consumer_id, time))
        
    def mark_departed(self, consumer_id):
        self.serve_queue = [item for item in self.serve_queue if (item.consumer_id!=consumer_id)]
            
    def clear(self):
        self.time = None
        self.wait_queue = []
        self.serve_queue = []
        
    def forecast_wait_time(self, consumer_id, time):
        seconds = datetime_to_seconds(time)
        return self.reg.predict(np.array([[len(self.wait_queue)]]))[0]