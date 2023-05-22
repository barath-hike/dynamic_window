from kafka import KafkaConsumer
from json import loads
import pickle
import pytz
import pandas as pd
from datetime import datetime

from utils.config_utils import load_config

def get_consumer():

    config = load_config()

    hosts = config[config['env']]['kafka']['hosts']
    topic_name = config[config['env']]['kafka']['topic_name']
    group_id = config[config['env']]['kafka']['group_id']

    consumer = KafkaConsumer(*topic_name, bootstrap_servers=hosts, enable_auto_commit=True, group_id=group_id, value_deserializer=lambda x: loads(x))

    return consumer

class DataAggregator:

    def __init__(self, pre):

        self.pre = pre

        self.ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(self.ist)

        self.current_time_rounded = self.round_time_to_ten_minutes(current_time_ist)
        self.start_time = self.current_time_rounded

        self.data = []
        self.counter = self.calculate_counter(self.current_time_rounded)

    @staticmethod
    def round_time_to_ten_minutes(dt):
        minute = (dt.minute // 10) * 10
        return dt.replace(minute=minute, second=0, microsecond=0)

    @staticmethod
    def calculate_counter(dt):
        return dt.hour * 6 + dt.minute // 10 + 1

    def add(self, record):
        
        current_time_ist = datetime.now(self.ist)
        current_time_rounded = self.round_time_to_ten_minutes(current_time_ist)
        if current_time_rounded > self.current_time_rounded:
            self.dump_data()
            self.reset(current_time_rounded)

        self.data.append(record)

    def dump_data(self):

        data = pd.DataFrame(self.data)
        out = {
            'mm_starts': len(data[['userid', 'mmid']].drop_duplicates()),
            'num_users': len(data['userid'].unique())
        }

        current_time_ist = datetime.now(self.ist)
        current_time_ist = current_time_ist.strftime('%Y-%m-%d')

        with open(f'../dynamic_window_data/{self.pre}_{current_time_ist}_{self.counter}.pickle', 'wb') as f:
            pickle.dump(out, f)

    def reset(self, new_start_time):
        
        self.current_time_rounded = new_start_time
        self.data = []
        self.counter = self.calculate_counter(self.current_time_rounded)