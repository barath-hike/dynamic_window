from kafka import KafkaConsumer
from json import loads
import pickle
import pytz
from datetime import datetime
from time import sleep
import threading

from utils.config_utils import load_config

def get_consumer():

    config = load_config()
    kafka_config = config[config['env']]

    hosts = kafka_config['kafka']['hosts']
    topic_name = kafka_config['kafka']['topic_name']
    group_id = kafka_config['kafka']['group_id']

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
        self.start_timer()

    @staticmethod
    def round_time_to_ten_minutes(dt):
        minute = (dt.minute // 10) * 10
        return dt.replace(minute=minute, second=0, microsecond=0)

    @staticmethod
    def calculate_counter(dt):
        return dt.hour * 6 + dt.minute // 10 + 1

    def add(self, record):
        self.data.append(record)

    def dump_data(self):

        if len(self.data) == 0:

            out = {
                'mm_starts': 0,
                'num_users': 0
            }

        else:

            out = {
                'mm_starts': len(set((d['userid'], d['mmid']) for d in self.data)),
                'num_users': len(set(d['userid'] for d in self.data))
            }

        current_time_ist = datetime.now(self.ist)
        current_time_ist = current_time_ist.strftime('%Y-%m-%d')

        filename = f'../dynamic_window_data/{self.pre}_{current_time_ist}_{self.counter}.pickle'

        with open(filename, 'wb') as f:
            pickle.dump(out, f)

        print("Saved " + str(out) + " at")
        print(filename)

    def reset(self, new_start_time):
        self.current_time_rounded = new_start_time
        self.data = []
        self.counter = self.calculate_counter(self.current_time_rounded)

    def start_timer(self):
        timer_thread = threading.Thread(target=self.periodic_dump)
        timer_thread.daemon = True
        timer_thread.start()

    def periodic_dump(self):
        while True:
            current_time_ist = datetime.now(self.ist)
            current_time_rounded = self.round_time_to_ten_minutes(current_time_ist)
            if current_time_rounded > self.current_time_rounded:
                self.dump_data()
                self.reset(current_time_rounded)
            sleep(10)  # sleep for 60 seconds