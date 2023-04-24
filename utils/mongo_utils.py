import pymongo
import urllib.parse
from utils.config_utils import load_config
from datetime import datetime
import pytz
import time

def mongo_connection():

    config = load_config()
    mongo_config = config[config['env']]['mongo']

    host = mongo_config['host']
    port = mongo_config['port']
    user_name = mongo_config['user_name']
    pass_word = mongo_config['pass_word']
    db_name = mongo_config['db_name']
    collection_name = mongo_config['collection_name']

    client = pymongo.MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}')

    db = client[db_name]
    col = db[collection_name]

    return col

def push_to_mongo(col, game, table, minute, window, num_users, mm_starts):

    ist_timezone = pytz.timezone("Asia/Kolkata")
    date = datetime.now(ist_timezone).strftime("%Y-%m-%d")

    data = {
        'dt': date,
        'minute': int(minute),
        'game_table': f'{game}-{table:.2f}',
        'time_stamp': time.time(),
        'window': window,
        'num_users': int(num_users),
        'mm_starts': int(mm_starts)
    }

    print(data)

    col.insert_one(data)