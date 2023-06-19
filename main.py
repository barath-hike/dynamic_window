import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import pytz
import os
import pickle

from utils.generative_model_utils import get_window as get_window_generative
from utils.genetic_algorithm_utils import get_window as get_window_genetic
from utils.data_utils import get_data
from utils.zk_utils import zk_connection, update_znode
from utils.timer_utils import nearest_minute, nearest_midnight_noon, nearest_10_minutes_ist
from utils.config_utils import load_config, load_slack_config
from utils.mongo_utils import mongo_connection, push_to_mongo

config = load_config()

games = config['game']
tables = config['table']
table_ids = config['table_id']
znode_path = config['znode_path']
window = config['window']
algo = config['algo']
generative_configs = config['generative_configs']

slack_url = load_slack_config()

# zk connection
zk = zk_connection()

# mongo connection
col = mongo_connection()

# load data
data = get_data(slack_url, {})

# function to load data periodically

def call_get_data():

    global data

    data = get_data(slack_url, data)

# window config update function

def update_window():

    minute = nearest_10_minutes_ist()

    ist = pytz.timezone('Asia/Kolkata')
    date = datetime.now(ist)

    if minute == 1:
        date = date - timedelta(days=1)
        num = 144
    else:
        num = minute - 1

    date = date.strftime('%Y-%m-%d')

    for i, game in enumerate(games):

        num_users_ratio = data[game][tables[i]][str(minute)]['num_users']
        mm_starts_ratio = data[game][tables[i]][str(minute)]['mm_started']

        file_path = f'../dynamic_window_data/{table_ids[i]}_{date}_{num}.pickle'
        
        while not os.path.exists(file_path):
            time.sleep(1)

        with open(file_path, 'rb') as f:
            liquidity = pickle.load(f)

        num_users = liquidity['num_users'] * num_users_ratio
        mm_starts = liquidity['mm_starts'] * mm_starts_ratio

        if algo == 'generative':
            v4_window = get_window_generative([*generative_configs[i], num_users, mm_starts], agg_type='mean')
        elif algo == 'genetic':
            v4_window = get_window_genetic(num_users, mm_starts, minute, game, table=tables[i])

        window_new = window[0].copy()
        window_new['v4'] = v4_window

        # updated_window = update_znode(zk, game, znode_path, window_new, window, slack_url)
        print(f"Window for table {tables[i]} is {v4_window}")

        # push_to_mongo(col, game, tables[i], minute, updated_window, num_users, mm_starts)

if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    if algo == 'generative':
        rounding = 9.5
    elif algo == 'genetic':
        rounding = 8.5

    # Schedule function_1 to run every 10 minutes, starting from the nearest 10th minute
    start_time_1 = nearest_minute(datetime.now(), rounding=rounding)
    scheduler.add_job(update_window, 'interval', minutes=10, start_date=start_time_1)

    # Schedule function_2 to run every 12 hours, starting from the nearest midnight or noon
    start_time_2 = nearest_midnight_noon(datetime.now())
    scheduler.add_job(call_get_data, 'interval', hours=12, start_date=start_time_2)

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()