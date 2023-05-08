import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from utils.generative_model_utils import get_window as get_window_generative
from utils.genetic_algorithm_utils import get_window as get_window_genetic
from utils.data_utils import get_data
from utils.zk_utils import zk_connection, update_znode
from utils.timer_utils import nearest_minute, nearest_midnight_noon, nearest_10_minutes_ist
from utils.config_utils import load_config, load_slack_config
from utils.mongo_utils import mongo_connection, push_to_mongo

config = load_config()

game = config['game']
znode_path = config['znode_path']
window = config['window']
algo = config['algo']

slack_url = load_slack_config()

# zk connection
zk = zk_connection()

# mongo connection
col = mongo_connection()

# load data
data = get_data(slack_url, {})

# function to load data periodically

def call_get_data(slack_url):

    global data

    data = get_data(slack_url, data)

# window config update function

def update_window(algo, zk, game, znode_path, data, window, slack_url):

    minute = nearest_10_minutes_ist()

    num_users = data[str(minute)]['num_users']
    mm_starts = data[str(minute)]['mm_started']

    if algo == 'generative':
        v4_window = get_window_generative([0.15, 3, 6, 9, 10, 11, num_users, mm_starts], agg_type='mean')
    elif algo == 'genetic':
        v4_window = get_window_genetic(num_users, mm_starts, minute, game)

    window_new = window.copy()
    window_new['v4'] = v4_window

    updated_window = update_znode(zk, game, znode_path, window_new, window, slack_url)

    push_to_mongo(col, game, 1.0, minute, updated_window, num_users, mm_starts)

if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    if algo == 'generative':
        rounding = 9.5
    elif algo == 'genetic':
        rounding = 8.5

    # Schedule function_1 to run every 10 minutes, starting from the nearest 10th minute
    start_time_1 = nearest_minute(datetime.now(), rounding=rounding)
    scheduler.add_job(update_window, 'interval', minutes=10, start_date=start_time_1, 
                      args=[algo, zk, game, znode_path, data, window, slack_url])

    # Schedule function_2 to run every 12 hours, starting from the nearest midnight or noon
    start_time_2 = nearest_midnight_noon(datetime.now())
    scheduler.add_job(call_get_data, 'interval', hours=12, start_date=start_time_2,
                      args=[slack_url])

    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()