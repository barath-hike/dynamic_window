import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from utils.model_utils import load_models, get_window
from utils.data_utils import get_data
from utils.zk_utils import zk_connection, update_znode
from utils.timer_utils import nearest_minute_10, nearest_midnight_noon, nearest_10_minutes_ist
from utils.config_utils import load_config, load_slack_config
from utils.mongo_utils import mongo_connection

config = load_config()

game = config['game']
znode_path = config['znode_path']
window = config['window']

slack_url = load_slack_config()

# zk connection
zk = zk_connection()

# mongo connection
col = mongo_connection()

# load ml models
scaler, dist = load_models()

# load data
data = get_data(slack_url)

# function to load data periodically

def call_get_data(slack_url):

    global data

    data = get_data(slack_url)

# window config update function

def update_window(zk, game, znode_path, data, scaler, dist, window, slack_url):

    time = nearest_10_minutes_ist()

    num_users = data[str(time)]['num_users']
    mm_starts = data[str(time)]['mm_started']

    v4_window = get_window([0.15, 3, 6, 9, 10, 11, num_users, mm_starts], scaler, dist, agg_type='mean')

    window_new = window.copy()
    window_new['v4'] = v4_window

    update_znode(zk, game, znode_path, window_new, window, slack_url)

if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    # Schedule function_1 to run every 10 minutes, starting from the nearest 10th minute
    start_time_1 = nearest_minute_10(datetime.now())
    scheduler.add_job(update_window, 'interval', minutes=10, start_date=start_time_1, 
                      args=[zk, game, znode_path, data, scaler, dist, window, slack_url])

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