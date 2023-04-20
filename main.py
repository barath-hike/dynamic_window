from kazoo.client import KazooClient
import json
import numpy as np
import pytz
import pickle
import tensorflow as tf
import os
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

from gen_model import GenModel

tfk = tf.keras

os.environ["CUDA_DEVICE_ORDER"]="PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"]="1"

physical_devices = tf.config.list_physical_devices('GPU')
for physical_device in physical_devices:
    tf.config.experimental.set_memory_growth(physical_device, True)

# zk config

zk = KazooClient(hosts='10.20.0.21:2181')  # Replace with your Zookeeper connection string
zk.start()  # Establish the connection

znode_path = "/mazuma/matchmaking/milestonesMapPerGame/fruitfight"

# scaler

with open('./models/scaler.pickle', 'rb') as f:
    scaler = pickle.load(f)

# neural network model

distribution = 'lognormal'

dist = GenModel((8,), (15,), distribution=distribution, num_mixtures=20)
inp = tfk.Input(shape=(8,))
out = dist.network.call(inp)

model = tfk.Model(inputs=inp, outputs=out)

save_dir = './models/window_logic_gen_model_' + distribution + '/'

model.load_weights(save_dir + 'weights.hdf5')

# get data

query = """

select * from (
select a.*, row_number() over(partition by minute order by dt desc) as row
from (
select dt, minute, game_type as game, table_amount, SUM(CASE 
        WHEN flag = 'match_making_screen' AND matchmaking_complete_reason = 'SUCCESSFUL' THEN 
          CASE game_format 
            WHEN 'battle' then matchmaking_complete_under_30
            WHEN 'tournament' then matchmaking_complete_under_60
          END
        WHEN flag = 'match_making_screen_v2' AND matchmaking_complete_reason = 'SUCCESS' AND sfs_mm_complete_reason = 'MM_RESPONSE_RECV' THEN 
          CASE game_format 
            WHEN 'battle' then matchmaking_complete_under_30
            WHEN 'tournament' then matchmaking_complete_under_60
          END
        ELSE 0
      END) as mm_success  , sum(mm_started) as mm_started, count(distinct user_id) as num_users from (

select time_stamp , date(time_stamp) as dt, user_id,
COaLESCE(b.appType,'CASH') as platform , extract (hour from time_stamp) as hour, game_type,match_making_started ,
match_making_success, matchmaking_complete_reason, val_int, sfs_mm_complete_reason,flag, 
case when  match_making_started = 'table_search_started' then 1 else 0 end as mm_started ,table_id, playerCount , 
case when playerCount = 2.0 then 'battle' else 'tournament' end as game_format ,
case when val_int/1000 <30 then 1 else 0 end as matchmaking_complete_under_30 , case when val_int/1000 <60 then 1 else 0 end as matchmaking_complete_under_60 ,
case when  match_making_success = 'table_search_completed' then 1 else 0 end as mm_completed,
b.pool as table_amount,
cast(floor((extract(hour from time_stamp) * 60 + extract(minute from time_stamp))/10) as int) + 1 as minute

from `analytics-156605.rush_app_bi.matchmaking_server_event_logs_v2` a 
left join `analytics-156605.rush_app_data_import.gameTable` b on b.tableId = a.table_id
where DATE(time_stamp) >= current_date() - interval 2 day
      )
where lower(PLATFORM) ='cash'

group by 1, 2, 3, 4
) a
where a.game in ('fruitfight') and table_amount = 1)
where row = 1

"""

def get_data():

    df = pd.read_gbq(query = query, use_bqstorage_api=True)

    result_dict = {}

    for index, row in df.iterrows():

        result_dict[str(row['minute'])] = {'num_users': row['num_users'], 'mm_started': row['mm_started']}

    return result_dict

data = get_data()

def call_get_data():
    globals()['data'] = get_data()
    print('Data Loaded')

# window config update functions

def ensure_increasing_order(arr):
    for i in range(1, len(arr)):
        if arr[i] < arr[i - 1]:
            arr[i] = arr[i - 1]
    return arr

def get_window(x, agg_type='median'):

    x = np.array(x).reshape(1, -1)
    x = scaler.transform(x)
    pred = np.array(dist.sample_n(x, 10000))

    if agg_type == 'median':
        pred = np.squeeze(np.round(np.median(pred, axis=0)).astype('int32'))
    elif agg_type == 'mean':
        pred = np.squeeze(np.round(np.mean(pred, axis=0)).astype('int32'))

    pred[pred >= 500] = 5000

    ensure_increasing_order(pred)
    
    try:
        index = np.where(pred == 5000)[0][0]
        pred = pred[:index + 1]
    except:
        pred = np.append(pred, 5000)
    
    keys = (np.arange(len(pred))*1000).astype('int32')

    return dict(zip(keys.astype('str'), pred.astype('str')))

def nearest_10_minutes_ist():
    # Get the current time in IST
    ist_timezone = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist_timezone)

    # Round to the nearest 10-minute interval
    minutes = round(now_ist.minute / 10) * 10
    nearest_10_minutes = now_ist.replace(minute=minutes % 60, second=0, microsecond=0)
    if minutes // 60 > 0:
        nearest_10_minutes += timedelta(hours=1)

    return int(np.floor((int(nearest_10_minutes.strftime("%H")) * 60 + int(nearest_10_minutes.strftime("%M")))/10) + 1)

def update_znode(path, new_data):
    json_data = json.dumps(new_data, indent=4)
    
    if zk.exists(path):
        zk.set(path, json_data.encode('utf-8'))
        print(f"Znode '{path}' updated with new data: {json_data}")
    else:
        print(f"Znode '{path}' not found")

def update_window():

    window = {
        "v4": {},
        "v3": {
            "0": "1000",
            "1000": "3000",
            "2000": "5000",
            "3000": "8000",
            "4000": "14000",
            "5000": "23000",
            "6000": "1000000"
        }
    }

    time = nearest_10_minutes_ist()

    num_users = data[str(time)]['num_users']
    mm_starts = data[str(time)]['mm_started']

    v4_window = get_window([0.15, 3, 6, 9, 10, 11, num_users, mm_starts], agg_type='mean')

    window['v4'] = v4_window

    update_znode(znode_path, window)

# schduler functions

def nearest_minute_10(dt):
    return dt + timedelta(minutes=(10 - dt.minute % 10))

def nearest_midnight_noon(dt):
    if dt.hour < 12:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(hour=12, minute=0, second=0, microsecond=0)

if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    # Schedule function_1 to run every 10 minutes, starting from the nearest 10th minute
    start_time_1 = nearest_minute_10(datetime.now())
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