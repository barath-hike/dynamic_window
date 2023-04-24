from kazoo.client import KazooClient
import json
import requests
from datetime import datetime
import pytz

from utils.config_utils import load_config

def zk_connection():

    config = load_config()

    host = config[config['env']]['zk']['host']

    zk = KazooClient(hosts=host)  # Replace with your Zookeeper connection string
    zk.start()  # Establish the connection

    return zk

def update_znode(zk, game, znode_path, new_data, default_data, slack_url):

    ist_timezone = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist_timezone)

    if now_ist.hour >= 2 and now_ist.hour < 5:
        path = znode_path + game + '/120-300'
    else:
        path = znode_path + game

    try:
        json_data = json.dumps(new_data, indent=4)
        
        if zk.exists(path):
            zk.set(path, json_data.encode('utf-8'))
            message = f"Znode '{path}' updated with new data: {json_data}"

            updated_window = new_data.copy()

        else:
            message = f"Znode '{path}' not found"

            updated_window = default_data.copy()

    except:

        try:
            json_data = json.dumps(default_data, indent=4)
            
            if zk.exists(path):
                zk.set(path, json_data.encode('utf-8'))
                message = f"Znode '{path}' updated with new data: {json_data}"

                updated_window = default_data.copy()

            else:
                message = f"Znode '{path}' not found"

                updated_window = default_data.copy()

        except Exception as e:

            message = f"Error updating znode '{path}': {e}"

            updated_window = default_data.copy()

    print(message)
    requests.post(slack_url, json={"text": message})

    return updated_window