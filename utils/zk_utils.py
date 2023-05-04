from kazoo.client import KazooClient
import json
import requests
from datetime import datetime
import pytz

from utils.config_utils import load_config

def zk_connection():

    config = load_config()

    zk = []

    for host in config[config['env']]['zk']['host']:

        zk_conn = KazooClient(hosts=host)  # Replace with your Zookeeper connection string
        zk_conn.start()  # Establish the connection

        zk.append(zk_conn)

    return zk

def path_exists_in_zk(zk_list, path):

    for zk_conn in zk_list:

        try:

            if zk_conn.exists(path):
                return True

        except:

            continue

    return False

def set_zknode(zk, path, json_data):

    for zk_conn in zk:

        try:

            zk_conn.set(path, json_data)
            return True

        except:

            continue

    return False

def update_znode(zk, game, znode_path, new_data, default_data, slack_url):

    ist_timezone = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist_timezone)

    if now_ist.hour >= 2 and now_ist.hour < 5:
        path = znode_path + game + '/120-300'
    else:
        path = znode_path + game

    try:

        json_data = json.dumps(new_data, indent=4).encode('utf-8')

        if path_exists_in_zk(zk, path):

            if not set_zknode(zk, path, json_data):
                raise Exception("Failed to update znode with new data")

            message = f"Znode '{path}' updated with new data: {new_data}"
            updated_window = new_data.copy()

        else:
            message = f"Znode '{path}' not found"
            updated_window = default_data.copy()

    except:

        try:

            json_data = json.dumps(default_data, indent=4).encode('utf-8')

            if path_exists_in_zk(zk, path):

                if not set_zknode(zk, path, json_data):
                    raise Exception("Failed to update znode with default data")

                message = f"Znode '{path}' updated with new data: {default_data}"
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