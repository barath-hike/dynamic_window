from kazoo.client import KazooClient
import json
import requests

def zk_connection():

    zk = KazooClient(hosts='10.20.0.21:2181')  # Replace with your Zookeeper connection string
    zk.start()  # Establish the connection

    return zk

def update_znode(zk, path, new_data, default_data, slack_url):

    try:
        json_data = json.dumps(new_data, indent=4)
        
        if zk.exists(path):
            zk.set(path, json_data.encode('utf-8'))
            message = f"Znode '{path}' updated with new data: {json_data}"
        else:
            message = f"Znode '{path}' not found"

    except:

        try:
            json_data = json.dumps(default_data, indent=4)
            
            if zk.exists(path):
                zk.set(path, json_data.encode('utf-8'))
                message = f"Znode '{path}' updated with new data: {json_data}"
            else:
                message = f"Znode '{path}' not found"

        except Exception as e:

            message = f"Error updating znode '{path}': {e}"

    print(message)
    requests.post(slack_url, json={"text": message})