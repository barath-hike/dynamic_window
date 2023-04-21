from kazoo.client import KazooClient
import json

def zk_connection():

    zk = KazooClient(hosts='10.20.0.21:2181')  # Replace with your Zookeeper connection string
    zk.start()  # Establish the connection

    return zk

def update_znode(zk, path, new_data):

    json_data = json.dumps(new_data, indent=4)
    
    if zk.exists(path):
        zk.set(path, json_data.encode('utf-8'))
        print(f"Znode '{path}' updated with new data: {json_data}")
    else:
        print(f"Znode '{path}' not found")