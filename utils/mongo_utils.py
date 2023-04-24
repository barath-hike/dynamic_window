import pymongo
import urllib.parse
from utils.config_utils import load_config

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