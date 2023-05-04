import json

config_path = './config.json'
slack_url_path = './slack_url.json'

def load_config():

    with open(config_path, 'r') as f:
        return json.load(f)

def load_slack_config():

    try:

        with open(slack_url_path, 'r') as f:
            return json.load(f)['slack_url']

    except:
        
        return ''