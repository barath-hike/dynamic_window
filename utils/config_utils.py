import json

def load_config(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def load_slack_config(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)['slack_url']