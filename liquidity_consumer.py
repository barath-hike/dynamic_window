from utils.config_utils import load_config
from utils.kafka_utils import get_consumer, DataAggregator

consumer = get_consumer()

config = load_config()

table_ids = config['table_id']
aggregators = {table_id:DataAggregator(table_id) for table_id in table_ids}

def check_start(string, str_list):
    for i, s in enumerate(str_list):
        if string.startswith(s):
            return True, i
    return False, None

for message in consumer:

    value = message.value

    if 'o' in value and 'k' in value and value['k'] == 'rush' and value['o'] == 'table_search_started' and 'f' in value and 'fu' in value and 's' in value:

        table_id = value['s']

        is_table, index = check_start(table_id, table_ids)

        if is_table:

            userid = value['fu']
            mmid = value['f']

            record = {'userid': userid, 'mmid': mmid}

            data_agg = aggregators[table_ids[index]].add(record)