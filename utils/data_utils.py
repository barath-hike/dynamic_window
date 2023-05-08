import pandas as pd
import requests
from utils.config_utils import load_config

def get_data(slack_url, fallback_data):

    config = load_config()

    game = config['game']

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
      where a.game in ('"""+game+"""') and table_amount = 1)
      where row = 1

    """

    try:

      df = pd.read_gbq(query = query, use_bqstorage_api=True, project_id='analytics-156605')

      df_dict = df.set_index('minute')[['num_users', 'mm_started']].to_dict('index')

      result_dict = {str(minute): df_dict.get(minute, {'num_users': 0, 'mm_started': 0}) for minute in range(1, 145)}

      message = 'Data loaded successfully'

    except:

      if fallback_data == {}:

        result_dict = {str(minute): {'num_users': 0, 'mm_started': 0} for minute in range(1, 145)}

      else:

        result_dict = fallback_data

      message = 'Data load failed, using old data'

    print(message)
    requests.post(slack_url, json={"text": message})

    return result_dict