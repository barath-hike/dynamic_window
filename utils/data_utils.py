import pandas as pd
import requests
import pandas_gbq
from google.auth import compute_engine

from utils.config_utils import load_config

project_id = 'analytics-156605'

credentials = compute_engine.Credentials()
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id

def get_data(slack_url, fallback_data):

    config = load_config()

    games = [temp["game_name"] for temp in config['games']]
    tables = [item["amount"] for temp in config['games'] for item in temp["tables"]]

    for i, game in enumerate(games):
      if i == 0:
        game_string = "'" + game + "'"
      else:
        game_string = game_string + ', ' + "'" + game + "'"

    for i, table in enumerate(tables):
      if i == 0:
        table_string = str(table)
      else:
        table_string = table_string + ', ' + str(table)

    query = """

      with data as (
      select * from (
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
      where DATE(time_stamp) >= current_date() - interval 8 day
            )
      where lower(PLATFORM) ='cash'

      group by 1, 2, 3, 4
      ) a
      where a.game in ("""+game_string+""") and table_amount in ("""+table_string+""")
      ),

      ratios AS (
        SELECT
          dt,
          minute,
          game,
          table_amount,
          LAG(mm_started) OVER (PARTITION BY game, cast(table_amount as string) ORDER BY dt, minute) as prev_mm_started,
          LAG(num_users) OVER (PARTITION BY game, cast(table_amount as string) ORDER BY dt, minute) as prev_num_users,
          mm_started,
          num_users
        FROM
          data),
        
        ratio_calculations AS (
        SELECT
          dt,
          minute,
          game,
          table_amount,
          IF(minute=1 AND prev_mm_started IS NULL, mm_started / LAG(mm_started) OVER (PARTITION BY game, cast(table_amount as string) ORDER BY dt DESC, minute DESC), mm_started / prev_mm_started) as mm_started_ratio,
          IF(minute=1 AND prev_num_users IS NULL, num_users / LAG(num_users) OVER (PARTITION BY game, cast(table_amount as string) ORDER BY dt DESC, minute DESC), num_users / prev_num_users) as num_users_ratio
        FROM
          ratios)
        
      SELECT
        game,
        table_amount,
        minute,
        AVG(mm_started_ratio) as mm_started,
        AVG(num_users_ratio) as num_users
      FROM
        ratio_calculations
      WHERE
        dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      GROUP BY
        1, 2, 3

    """

    try:

      df = pd.read_gbq(query = query, use_bqstorage_api=True, project_id=project_id, credentials=credentials)

      result_dict = {}

      for i, game in enumerate(config['games']):

        for j, table in enumerate(game["tables"]):

          df_dict = df[(df['game'] == game["game_name"]) & (df['table_amount'] == table["amount"])].set_index('minute')[['num_users', 'mm_started']].to_dict('index')

          if game["game_name"] not in result_dict:

            result_dict[game["game_name"]] = {}

          result_dict[game["game_name"]][table["amount"]] = {str(minute): df_dict.get(minute, {'num_users': 1, 'mm_started': 1}) for minute in range(1, 145)}

      message = 'Data loaded successfully'

    except Exception as e:

      if fallback_data == {}:

        result_dict = {}

        for i, game in enumerate(config['games']):

          for j, table in enumerate(game["tables"]):

            if game["game_name"] not in result_dict:

              result_dict[game["game_name"]] = {}

            result_dict[game["game_name"]][table["amount"]] = {str(minute): {'num_users': 1, 'mm_started': 1} for minute in range(1, 145)}

      else:

        result_dict = fallback_data

      print(e)

      message = 'Data load failed, using old data'

    print(message)
    # requests.post(slack_url, json={"text": message})

    return result_dict