from nba_api.stats.endpoints import BoxScoreTraditionalV2
import numpy as np

def getPlayerStats(game_id, player_id):

    boxscore = BoxScoreTraditionalV2(game_id)
    stats_df = boxscore.get_data_frames()
    player_stats_df = stats_df[0]
    player_stats_df = player_stats_df

    this_player_stats_df = player_stats_df[player_stats_df['PLAYER_ID'] == player_id]
    return this_player_stats_df.values()

print(getPlayerStats('0022000388', '1629023'))