from nba_api.stats.endpoints import boxscoretraditionalv2
import mysql.connector
import pandas as pd

#recieves a game id and inserts stats for all players in that game into the database. 
def extractAllGameStats(game_id):
    #connect to database:
    conn = mysql.connector.connect(user='root', password='Tomer89t$', host='localhost', database='games')
    cursor = conn.cursor()

    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id)
    player_stats = boxscore.get_data_frames()[0]
    #currently the df is only the stats of each player from the same game. 
    #there are 2 teams and roughly 12 players per team who played for some ammount of time in the game.
    #EACH ROW represents a player in that specific game
    df = pd.DataFrame(player_stats)
    for player in df:
        player_id = player['PLAYER_ID']
        position = player['START_POSITION']
        minutes_played = player['MIN']
        field_goals_made = player['FGM']
        field_goals_attempted = player['FGA']
        three_pointers_made = player['FG3M']
        three_pointers_attempted = player['FG3A']
        free_throws_made = player['FTM']
        free_throws_attempted = player['FTA']
        offensive_rebound = player['OREB']
        deffensive_rebound = player['DREB']
        assists = player['AST']
        steals = player['STL']
        blocks = player['BLK']
        turnovers = player['TO']
        power_fouls = player['PF']
        all_points = player['PTS']
        plus_minus = player['PLUS_MINUS']
        #now that we have the data we can insert it into the database
        sql = """INSERT IGNORE into statistics
        (gameId, playerId, points, pos, minutes_played, fgm, fga, tpm, tpa, ftm, fta, offRebound, defRebound, assists, persFouls, steals, turnovers, blocks, plusMinus)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, (game_id, player_id, all_points, position, minutes_played, field_goals_made, field_goals_attempted, three_pointers_made, three_pointers_attempted, free_throws_made, free_throws_attempted, offensive_rebound, deffensive_rebound, assists, power_fouls, steals, turnovers, blocks, plus_minus))
        
extractAllGameStats('0022000408')