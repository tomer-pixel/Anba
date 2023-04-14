from nba_api.stats.endpoints import boxscoretraditionalv2
import mysql.connector
import pandas as pd
from nba_api.stats.endpoints import LeagueGameFinder
from nba_api.stats.endpoints import boxscoresummaryv2
def nanToNull(value):
    if(value == float("NaN")):
        value = None
    return value
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
    for player in df.index:
        player_id = df['PLAYER_ID'].iloc[player]
        position = df['START_POSITION'].iloc[player]
        minutes_played = df['MIN'].iloc[player]
        field_goals_made = df['FGM'].iloc[player]
        field_goals_attempted = df['FGA'].iloc[player]
        three_pointers_made = df['FG3M'].iloc[player]
        three_pointers_attempted = df['FG3A'].iloc[player]
        free_throws_made = df['FTM'].iloc[player]
        free_throws_attempted = df['FTA'].iloc[player]
        offensive_rebound = df['OREB'].iloc[player]
        deffensive_rebound = df['DREB'].iloc[player]
        assists = df['AST'].iloc[player]
        steals = df['STL'].iloc[player]
        blocks = df['BLK'].iloc[player]
        turnovers = df['TO'].iloc[player]
        power_fouls = df['PF'].iloc[player]
        all_points = df['PTS'].iloc[player]
        plus_minus = df['PLUS_MINUS'].iloc[player]

        #now that we have the data we can insert it into the database
        sql = """INSERT IGNORE into statistics
        (gameId, playerId, points, pos, minutes_played, fgm, fga, tpm, tpa, ftm, fta, offRebound, defRebound, assists, persFouls, steals, turnovers, blocks, plusMinus)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        if(minutes_played != None):
            cursor.execute(sql, (int(game_id), int(player_id), int(all_points), str(position), minutes_played, int(field_goals_made), int(field_goals_attempted), int(three_pointers_made), int(three_pointers_attempted), int(free_throws_made), int(free_throws_attempted), int(offensive_rebound), int(deffensive_rebound), int(assists), int(power_fouls), int(steals), int(turnovers), float(blocks), float(plus_minus)))
            conn.commit()
    cursor.close()
    conn.close()  

def extractSeasonGames(season):
    game_finder = LeagueGameFinder(season_nullable=season)
    games = game_finder.get_data_frames()[0]
    game_ids = games['GAME_ID'].tolist()
    print(len(game_ids))
    game_ids.reverse()
    for game in game_ids:
        extractAllGameStats(str(game))
        print(game_ids.index(game))

extractSeasonGames('2013')