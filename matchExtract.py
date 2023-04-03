import mysql.connector
import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import boxscoresummaryv2
from nba_api.stats.endpoints import leaguegamefinder

    #connecting to the database:
conn = mysql.connector.connect(
host = "localhost",
user = "root",
passwd = "Tomer89t$",
database = "games"
)
cursor = conn.cursor()

#receives a game id and uploads all stats from that game to the statistics table in the database.
def extractGame(game_id):
    conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "Tomer89t$",
    database = "games"
    )
    cursor = conn.cursor()
    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id = game_id)
    player_stats = boxscore.player_stats.get_data_frame()
    pd_stats = pd.DataFrame(player_stats)
    #there are 2 tables that are needed to be filled: matches and statistics.
    #first ill deal with the matches table
    team1 = pd_stats['TEAM_ID'].iloc[0]
    team2 = pd_stats['TEAM_ID'].iloc[1]
    score1 = pd_stats['PTS'].iloc[0]
    for i in range(len(pd_stats)):
        team2 = pd_stats['TEAM_ID'].iloc[i]
        if(team2 != team1):
            score2 = pd_stats['PTS'].iloc[i]
            break
    boxscore_summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id)
    game_info = boxscore_summary.game_summary.get_data_frame()
    season = game_info['SEASON'].values
    season = season[0]
    print(game_id)
    #this is the basic information that every nba game will have, no matter who plays
    sql = "INSERT IGNORE INTO matches(TeamId1, TeamId2, TeamId1Score, TeamId2Score, Season, MatchId) VALUES(%s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (str(team1), str(team2), str(score1), str(score2), str(season), str(game_id)))
    conn.commit()
cursor.close()
conn.close()    

def extractSeasongames(season):
    gamefinder = leaguegamefinder.LeagueGameFinder()
    # The first DataFrame of those returned is what we want.
    games = gamefinder.get_data_frames()[0]
    wanted_games = games[games.SEASON_ID.str[-4:] == str(season)]
    game_ids = wanted_games['GAME_ID'].values
    unique_games = []
    for id in game_ids:
        if (id not in unique_games):
            unique_games.append(id)
    for id in unique_games:
        extractGame(str(id))

    
extractSeasongames('2020')