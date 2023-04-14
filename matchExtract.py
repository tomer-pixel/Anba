import mysql.connector
import pandas as pd
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import boxscoretraditionalv2
from nba_api.stats.endpoints import scoreboardv2
#connecting to the database:
class Match:
    #creating an object of a Match to insert easily
    def __init__(self, team1, team2, score1, score2, season, game_id):
        setattr(self, 'team1', team1)
        setattr(self, 'team2', team2)
        setattr(self, 'score1', score1)
        setattr(self, 'score2', score2)
        setattr(self, 'season', season)
        setattr(self, 'game_id', game_id)
    def toTupple(match):
        object = (match.team1, match.team2, match.score1, match.score2, match.season, match.game_id)
        print(type(object))
        return object
        print(type(object))
conn = mysql.connector.connect(
host = "localhost",
user = "root",
passwd = "Tomer89t$",
database = "games"
)
cursor = conn.cursor()

#receives a game id and uploads all basic info  from that game to the matches table in the database.
def extractGame(game_id, season):
    conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "Tomer89t$",
    database = "games"
    )
    cursor = conn.cursor()
    # Retrieve the box score data for the specified game ID
    bs = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id)
    team_stats = bs.team_stats.get_data_frame()
    # Extract the DataFrame containing the final score information
    score_df = bs.get_data_frames()[1]

    # Extract the final score for each team
    away_score = score_df.iloc[0]['PTS']
    home_score = score_df.iloc[1]['PTS']

    team_ids = team_stats['TEAM_ID']
    visitor_team = team_ids[0]
    home_team = team_ids[1]

    team1 = home_team
    team2 = visitor_team
    score1 = home_score
    score2 = away_score
    if(team1 == None or team2 == None or score1 == None or score2 == None or season == None):
        print("fuck")
    else:
        match_object = Match(int(team1), int(team2), int(score1), int(score2), str(season), str(game_id))
        match_object = Match.toTupple(match_object)
        #this is the basic information that every nba game will have, no matter who plays
        sql = """INSERT IGNORE INTO matches
        (TeamId1, TeamId2, TeamId1Score, TeamId2Score, Season, MatchId) 
        VALUES(%s, %s, %s, %s, %s, %s)"""
        print(game_id)
        cursor.execute(sql, match_object)
        conn.commit()
#recieves a season and inserts all games from that particular season
def extractSeasonMatches(season):
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable = season)
    games = game_finder.get_data_frames()[0]
    game_ids = games['GAME_ID'].tolist()
    print(len(game_ids))
    for game in game_ids:
        extractGame(str(game), str(season))

extractSeasonMatches('2022')

