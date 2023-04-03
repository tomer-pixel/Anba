import nba_api.stats.static.players as players
from nba_api.stats.static import teams 
from nba_api.stats.endpoints import CommonPlayerInfo
import mysql.connector
import pandas as pd

#connect to database
conn = mysql.connector.connect(user='root', password='Tomer89t$', host='localhost', database='games')
cursor = conn.cursor()

def addAllPlayers():
    conn = mysql.connector.connect(user='root', password='Tomer89t$', host='localhost', database='games')
    cursor = conn.cursor()

    #insert players
    all_players = players.get_players()
    player_dict = all_players
    for player in reversed(player_dict):
        player_info = CommonPlayerInfo(player_id=player['id'])
        player_data = player_info.get_normalized_dict()
        this_player_info = player_data['CommonPlayerInfo']
        data = this_player_info[0]
        player_id = data['PERSON_ID']
        team_id = data['TEAM_ID']
        first_name = data['FIRST_NAME']
        last_name = data['LAST_NAME']
        
        #inserting the now existing data into the database
        sql = "INSERT IGNORE INTO players(PlayerId, FirstName, LastName, TeamId) VALUES (%s, %s, %s, %s)"
        PlayerId = player_id
        FirstName = first_name
        LastName = last_name
        TeamId = team_id
        
        cursor.execute(sql, (PlayerId, FirstName, LastName, TeamId))
        conn.commit()
        print("added player " + first_name + " " + last_name)
    cursor.close()
    conn.close()
        

def addTeams():
        #insert teams into databases
    from nba_api.stats.static import teams 
    import mysql.connector

    #connect to database
    conn = mysql.connector.connect(user='root', password='Tomer89t$', host='localhost', database='games')
    cursor = conn.cursor()

    all_teams = teams.get_teams()
    for team in all_teams:
        team_id = team['id']
        full_name = team['full_name']
        city = team['city']
        sql = "INSERT IGNORE INTO teams(TeamId, Name, City) VALUES (%s, %s, %s)"
        cursor.execute(sql, (team_id, full_name, city))
        print("successfully added " + full_name)
        conn.commit()
    cursor.close()
    conn.commit()

addAllPlayers()