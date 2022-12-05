import sqlite3
import csv
import pandas as pd
import os.path

if __name__ == "__main__":
    conn = sqlite3.connect('basketball.db')
    c = conn.cursor()

    if not os.path.exists("basketball.db"):

        #Build Database

        c.execute('''CREATE TABLE players 
                    (player_name text, team_id int, player_id int, season int)''')
        players = pd.read_csv('players.csv')
        players.to_sql('players', conn, if_exists='append', index = False)

        c.execute('''CREATE TABLE teams ( league_id int, team_id int, min_year int, max_year int, 
                    abbreviation text, nickname text, yearfounded int, city text, 
                    arena text, arenacapacity int, owner text, generalmanager text, 
                    headcoach text, dleagueaffiliation text)''')
        teams = pd.read_csv('teams.csv')
        teams.to_sql('teams', conn, if_exists='append', index = False)

        c.execute('''CREATE TABLE rankings (team_id int, league_id int, season_id int, 
                    standingsdate text, conference text, team text, g int, w int, l int, w_pct real, 
                    home_record text, road_record text, returntoplay text)''')
        rankings = pd.read_csv('ranking.csv')
        rankings.to_sql('rankings', conn, if_exists='append', index = False)

        c.execute('''CREATE TABLE games (game_date_est text, game_id int, game_status_text text, 
                    home_team_id int, visitor_team_id int, season int, team_id_home int, pts_home int, 
                    fg_pct_home real, ft_pct_home real, fg3_pct_home real, ast_home int, reb_home int, 
                    team_id_away int, pts_away int, fg_pct_away real, ft_pct_away real, fg3_pct_away real, 
                    ast_away int, reb_away int, home_team_wins int)''')
        games = pd.read_csv('games.csv')
        games.to_sql('games', conn, if_exists='append', index = False)

        c.execute('''CREATE TABLE games_details (game_id int, team_id int, team_abbreviation text, 
                    team_city text, player_id int, player_name text, nickname text, start_position text, 
                    comment text, min text, fgm int, fga int, fg_pct real, fg3m int, fg3a int, fg3_pct real, 
                    ftm int, fta int, ft_pct real, oreb int, dreb int, reb int, ast int, stl int, blk int, [to] 
                    int, pf int, pts int, plus_minus int)''')
        games_details = pd.read_csv('games_details.csv')
        games_details.to_sql('games_details', conn, if_exists='append', index = False)


        c.execute('''CREATE VIEW game_wins as SELECT game_id, CASE WHEN games.home_team_wins = 1 
                    THEN home_team_id ELSE visitor_team_id END AS team_id FROM games''')

        c.execute('''CREATE VIEW player_wins AS SELECT games_details.game_id, games_details.team_id, 
                    games_details.player_id, games_details.player_name, game_wins.team_id AS winning_team 
                    FROM games_details LEFT JOIN game_wins ON games_details.game_id = game_wins.game_id 
                    AND games_details.team_id = game_wins.team_id''')
   

    df_game_wins = pd.read_sql_query('''SELECT * FROM game_wins''', conn)
    df_player_wins = pd.read_sql_query('''SELECT * FROM player_wins''', conn)