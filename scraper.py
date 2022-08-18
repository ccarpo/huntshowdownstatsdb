from multiprocessing.sharedctypes import Value
import xml.etree.ElementTree as ET
import sqlite3
import os
from datetime import datetime
from hashlib import sha256

PATH_TO_XML = "D:/Temp/HuntShowdownStats/"
PATH_TO_ARCHIVE = "D:/Temp/HuntShowdownStats/archive"

PLAYER_NAME = "CCarpo"

def start_ingestion():
# connect to database
    try:
        con = sqlite3.connect('hunttracker.db')
        cursor = con.cursor()

        variables_table = """ CREATE TABLE if not exists Variables (
            Key VARCHAR(30) NOT NULL primary key,
            Value VARCHAR(256) NOT NULL
        ); """

        all_matches_table = """ CREATE TABLE if not exists Matches (
            AccoladeHash VARCHAR(256) NOT NULL,
            Timestamp DATETIME NOT NULL,
            Team_Is_Invite BOOLEAN NOT NULL,
            Team_Size TINYINT NOT NULL,
            Team_Mmr  INTEGER NOT NULL,
            Game_Is_Quickplay BOOLEAN NOT NULL, 
            Game_Is_SkillbasedMM BOOLEAN NOT NULL, 
            Game_Is_Single_Bounty BOOLEAN NOT NULL, 
            Game_Boss_1 VARCHAR(30),
            Game_Boss_2 VARCHAR(30),
            Player_Is_Extracted BOOLEAN,
            Player_Is_Bounty_Extracted BOOLEAN,
            Player_Is_Bounty_Picked_Up BOOLEAN,
            Player_Mmr INTEGER NOT NULL,
            Player_Is_Soul_Survivor BOOLEAN,
            Player_Is_Wellspring_Activated BOOLEAN,
            Player_Downed_Count INTEGER NOT NULL,
            Player_Kill_Count_Total INTEGER NOT NULL,
            Player_Kill_Count_Mmr1 INTEGER NOT NULL,
            Player_Kill_Count_Mmr2 INTEGER NOT NULL,
            Player_Kill_Count_Mmr3 INTEGER NOT NULL,
            Player_Kill_Count_Mmr4 INTEGER NOT NULL,
            Player_Kill_Count_Mmr5 INTEGER NOT NULL,
            Player_Kill_Count_Mmr6 INTEGER NOT NULL,
            Player_Assist_Count INTEGER NOT NULL,
            Player_Collect_Clues_Count INTEGER NOT NULL,
            Player_Loot_Upgrade_Points INTEGER NOT NULL,
            Player_Loot_Bloodline INTEGER NOT NULL,
            Player_Loot_Hunt_Dollar INTEGER NOT NULL,
            Player_Grunts_Killed INTEGER NOT NULL,
            Player_Hives_Killed INTEGER NOT NULL,
            Player_Armored_Killed INTEGER NOT NULL,
            Player_Immolators_Killed INTEGER NOT NULL,
            Player_Waterdevils_Killed INTEGER NOT NULL
        ); """

        cursor.execute(variables_table)
        cursor.execute(all_matches_table)
        con.commit()

# read files
        files = os.listdir(PATH_TO_XML)
# for each file 
        for file in files:
            full_file = os.path.join(PATH_TO_XML, file)
            print(file)
            print(full_file)
            if os.path.isfile(full_file) and full_file.endswith("xml"):
                tree = ET.parse(full_file)
                root = tree.getroot()
                accolades = {}
                teams = {}
                players = {}
                boss = {}
                bag_entries = {}
                bag_var = {}
                
                for child in root:
                    if child.attrib['name'].startswith("MissionAccoladeEntry"):
                        accolades[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagBoss"):
                        boss[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagBoss"):
                        teams[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagTeam"):
                        players[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagEntry"):
                        bag_entries[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'] == "MissionBagIsHunterDead" \
                        or child.attrib['name'] == "MissionBagIsQuickPlay" \
                        or child.attrib['name'] == "MissionBagNumAccolades" \
                        or child.attrib['name'] == "MissionBagNumEntries" \
                        or child.attrib['name'] == "MissionBagNumTeams":
                        bag_var[child.attrib['name']] = child.attrib['value']
                #print(accolades)
                #print(boss)
                #print(teams)
                #print(players)
                #print(bag_entries)
# check if the file contains new accolades so this would be a new match and not just some other changes in the attributes.xml
                same_match = True
                read_last_accolade_hash = """ SELECT Value FROM Variables WHERE Key = 'LastAccoladeHash';"""
                cursor.execute(read_last_accolade_hash)
                last_accolade_hash = cursor.fetchone()
                current_accolade_hash = sha256(str(accolades).encode()).hexdigest()
                print(not last_accolade_hash)
                print(current_accolade_hash)
                print(last_accolade_hash != current_accolade_hash)
                if not last_accolade_hash or last_accolade_hash[0] != current_accolade_hash:
                    print("Not the same match as before")
# check if match already in db through timestamp
                    current_datetime = datetime.strptime(file, "attributes_%Y-%b-%d_%H-%M.xml")
                    formatted_current_datetime = datetime.strftime(current_datetime, "%Y-%m-%d %H:%M")
                    cursor.execute(""" SELECT Timestamp FROM Matches;""")
                    print(formatted_current_datetime)
                    all_timestamps = cursor.fetchall()
                    print(all_timestamps)
                    print(formatted_current_datetime not in str(all_timestamps))
                    if formatted_current_datetime not in all_timestamps:
# extract data & write to sqlite
                        print("Insert new match")
                        insert_match = """
                            INSERT INTO Matches VALUES (
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?, 
                                ?, 
                                ?, 
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?,
                                ?
                            );
                            """
                        cursor.execute(insert_match, (
                            current_accolade_hash, 
                            formatted_current_datetime, 
                            is_team_invite(teams), 
                            get_team_size(teams),
                            get_team_mmr(teams),
                            is_game_quickplay(bag_var),
                            is_game_skillbased_mm(players),
                            is_game_single_boss(boss),
                            get_boss(boss, 1, is_game_single_boss()),
                            get_boss(boss, 2, is_game_single_boss()),
                            has_player_extracted(players),
                            has_player_extracted_with_bounty(players),
                            has_player_picked_up_bounty(players),
                            get_player_mmr(players),
                            is_player_soul_survivor(players),
                            has_player_wellspring_activated(players),
                            get_player_downed_count(players),
                            get_player_kill_count_total(),
                            get_player_kill_count_mmr1(bag_entries),
                            get_player_kill_count_mmr2(bag_entries),
                            get_player_kill_count_mmr3(bag_entries),
                            get_player_kill_count_mmr4(bag_entries),
                            get_player_kill_count_mmr5(bag_entries),
                            get_player_kill_count_mmr6(bag_entries),
                            get_player_assist_count(bag_entries),
                            get_player_collected_clues_count(bag_entries),
                            get_player_loot_upgrade_points_count(bag_entries),
                            get_player_loot_bloodline_count(bag_entries),
                            get_player_loot_hunt_dollar_count(bag_entries),
                            get_player_grunts_killed(bag_entries),
                            get_player_hives_killed(bag_entries),
                            get_player_armored_killed(bag_entries),
                            get_player_immolators_killed(bag_entries),
                            get_player_waterdevils_killed(bag_entries),
                            ))
                        con.commit()
                       
# replace last match
                        cursor.execute("INSERT INTO Variables('Key','Value') VALUES ('LastAccoladeHash', ?) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;", (current_accolade_hash,))
                        con.commit()
                    else:
                        print("Match already exists in DB. Probably old file")
                else:
                    print("Same match. Do nothing")
# move/delete file
                os.rename(full_file, os.path.join(PATH_TO_XML, "archive", file))
                print(f"File ${file} moved to archive")
    except sqlite3.Error as error:
        print('Error occured - ', error)
    finally:
        if con:
            con.close()
            print('SQLite Connection closed')

def is_team_invite(teams):
    teamNumber = findOwnTeamNo(teams)
    if teams[f"MissionBagTeam_${teamNumber}_isinvite"] == "true":
        return 1
    else:
        return 0
def get_team_size(teams):
    teamNumber = findOwnTeamNo(teams)
    return teams[f"MissionBagTeam_${teamNumber}_numplayers"]
def get_team_mmr(teams):
    teamNumber = findOwnTeamNo(teams)
    return teams[f"MissionBagTeam_${teamNumber}_mmr"]
def is_game_quickplay(bag_var):
    if bag_var["MissionBagIsQuickPlay"] == "true":
        return 1
    else:
        return 0
def is_game_skillbased_mm(players):
    playerNumber = findPlayerNo(players)
    return players[f"MissionBagPlayer_${playerNumber}_skillbased"]
def is_game_single_boss(boss):
    for value in boss.values():
        count = -1 # there is always a -1 boss true
        if value == "true":
            count = count+1
    if count == 1:
        return 1
    else:
        return 0
def get_boss(boss_number, is_single_boss):
    return "Assassin"
def has_player_extracted():
    return 1
def has_player_extracted_with_bounty():
    return 1
def has_player_picked_up_bounty():
    return 1
def get_player_mmr():
    return 1
def is_player_soul_survivor():
    return 1
def has_player_wellspring_activated(players):
    return 1
def get_player_downed_count(players):
    return 1
def get_player_kill_count_total():
    return 1
def get_player_kill_count_mmr1(bag_entries):
    return 1
def get_player_kill_count_mmr2(bag_entries):
    return 1
def get_player_kill_count_mmr3(bag_entries):
    return 1
def get_player_kill_count_mmr4(bag_entries):
    return 1
def get_player_kill_count_mmr5(bag_entries):
    return 1
def get_player_kill_count_mmr6(bag_entries):
    return 1
def get_player_assist_count(bag_entries):
    return 1
def get_player_collected_clues_count(bag_entries):
    return 1
def get_player_loot_upgrade_points_count(bag_entries):
    return 1
def get_player_loot_bloodline_count(bag_entries):
    return 1
def get_player_loot_hunt_dollar_count(bag_entries):
    return 1
def get_player_grunts_killed(bag_entries):
    return 1
def get_player_hives_killed(bag_entries):
    return 1
def get_player_armored_killed(bag_entries):
    return 1
def get_player_immolators_killed(bag_entries):
    return 1
def get_player_waterdevils_killed(bag_entries):
    return 1



def findOwnTeamNo(teams):
    for entry in teams:
        if entry[0].endswith("ownteam") and entry[1] == "true":
            return entry[0][15]
    else:
        raise ValueError
def findPlayerNo(players):
    for entry in players:
        if entry[0].endswith("blood_line_name") and entry[1] == PLAYER_NAME:
            return entry[0][17:19]
    else:
        raise ValueError

if __name__ == '__main__':
    start_ingestion()