# TODO: calculate mmr kills from players_dict

from multiprocessing.sharedctypes import Value
from statistics import variance
import xml.etree.ElementTree as ET
import sqlite3
import os
from datetime import datetime
from hashlib import sha256
from result import Result
from player import Player
import traceback

PATH_TO_XML = "D:/Temp/HuntShowdownStats/"
PATH_TO_ARCHIVE = "D:/Temp/HuntShowdownStats/archive"
MMR = {"MMR1": 2000, "MMR2": 2300, "MMR3": 2600, "MMR4": 2750, "MMR5": 3000, "MMR6": 5000}

PLAYER_NAME = "CCarpo"

def start_ingestion():
# connect to database
    try:
        con = sqlite3.connect('huntstats.db')
        cursor = con.cursor()

        # variables_table = """ CREATE TABLE if not exists Variables (
        #     Key VARCHAR(30) NOT NULL primary key,
        #     Value VARCHAR(256) NOT NULL
        # ); """

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
            Player_Bounty_Extracted_Count BOOLEAN,
            Player_Is_Bounty_Picked_Up_Assassin BOOLEAN,
            Player_Is_Bounty_Picked_Up_Butcher BOOLEAN,
            Player_Is_Bounty_Picked_Up_Spider BOOLEAN,
            Player_Is_Bounty_Picked_Up_Scrapbeak BOOLEAN,
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
            Player_Collect_Assassin_Clues_Count INTEGER NOT NULL,
            Player_Collect_Butcher_Clues_Count INTEGER NOT NULL,
            Player_Collect_Spider_Clues_Count INTEGER NOT NULL,
            Player_Collect_Scrapbeak_Clues_Count INTEGER NOT NULL,
            Player_Loot_Upgrade_Points INTEGER NOT NULL,
            Player_Loot_Bloodline INTEGER NOT NULL,
            Player_Loot_Hunt_Dollar INTEGER NOT NULL,
            Player_Loot_Bloodbounds INTEGER NOT NULL,
            Player_Grunts_Killed INTEGER NOT NULL,
            Player_Hives_Killed INTEGER NOT NULL,
            Player_Armored_Killed INTEGER NOT NULL,
            Player_Immolators_Killed INTEGER NOT NULL,
            Player_Waterdevils_Killed INTEGER NOT NULL,
            Player_Meatheads_Killed INTEGER NOT NULL,
            Player_Leeches_Killed INTEGER NOT NULL,
            Player_Revive_Team_Mate INTEGER NOT NULL,
            Player_Banish_Assassin BOOLEAN,
            Player_Banish_Butcher BOOLEAN,
            Player_Banish_Spider BOOLEAN,
            Player_Banish_Scrapbeak BOOLEAN
        ); """

        # cursor.execute(variables_table)
        cursor.execute(all_matches_table)
        con.commit()

# read files
        files = os.listdir(PATH_TO_XML)
# for each file 
        for file in files:
            full_file = os.path.join(PATH_TO_XML, file)
            print(f"File: {full_file}")
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
                    elif child.attrib['name'].startswith("MissionBagTeam"):
                        teams[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagPlayer"):
                        players[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'].startswith("MissionBagEntry"):
                        bag_entries[child.attrib['name']] = child.attrib['value']
                    elif child.attrib['name'] == "MissionBagIsHunterDead" \
                        or child.attrib['name'] == "MissionBagIsQuickPlay" \
                        or child.attrib['name'] == "MissionBagNumAccolades" \
                        or child.attrib['name'] == "MissionBagNumEntries" \
                        or child.attrib['name'] == "MissionBagNumTeams":
                        bag_var[child.attrib['name']] = child.attrib['value']
                player_dict = generate_player_dict(players, bag_var, teams)
                mmr_kill_count_dict = get_player_kill_count(player_dict)
                # print(f'No Accolade Entries: {len(accolades)}')
                # print(f'No Boss Entries:     {len(boss)}')
                # print(f'No Team Entries:     {len(teams)}')
                # print(f'No Player Entries:   {len(players)}')
                # print(f'No Bag Entries:      {len(bag_entries)}')
                # print(f'No Var Entries:      {len(bag_entries)}')

# check if the file contains new accolades so this would be a new match and not just some other changes in the attributes.xml
                same_match = True
                select_all_hashes = """ SELECT AccoladeHash FROM Matches;"""
                cursor.execute(select_all_hashes)
                accolade_hashes = cursor.fetchall()
                current_accolade_hash = sha256(str(accolades).encode()).hexdigest()
                print(f'Current Hash: {current_accolade_hash}')
                if current_accolade_hash not in str(accolade_hashes):
                    print("Hash not found in database")
                    current_datetime = datetime.strptime(file, "attributes_%Y-%b-%d_%H-%M.xml")
                    formatted_current_datetime = datetime.strftime(current_datetime, "%Y-%m-%d %H:%M")
                    result = Result(current_accolade_hash, formatted_current_datetime)
                    result = parse(bag_entries, result, bag_var)

# check if match already in db through timestamp
                    # cursor.execute(""" SELECT Timestamp FROM Matches;""")
                    # print(f'Timestamp: {formatted_current_datetime}')
                    # all_timestamps = cursor.fetchall()
                    # #print(all_timestamps)
                    # print(f'Timestamp found NOT in DB: {formatted_current_datetime not in str(all_timestamps)}')
                    # if formatted_current_datetime not in all_timestamps:
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
                        get_boss(boss, 1, is_game_single_boss(boss), is_game_quickplay(bag_var)),
                        get_boss(boss, 2, is_game_single_boss(boss), is_game_quickplay(bag_var)),
                        result.Player_Is_Extracted,
                        result.Player_Extracted_Tokens,
                        result.Player_Pickup_Assassin_Token,
                        result.Player_Pickup_Butcher_Token,
                        result.Player_Pickup_Spider_Token,
                        result.Player_Pickup_Scrapbeak_Token,
                        get_player_mmr(players),
                        is_player_soul_survivor(players, bag_var),
                        has_player_wellspring_activated(players, bag_var),
                        get_player_downed_count(player_dict),
                        get_player_kill_count_total(player_dict),
                        mmr_kill_count_dict["MMR1"],
                        mmr_kill_count_dict["MMR2"],
                        mmr_kill_count_dict["MMR3"],
                        mmr_kill_count_dict["MMR4"],
                        mmr_kill_count_dict["MMR5"],
                        mmr_kill_count_dict["MMR6"],
                        result.Player_Kill_Assists,
                        result.Player_Assassin_Clues,
                        result.Player_Butcher_Clues,
                        result.Player_Spider_Clues,
                        result.Player_Scrapbeak_Clues,
                        result.Player_Loot_Upgrade_Points,
                        result.Player_Loot_Bloodline,
                        result.Player_Loot_Hunt_Dollar,
                        result.Player_Loot_Bloodbounds,
                        result.Player_Grunts_Killed,
                        result.Player_Hives_Killed,
                        result.Player_Armored_Killed,
                        result.Player_Immolators_Killed,
                        result.Player_Waterdevils_Killed,
                        result.Player_Meatheads_Killed,
                        result.Player_Leeches_Killed,
                        result.Player_Revive_Team_Mate,
                        result.Player_Banish_Assassin,
                        result.Player_Banish_Butcher,
                        result.Player_Banish_Spider,
                        result.Player_Banish_Scrapbeak,
                        ))
                    con.commit()
                       
# replace last match
                    # cursor.execute("INSERT INTO Variables('Key','Value') VALUES ('LastAccoladeHash', ?) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;", (current_accolade_hash,))
                    # con.commit()
                    # else:
                    #     print("Timstamp exists in DB. Probably old file.")
                else:
                    print("Hash found in Database. Do nothing")
# move/delete file
                os.rename(full_file, os.path.join(PATH_TO_XML, "archive", file))
                print(f"File {file} moved to archive")
    except sqlite3.Error as error:
        traceback.print_exc()
    finally:
        if con:
            con.close()
            print('SQLite Connection closed')

def is_team_invite(teams):
    teamNumber = findOwnTeamNo(teams)
    if teams[f"MissionBagTeam_{teamNumber}_isinvite"] == "true":
        return 1
    else:
        return 0
def get_team_size(teams):
    teamNumber = findOwnTeamNo(teams)
    return teams[f"MissionBagTeam_{teamNumber}_numplayers"]
def get_team_mmr(teams):
    teamNumber = findOwnTeamNo(teams)
    return teams[f"MissionBagTeam_{teamNumber}_mmr"]
def is_game_quickplay(bag_var):
    if bag_var["MissionBagIsQuickPlay"] == "true":
        return 1
    else:
        return 0
def is_game_skillbased_mm(players):
    playerNumber = findPlayerNo(players)
    if players[f"MissionBagPlayer_{playerNumber}_skillbased"] == "true":
        return 1
    else: 
        return 0
def is_game_single_boss(boss):
    count = -1 # there is always a -1 boss true
    for value in boss.values():
        if value == "true":
            count = count+1
    if count == 1:
        return 1
    else:
        return 0
def get_boss(boss, boss_number, is_single_boss, is_quickplay):
    bosses = []
    if (is_single_boss and boss_number == 2) or is_quickplay:
        return ""
    if boss["MissionBagBoss_0"] == "true":
        bosses.append("Butcher")
    if boss["MissionBagBoss_1"] == "true":
        bosses.append("Assassin")
    if boss["MissionBagBoss_2"] == "true":
        bosses.append("Spider")
    if boss["MissionBagBoss_3"] == "true":
        bosses.append("Scrapbeak")
    if boss_number == 1:
        return bosses[0]
    else:
        return bosses[1]
def get_player_mmr(players):
    playerNumber = findPlayerNo(players)
    return players[f"MissionBagPlayer_{playerNumber}_mmr"]
def is_player_soul_survivor(players, bag_var):
    playerNumber = findPlayerNo(players)
    if players[f"MissionBagPlayer_{playerNumber}_issoulsurvivor"] == "true":
        return 1
    else:
        return 0
def has_player_wellspring_activated(players, bag_var):
    playerNumber = findPlayerNo(players)
    if players[f"MissionBagPlayer_{playerNumber}_hadWellspring"] == "true":
        return 1
    else:
        return 0


def generate_player_dict(players, bag_var, teams):
    player_dict = {}
    for item in players:
        if int(item.split("_")[1]) < int(bag_var["MissionBagNumTeams"]) and item.endswith("blood_line_name"):
            nameparts = item.split("_")
            teamnumber = nameparts[1]
            playernumber = nameparts[2]
            if int(playernumber) < int(teams[f"MissionBagTeam_{teamnumber}_numplayers"]):
                print(f"New player found: {players[item]}")
                player = Player(players[f'MissionBagPlayer_{teamnumber}_{playernumber}_profileid'],players[item])
                try:
                    player.bountyextracted = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_bountyextracted']
                except KeyError:
                    pass
                try:
                    player.bountypickedup = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_bountypickedup']
                except KeyError:
                    pass
                try:
                    player.downedbyme = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_downedbyme']
                except KeyError:
                    pass
                try:
                    player.downedbyteammate = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_downedbyteammate']
                except KeyError:
                    pass
                try:
                    player.downedme = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_downedme']
                except KeyError:
                    pass
                try:
                    player.downedteammate = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_downedteammate']
                except KeyError:
                    pass
                try:
                    player.hadWellspring = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_hadWellspring']
                except KeyError:
                    pass
                try:
                    player.hadbounty = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_hadbounty']
                except KeyError:
                    pass
                try:
                    player.ispartner = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_ispartner']
                except KeyError:
                    pass
                try:
                    player.issoulsurvivor = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_issoulsurvivor']
                except KeyError:
                    pass
                try:
                    player.killedbyme = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_killedbyme']
                except KeyError:
                    pass
                try:
                    player.killedbyteammate = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_killedbyteammate']
                except KeyError:
                    pass
                try:
                    player.killedme = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_killedme']
                except KeyError:
                    pass
                try:
                    player.killedteammate = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_killedteammate']
                except KeyError:
                    pass
                try:
                    player.mmr = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_mmr']
                except KeyError:
                    pass
                try:
                    player.proximity = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_proximity']
                except KeyError:
                    pass
                try:
                    player.proximitytome = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_proximitytome']
                except KeyError:
                    pass
                try:
                    player.proximitytoteammate = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_proximitytoteammate']
                except KeyError:
                    pass
                try:
                    player.skillbased = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_skillbased']
                except KeyError:
                    pass
                try:
                    player.teamextraction = players[f'MissionBagPlayer_{teamnumber}_{playernumber}_teamextraction']
                except KeyError:
                    pass
                player_dict[player.blood_line_name] = player
    return player_dict

def get_player_downed_count(player_dict):
    count = 0
    for player in player_dict:
        if player_dict[player].blood_line_name != PLAYER_NAME:
            count += int(player_dict[player].downedme)
            count += int(player_dict[player].killedme)
    return count


def get_player_kill_count_total(player_dict):
    count = 0
    for player in player_dict:
        if player_dict[player].blood_line_name != PLAYER_NAME:
            count += int(player_dict[player].downedbyme)
            count += int(player_dict[player].killedbyme)
    return count

def parse(bag_entries, result, bag_var):
    for item in bag_entries.items():
        entrynumber = item[0].split("_")[1]
        if int(entrynumber) < int(bag_var["MissionBagNumEntries"]):
            #print("Parse Entry")
            if item[0].endswith("_descriptorName"):
                if item[1] == "banish assassin":
                    result.Player_Banish_Assassin = 1
                if item[1] == "banish butcher":
                    result.Player_Banish_Butcher = 1
                if item[1] == "banish spider":
                    result.Player_Banish_Spider = 1
                if item[1] == "banish scrapbeak":
                    result.Player_Banish_Scrapbeak = 1
                if item[1] == "clue assassin 1st":
                    result.Player_Assassin_Clues += 1
                if item[1] == "clue assassin 2nd":
                    result.Player_Assassin_Clues += 1
                if item[1] == "clue assassin 3rd":
                    result.Player_Assassin_Clues += 1
                if item[1] == "clue butcher 1st":
                    result.Player_Butcher_Clues += 1
                if item[1] == "clue butcher 2nd":
                    result.Player_Butcher_Clues += 1
                if item[1] == "clue butcher 3rd":
                    result.Player_Butcher_Clues += 1
                if item[1] == "clue scrapbeak 1st":
                    result.Player_Scrapbeak_Clues += 1
                if item[1] == "clue scrapbeak 2nd":
                    result.Player_Scrapbeak_Clues += 1
                if item[1] == "clue scrapbeak 3rd":
                    result.Player_Scrapbeak_Clues += 1
                if item[1] == "clue spider 1st":
                    result.Player_Spider_Clues += 1
                if item[1] == "clue spider 2nd":
                    result.Player_Spider_Clues += 1
                if item[1] == "clue spider 3rd":
                    result.Player_Spider_Clues += 1
                if item[1] == "extract four tokens":
                    result.Player_Extracted_Tokens = 4
                if item[1] == "extract three tokens":
                    result.Player_Extracted_Tokens = 3
                if item[1] == "extract two tokens":
                    result.Player_Extracted_Tokens = 2
                if item[1] == "extraction solo":
                    result.Player_Extracted_Tokens = 1
                if item[1] == "hunter points":
                    result.Player_Loot_Upgrade_Points = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "kill armored":
                    result.Player_Armored_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill assassin":
                    result.Player_Killed_Assassin = 1
                if item[1] == "kill butcher":
                    result.Player_Killed_Butcher = 1
                if item[1] == "kill grunt":
                    result.Player_Grunts_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill hellhound":
                    result.Player_Hellhounds_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill hive":
                    result.Player_Hives_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill immolator":
                    result.Player_Immolators_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill leeches":
                    result.Player_Leeches_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill meathead":
                    result.Player_Meatheads_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player assist":
                    result.Player_Kill_Assists = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 1 stars":
                    result.Player_Kill_Count_Mmr1 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 2 stars":
                    result.Player_Kill_Count_Mmr2 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 3 stars":
                    result.Player_Kill_Count_Mmr3 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 4 stars":
                    result.Player_Kill_Count_Mmr4 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 5 stars":
                    result.Player_Kill_Count_Mmr5 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill player mm rating 6 stars":
                    result.Player_Kill_Count_Mmr6 = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "kill scrapbeak event":
                    result.Player_Killed_Scrapbeak = 1
                if item[1] == "kill spider":
                    result.Player_Killed_Spider = 1
                if item[1] == "kill waterdevil":
                    result.Player_Waterdevils_Killed = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "locate assassin":
                    result.Player_Locate_Assassin = 1
                if item[1] == "located butcher":
                    result.Player_Locate_Butcher = 1
                if item[1] == "located spider":
                    result.Player_Locate_Spider = 1
                if item[1] == "located scrapbeak":
                    result.Player_Locate_Scrapbeak = 1
                if item[1] == "loot assassin token":
                    result.Player_Loot_Assassin_Token = 1
                if item[1] == "loot bloodline xp":
                    result.Player_Loot_Bloodline = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "loot butcher token":
                    result.Player_Loot_Butcher_Token = 1
                if item[1] == "loot gems":
                    result.Player_Loot_Bloodbounds = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "loot gold":
                    result.Player_Loot_Hunt_Dollar = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "loot hunter xp":
                    result.Player_Loot_Hunter_XP = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "loot scrapbeak token":
                    result.Player_Loot_Scrapbeak_Token = 1
                if item[1] == "loot spider token":
                    result.Player_Loot_Spider_Token = 1
                if item[1] == "loot upgrade points":
                    result.Player_Loot_Upgrade_Points = bag_entries[f"MissionBagEntry_{entrynumber}_rewardSize"]
                if item[1] == "pickup assassin token":
                    result.Player_Pickup_Assassin_Token = 1
                if item[1] == "pickup butcher token":
                    result.Player_Pickup_Butcher_Token = 1
                if item[1] == "pickup scrapbeak token":
                    result.Player_Pickup_Scrapbeak_Token = 1
                if item[1] == "pickup spider token":
                    result.Player_Pickup_Spider_Token = 1
                if item[1] == "revive team mate":
                    result.Player_Revive_Team_Mate = bag_entries[f"MissionBagEntry_{entrynumber}_amount"]
                if item[1] == "successful extraction" and bag_var["MissionBagIsHunterDead"] != "true":
                    result.Player_Is_Extracted = 1
    return result

def get_player_kill_count(player_dict):
    mmr_kill_count = { "MMR1": 0, "MMR2": 0, "MMR3": 0, "MMR4": 0, "MMR5": 0, "MMR6": 0}

    for player in player_dict:
        if player_dict[player].blood_line_name != PLAYER_NAME:
            for i in range(1,6):
                if int(player_dict[player].mmr) > MMR[f"MMR{i}"]:
                    continue
                else:
                    mmr_kill_count[f"MMR{i}"] += int(player_dict[player].downedbyme)
                    mmr_kill_count[f"MMR{i}"] += int(player_dict[player].killedbyme)
                    break
    return mmr_kill_count

def findOwnTeamNo(teams):
    for entry in teams:
        if entry.endswith("ownteam") and teams[entry] == "true":
            #print(f"My Team No: {entry[15]}")
            return entry[15]
    else:
        raise ValueError
def findPlayerNo(players):
    for entry in players:
        if entry.endswith("blood_line_name") and players[entry] == PLAYER_NAME:
            nameparts = entry.split("_")
            return nameparts[1]+"_"+nameparts[2]
    else:
        raise ValueError


if __name__ == '__main__':
    start_ingestion()