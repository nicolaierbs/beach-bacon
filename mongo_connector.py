import pymongo
from collections import defaultdict
import re
from datetime import datetime
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

server_name = config.get('MONGODB', 'server')
database_name = config.get('MONGODB', 'database')

client = pymongo.MongoClient("mongodb://" + server_name + ":27017/")
db = client[database_name]


def ingest_player(player):
    db.player.replace_one({'dvv_id': player['dvv_id']}, player, True)


def ingest_tournament(tournament):
    db.tournament.replace_one({'dvv_id': tournament['dvv_id']}, tournament, True)


def retrieve_teams(men=True, women=True, mixed=True):
    gender = list()
    if men:
        gender.append('Männer')
    if women:
        gender.append('Frauen')
    if mixed:
        gender.append('Mixed')
    return db.team.find({'gender': {'$in': gender}}, {'player': 1})


def player_name(dvv_id):
    player = db.player.find_one({'dvv_id': dvv_id}, {'first_name': 1, 'last_name': 1})
    return player['first_name'] + ' ' + player['last_name']


def tournament_count(dvv_id):
    player = db.player.find_one({'dvv_id': dvv_id}, {'results': 1})
    return len(player['results'])


def partner_count(dvv_id):
    player = db.player.find_one({'dvv_id': dvv_id}, {'results': 1})
    teams = set()
    for result in player['results']:
        teams.add(result['team_id'])
    return len(teams)



def create_teams():
    for player in list(db.player.find({'results': {'$exists': True, '$ne': []}}, {'results': 1, 'dvv_id': 1})):
        for result in player['results']:
            tournament_id = db.tournament.find_one({'dvv_id': result['tournament_id']})
            if tournament_id:
                gender = db.tournament.find_one({'dvv_id': result['tournament_id']})['gender']
                db.team.update_one({'dvv_id': result['team_id']},
                                   {'$addToSet': {'player': player['dvv_id'], 'gender': gender}},
                                   True)
            else:
                print(str(result['tournament_id']) + ' not found')
