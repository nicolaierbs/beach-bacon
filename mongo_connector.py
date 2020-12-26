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

    for result in player['results']:
        db.team.update_one({'dvv_id': result['team_id']}, {'$addToSet': {'player': player['dvv_id']}}, True)


def retrieve_teams():
    return db.team.find({}, {'player': 1})


def player_name(dvv_id):
    player = db.player.find_one({'dvv_id': dvv_id}, {'first_name': 1, 'last_name': 1})
    return player['first_name'] + ' ' + player['last_name']
