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
