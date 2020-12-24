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


def store_player(player):
    db.player.insert_one(player)


def store_team(team):
    db.team.insert_one(team)


def store_tournament(tournament):
    db.tournament.insert_one(tournament)
