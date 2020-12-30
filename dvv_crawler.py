import requests
import re
from datetime import datetime
import mongo_connector

player_first_name_pattern = re.compile(r'Vorname:</td><td valign=top>([^<]*)</td>')
player_last_name_pattern = re.compile(r'Name:</td><td valign=top>([^<]*)</td>')
club_last_name_pattern = re.compile(r'Verein:</td><td valign=top>([^<]*)</td>')
dvv_id_pattern = re.compile(r'DVV-Lizenznummer:</td><td valign=top>(\d*)</td>')

player_result_pattern = re.compile(r'<tr><td rowspan=1 valign=top>(\d{2}.\d{2}.\d{4})</tD>'  # date
                                   r'<td rowspan=1 nowrap valign=top><a href=team.php\?id=(\d*)'  # team id
                                   r'>([^<]*)</a></td>'  # team name
                                   r'<td rowspan=1 valign=top><a href=tur-er.php\?id=(\d*)>'  # tournament id
                                   r'([^<]*)</a></td>'  # tournament name
                                   r'<td rowspan=1 valign=top><a href=tur-er.php\?id=(\d*)>'  # tournament id
                                   r'([^<]*)</a></td>'  # location
                                   r'<td rowspan=1 valign=top  align="right">(\d+)</tD>'  # place
                                   r'(<td nowrap valign=top  align="right">(\d+)</td>'  # points
                                   r'<td>([^<]*)</tD></tr>)?'  # Point type
                                   )

tornament_gender_pattern = re.compile(r'<td class="bez" valign="top">Geschlecht:</td><td valign=top>([^<]*)</td>')


def get_player_results(source):
    results = list()
    for match in player_result_pattern.finditer(source):
        result = dict()
        result['date'] = datetime.strptime(match.group(1), '%d.%m.%Y')
        result['team_id'] = int(match.group(2))
        result['team_name'] = match.group(3)
        result['tournament_id'] = int(match.group(4))
        result['tournament_type'] = match.group(5)
        result['tournament_location'] = match.group(7)
        result['rank'] = int(match.group(8))
        if match.group(9):
            result['points'] = int(match.group(10))
            result['points_type'] = match.group(11)
        # print(match.groups())
        results.append(result)

    return results


def get_tournament(web_id):
    url = 'https://beach.volleyball-verband.de/public/tur-show.php?id=' + str(web_id)
    source = requests.get(url).text

    tournament_details = dict()
    tournament_details['dvv_id'] = web_id
    tournament_details['gender'] = tornament_gender_pattern.search(source).group(1)

    return tournament_details


def get_player(web_id):
    url = 'https://beach.volleyball-verband.de/public/spieler.php?id=' + str(web_id)
    source = requests.get(url).text

    player_details = dict()
    # player_details['id'] = web_id
    player_details['first_name'] = player_first_name_pattern.search(source).group(1)
    player_details['last_name'] = player_last_name_pattern.search(source).group(1)
    player_details['club'] = club_last_name_pattern.search(source).group(1)
    player_details['dvv_id'] = int(dvv_id_pattern.search(source).group(1))
    player_details['results'] = get_player_results(source)

    return player_details


def crawl_player(min_id, max_id):
    for i in range(min_id, max_id):

        try:
            player = get_player(i)
            mongo_connector.ingest_player(player)
        except ValueError as e:
            print(str(i) + ': ' + str(e))


def crawl_tournaments(min_id, max_id):
    for i in range(min_id, max_id):

        try:
            tournament = get_tournament(i)
            mongo_connector.ingest_tournament(tournament)
        except ValueError as e:
            print(str(i) + ': ' + str(e))
        except AttributeError as e:
            print(str(i) + ': ' + str(e))


# crawl_player(59525, 62650)
crawl_tournaments(5073, 5076)
