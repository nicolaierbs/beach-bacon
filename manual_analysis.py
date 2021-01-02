import pandas as pd
import mongo_connector

# df = pd.read_pickle('men_paths.pkl')
# df = df[df.count_nan <= 1791]

# df = pd.read_pickle('women_paths.pkl')
# df = df[df.count_nan <= 1690]

df = pd.read_pickle('mixed_aths.pkl')
df = df[df.count_nan <= 813]

# df = df.sort_values('count_nan', ascending=True)
# print(df.head(50))

df = df.sort_values('mean_path')
df['name'] = df['player'].apply(lambda x: mongo_connector.player_name(int(x)))
df['tournaments'] = df['player'].apply(lambda x: mongo_connector.tournament_count(int(x)))
df['partners'] = df['player'].apply(lambda x: mongo_connector.partner_count(int(x)))
df = df.drop(['player', 'player_count'], axis=1)
df = df.reset_index()
print(len(df.index))

count = 1
for row in df.iterrows():

    # print(row)
    # print(str(row[1]['name']))
    print(
        '<tr><td>' + str(count)
        + '</td><td>' + str(row[1]['name'])
        + '</td><td>' + str(row[1]['partners'])
        + '</td><td>' + str(row[1]['tournaments'])
        # + '</td><td>' + str(round(row[1]['longest_path']))
        + '</td><td>' + str(round(row[1]['mean_path'], 3))
        + '</td></tr>')
    if count > 20:
        break
    count += 1

# print(df.head(50))


print('Longest path: ' + str(max(df.longest_path)))
