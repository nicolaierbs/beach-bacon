import pandas as pd
import mongo_connector

df = pd.read_pickle('shortest_paths.pkl')
df = df[df.count_nan < 3000]
df = df.sort_values('mean_path')
df['name'] = df['player'].apply(lambda x: mongo_connector.player_name(int(x)))
df = df.drop(['player', 'player_count'], axis=1)
df = df.reset_index()
print(len(df.index))

print(df.head(50))

print(df[df.name == 'Nicolai Erbs'])
print(df[df.name == 'Burkhard Sude'])
