import np as np
from dijkstar import Graph, find_path
from dijkstar.algorithm import NoPathError
import mongo_connector
import numpy as np
import pandas as pd
from tqdm import tqdm

file_name = 'paths.pkl'
graph_name = 'mixed_graph.graph'


def compute_connectors(graph):
    nodes = list(graph.get_data().keys())

    column_names = ['player', 'longest_path', 'mean_path', 'count_nan', 'player_count']
    df = pd.DataFrame(columns=column_names)
    for node_one in tqdm(nodes):
        paths = np.empty([0], dtype=int)
        for node_two in nodes:
            try:
                paths = np.append(paths, find_path(graph, node_one, node_two).total_cost)
            except NoPathError as e:
                paths = np.append(paths, np.nan)

        df = df.append({'player': node_one,
                        'longest_path': np.nanmax(paths),
                        'mean_path': np.nanmean(paths),
                        'count_nan': np.count_nonzero(np.isnan(paths)),
                        'player_count': len(paths)}, ignore_index=True)
        df.to_pickle(file_name)

    return df


def build_graph():
    graph = Graph(undirected=True)
    for team in mongo_connector.retrieve_teams(men=False, women=False, mixed=True):
        if len(team['player']) == 2:
            graph.add_edge(team['player'][0], team['player'][1], 1)
    print('Built graph with ' + str(len(graph.get_data().keys())) + ' nodes')
    return graph


team_graph = build_graph()
team_graph.dump(graph_name)
print('exported graph')
team_graph = Graph.load(graph_name)

shortest_paths = compute_connectors(team_graph)
shortest_paths.to_pickle(file_name)
print(shortest_paths.describe())
