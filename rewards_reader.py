import json
import networkx as nx
import os
import gzip

os.chdir('claim_rewards_operation')
n_file = os.listdir(".")
graph = nx.read_gexf('blockchain_graph.gexf')
for gz in n_file:
    with gzip.open(gz,'rb') as f:
        for line in f:
            op = json.loads(line.decode())
            cj = op['value']['json']
            if op['value']['id'] == 'follow':
                graph.add_edge(cj['follower'],cj['following'], timestamp = op['timestamp'])
            else:
                continue
nx.write_gexf(graph,'blockchain_graph.gexf')