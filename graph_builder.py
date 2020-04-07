import json
import networkx as nx
import os
import gzip
import time
import pickle
import numpy as np

def monitor_elapsed_time(func):
    def wrapper(*args, **kwargs):
        print('Starting', func.__name__)
        t_start = time.time()
        ret = func(*args, **kwargs)
        print('Completed in ', time.time()-t_start)
        return ret
    return wrapper



@monitor_elapsed_time
def read_rewards(graph):
    os.chdir('claim_reward_balance_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                reward = op['value']
                try:
                    graph.nodes[reward['account']][1]['rewards_steem'] += int(reward['reward_steem']['amount'])
                    graph.nodes[reward['account']][1]['rewards_sbd'] += int(reward['reward_sbd']['amount'])
                    graph.nodes[reward['account']]['rewards_vests'] += int(reward['reward_vests']['amount'])
                    graph.nodes[reward['account']][1]['last_reward'] = op['timestamp']
                    #print("New claimed reward for"+reward['acccount']+"  on: "+op['timestamp'])
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_post_comments(graph):
    os.chdir('comment_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())['value']
                if op['author'] == op['parent_author']:
                    try:
                        graph.nodes[op['author']]['posts'] += 1
                    except KeyError:
                        pass
                else:
                    try:
                        graph.nodes[op['author']]['comments'] += 1
                    except KeyError:
                        pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_votes(graph):
    os.chdir('vote_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                vote = op['value']
                try:
                    graph.nodes[vote['author']]['votes_received'] +=1
                    graph.nodes[vote['voter']]['votes_given'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_pow(graph):
    os.chdir('pow_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                work = op['value']
                try:
                    graph.nodes[work['worker_account']]['pow'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    os.chdir('pow2_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                work = op['value']['work']['value']['input']
                try:
                    graph.nodes[work['worker_account']]['pow'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph
@monitor_elapsed_time
def default(graph):
    for node in graph.nodes(data=True):
        #graph.nodes[node[0]]['comments'] = 0
        #graph.nodes[node[0]]['posts'] = 0
        graph.nodes[node[0]]['votes_given'] = 0
        graph.nodes[node[0]]['votes_received'] =0
    return graph



print('Loading graph')
graph = nx.read_gpickle('../steemit_on_nas/blockchain_graph.gpickle')
os.chdir('../steemit_on_nas/anonymized_data')
graph = default(graph)
#graph = read_post_comments(graph)
#graph = read_pow(graph)
#graph = read_rewards(graph)
graph = read_votes(graph)
os.chdir('..')
nx.write_gpickle(graph, 'blockchain_graph.gpickle')
