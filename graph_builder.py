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
                print(graph.nodes[reward['account']])
                try:
                    #graph.nodes[reward['account']]['rewards_steem'] += int(reward['reward_steem']['amount'])
                    #graph.nodes[reward['account']]['rewards_sbd'] += int(reward['reward_sbd']['amount'])
                    #graph.nodes[reward['account']]['rewards_vests'] += int(reward['reward_vest']['amount'])
                    graph.nodes[reward['account']][1]['last_reward'] = op['timestamp']
                    print("New claimed reward for"+reward['acccount']+"  on: "+op['timestamp'])
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_comments(graph):
    os.chdir('comment_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                comment = op['value']
                try:
                    graph.nodes[comment['author']]['comments'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_posts(graph):
    os.chdir('feed_publish_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                
                info_post = op['value']
                try:
                    graph.nodes[info_post['publisher']]['posts'] += 1
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
                    graph.nodes[vote['voter']]['votes'] += 1
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


graph = nx.read_gpickle("../steemit_on_nas/blockchain_graph.gpickle")
os.chdir('../steemit_on_nas/anonymized_data')
#graph = read_comments(graph)
#graph = read_posts(graph)
#graph = read_pow(graph)
graph = read_rewards(graph)
#graph = read_votes(graph)
os.chdir('..')
nx.write_gpickle(graph, 'blockchain_graph.gpickle')
