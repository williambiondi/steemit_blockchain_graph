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



def check_node(graph, node):
    msg = ''
    if node in graph.nodes:
        msg = str(node)+' in the graph'
    else:
        msg = str(node)+' NOT in the graph'
    return msg

@monitor_elapsed_time
def read_rewards(graph):
    os.chdir('claim_reward_balance_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                reward = op['value']
                print(check_node(graph,reward['account']))
                try:
                    graph.nodes[reward['account']]['rewards_steem'] += reward['reward_steem']['amount']
                    graph.nodes[reward['account']]['rewards_sbd'] += reward['reward_sbd']['amount']
                    graph.nodes[reward['account']]['rewards_vests'] += reward['reward_vest']['amount']
                    graph.nodes[reward['account']]['last_reward'] = op['timestamp']
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
                print(check_node(graph,comment['author']))
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
                print(check_node(graph,info_post['publisher']))
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
                print(check_node(graph,vote['voter']))
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
                print(check_node(graph,work['worker_account']))
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


graph = nx.DiGraph()
os.chdir('../steemit_on_nas/anonymized_data')
graph = read_nodes(graph)
graph = read_links(graph)
graph = read_comments(graph)
graph = read_posts(graph)
graph = read_pow(graph)
graph = read_rewards(graph)
graph = read_votes(graph)
os.chdir('..')
nx.write_gpickle(graph, 'blockchain_graph.gpickle')
