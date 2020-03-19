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
def read_nodes(graph):
    os.chdir('account_create_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                try:
                    graph.add_node(op['value']['new_account_name'],
                                    creation_date = op['timestamp'])
                except KeyError:
                    pass
                                
    os.chdir('../account_create_with_delegation_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                try:
                    graph.add_node(op['value']['new_account_name'],
                                    creation_date = op['timestamp'])
                except KeyError:
                    pass

    os.chdir('../create_claimed_account_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                try:
                    graph.add_node(op['value']['new_account_name'],
                                    creation_date = op['timestamp'])
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def read_links(graph):
    os.chdir('custom_json_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                
                if op['value']['id'] == 'follow':
                    try:
                        cj = json.loads(op['value']['json'])
                    except TypeError:   
                        cj = op['value']['json']
                    try:
                        graph.add_edge(cj['follower'],cj['following'], timestamp = op['timestamp'])
                    except KeyError:
                        pass
                    except TypeError:
                        try:
                            c_json = json.loads(cj[1])
                            graph.add_edge(c_json['follower'],c_json['following'], timestamp = op['timestamp'])
                        except:
                            pass
                else:
                    continue
    os.chdir('..')
    return graph

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
                    graph[reward['account']]['rewards_steem'] += reward['reward_steem']['amount']
                    graph[reward['account']]['rewards_sbd'] += reward['reward_sbd']['amount']
                    graph[reward['account']]['rewards_vests'] += reward['reward_vest']['amount']
                    graph[reward['account']]['last_reward'] = op['timestamp']
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
                    graph[comment['author']]['comments'] += 1
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
                    graph[info_post['publisher']]['posts'] += 1
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
                    graph[vote['voter']]['votes'] += 1
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
                    graph[work['worker_account']]['pow'] += 1
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
                    graph[work['worker_account']]['pow'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph

@monitor_elapsed_time
def set_attributes(graph):
    for node in graph.nodes():
        graph[node]['comments'] = 0
        graph[node]['votes'] = 0
        graph[node]['posts'] = 0
        graph[node]['last_reward'] = ''
        graph[node]['rewards_steem'] = 0
        graph[node]['rewards_vests'] = 0
        graph[node]['rewards_sbd'] = 0
        graph[node]['pow'] = 0
    return graph

graph = nx.DiGraph()
os.chdir('../steemit_on_nas/anonymized_data')
graph = read_nodes(graph)
graph = read_links(graph)
graph = set_attributes(graph)
graph = read_comments(graph)
graph = read_posts(graph)
graph = read_pow(graph)
graph = read_rewards(graph)
graph = read_votes(graph)
os.chdir('..')
nx.write_gpickle(graph, 'blockchain_graph.gpickle')
