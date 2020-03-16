import json
import networkx as nx
import os
import gzip
 
def read_nodes(graph):
    os.chdir('account_create_operation')
    n_file = os.listdir(".")
    print('read_nodes')
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                print(op)
                try:
                    graph.add_node(op['value']['new_account_name'],
                                    creation_date = op['timestamp'],
                                    rewards_steem= 0,
                                    rewards_sbd=0,
                                    rewards_vests=0, 
                                    last_reward = '',
                                    posts=0, 
                                    votes=0, 
                                    resteems=0, 
                                    comments=0, 
                                    pow=0 )
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
                                    creation_date = op['timestamp'],
                                    rewards_steem= 0,
                                    rewards_sbd=0,
                                    rewards_vests=0, 
                                    last_reward = '',
                                    posts=0, 
                                    votes=0, 
                                    resteems=0, 
                                    comments=0, 
                                    pow=0 )
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
                                    creation_date = op['timestamp'],
                                    rewards_steem= 0,
                                    rewards_sbd=0,
                                    rewards_vests=0, 
                                    last_reward = '',
                                    posts=0, 
                                    votes=0, 
                                    resteems=0, 
                                    comments=0, 
                                    pow=0 )
                except KeyError:
                    pass
    os.chdir('..')
    return graph

def read_links(graph):
    print('read_links')
    os.chdir('custom_json_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                print(op)
                cj = op['value']['json']
                if op['value']['id'] == 'follow':
                    try:
                        graph.add_edge(cj['follower'],cj['following'], timestamp = op['timestamp'])
                    except KeyError:
                        pass
                else:
                    continue
    os.chdir('..')
    return graph



def read_rewards(graph):
    print('read_rewards')
    os.chdir('claim_reward_balance_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                print(op)
                reward = op['value']
                try:
                    graph[reward['account']]['rewards_steem'] += reward['reward_steem']['amount']
                    graph[reward['account']]['rewards_sbd'] += reward['reward_sbd']['amount']
                    graph[reward['account']]['rewards_vests'] += reward['reward_vest']['amount']
                    graph[reward['account']]['last_reward'] += op['timestamp']
                except KeyError:
                    pass
    os.chdir('..')
    return graph

def read_comments(graph):
    print('read_comments')
    os.chdir('comment_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                print(op)
                comment = op['value']
                try:
                    graph[comment['author']]['comments'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph

def read_posts(graph):
    print('read_posts')
    os.chdir('feed_publish_operation')
    n_file = os.listdir(".")
    for gz in n_file:
        with gzip.open(gz,'rb') as f:
            for line in f:
                op = json.loads(line.decode())
                print(op)
                info_post = op['value']
                try:
                    graph[info_post['publisher']]['posts'] += 1
                except KeyError:
                    pass
    os.chdir('..')
    return graph

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

graph = nx.DiGraph()
os.chdir('../steemit_on_nas/anonymized_data')
graph = read_nodes(graph)
graph = read_links(graph)
graph = read_comments(graph)
graph = read_posts(graph)
graph = read_pow(graph)
graph = read_rewards(graph)
graph = read_votes(graph)
os.chdir('../../steemit_blockchain_graph')
nx.write_gexf(graph, 'blockchain_graph.gexf')
