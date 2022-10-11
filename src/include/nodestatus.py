#!/usr/bin/env python3
import traceback
import argparse
import os
import requests
import pprint
import random
import json
import datetime
import sys
from tenacity import retry, stop_after_attempt, wait_fixed
from urllib.parse import urljoin, urlparse
from .checker import Checker

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser()
parser.add_argument("-v",
                    '--verbose',
                    action="store_true",
                    dest="verbose",
                    help='Print logged info to screen')
parser.add_argument("-d",
                    '--debug',
                    action="store_true",
                    dest="debug",
                    help='Print debug info')
parser.add_argument('-l',
                    '--log_file',
                    default='{}.log'.format(
                        os.path.basename(__file__).split('.')[0]),
                    help='Log file')

args = parser.parse_args()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_producers(chain):
    try:
        LIMIT = 200
        url = f'{chain["api_node"]}/v1/chain/get_table_rows'
        payload = {
            "json": True,
            "code": "eosio",
            "scope": "eosio",
            "table": "producers",
            "lower_bound": None,
            "upper_bound": None,
            "index_position": 1,
            "key_type": "",
            "limit": LIMIT,
            "reverse": False,
            "show_payer": False
        }
        result = requests.post(url, json= payload, timeout=chain["timeout"]).json()
        

        isFIO = chain[
            'chain_id'] == '21dcae42c0182200e93f954a074011f9048a7624c6fe81d3c9541a614a88bd1c' or chain[
                'chain_id'] == 'b20901380af44ef59c5918439a1f9a41d83669020319a80574b804a5f95cbd7e'

        if isFIO:
            producers = result['producers']
        else:
            producers = result['rows']

        while result['more']:
            payload["lower_bound"] = result['next_key']
            result = requests.post(url, json= payload, timeout=chain["timeout"]).json()
            producers += result['rows']  

        active_producers = []
        for producer in producers:
            if producer['is_active'] != 0:
                
                if not producer['url'].startswith('http'):
                    producer['url'] = 'http://' + producer['url']
                p = {
                    'owner': producer['owner'],
                    'url': producer['url'],
                    'bp_json_url': urljoin(producer['url'], 'bp.json'),
                    'chains_json_url': urljoin(producer['url'], '/chains.json')
                }
                if isFIO:
                    p['fio_address'] = producer['fio_address']
                active_producers.append(p)

        if chain[
                'chain_id'] == '1064487b3cd1a897ce03ae5b6a865651747e2e152090f99c1d19d44e01aea5a4':
            active_producers = [
                x for x in active_producers
                if not (x['url'] == 'https://wax.io'
                        and x['owner'].endswith('.wax'))
            ]

        if chain['limit']:
            active_producers = active_producers[:chain['limit']]

        for num, _ in enumerate(active_producers):
            active_producers[num]['position'] = num + 1
            if num < 21:
                active_producers[num]['top21'] = True
            else:
                active_producers[num]['top21'] = False

        return active_producers

    except Exception as e:
        print('Error getting producers: {}'.format(e))
        traceback.print_exc(file=sys.stdout)
        raise


def run_nodestatus(config, pub_path):
    for chain_info in config:
        print('Inspecting chain {}'.format(chain_info))
        healthy_api_endpoints = []
        healthy_p2p_endpoints = []
        healthy_history_endpoints = []
        healthy_hyperion_endpoints = []
        healthy_atomic_endpoints = []
        healthy_ipfs_endpoints = []
        producers_array = []
        testnet_producers = []
        
        isFIO = chain_info[
            'chain_id'] == '21dcae42c0182200e93f954a074011f9048a7624c6fe81d3c9541a614a88bd1c' or chain_info[
                'chain_id'] == 'b20901380af44ef59c5918439a1f9a41d83669020319a80574b804a5f95cbd7e'

        try:
            producers = get_producers(chain_info)
        except Exception as e:
            print('Too many retries getting producers')
            continue
        
        for producer in producers:
            if producer['owner'] != 'ivote4waxusa':
                continue
            print(producer)
            print('Checking producer {}'.format(producer['owner']))
            checker = Checker(chain_info, producer)
            checker.run_checks()

            healthy_api_endpoints += checker.healthy_api_endpoints
            healthy_p2p_endpoints += checker.healthy_p2p_endpoints
            healthy_history_endpoints += checker.healthy_history_endpoints
            healthy_hyperion_endpoints += checker.healthy_hyperion_endpoints
            healthy_atomic_endpoints += checker.healthy_atomic_endpoints
            healthy_ipfs_endpoints += checker.healthy_ipfs_endpoints

            producer_info = {
                'account': producer['owner'],
                'org_name': checker.org_name,
                'history': len(checker.healthy_history_endpoints),
                'hyperion': len(checker.healthy_hyperion_endpoints),
                'atomic': len(checker.healthy_atomic_endpoints),
                'patroneos': checker.patroneos,
                'position': checker.producer_info['position'],
                'status': checker.status,
                'errors': checker.errors,
                'oks': checker.oks,
                'warnings': checker.warnings,
                'endpoint_errors': checker.endpoint_errors,
                'endpoint_oks': checker.endpoint_oks,
                'endpoints': checker.endpoints,
                # 'p2p_endpoints': checker.p2p_endpoints,
                'bp_json': checker.producer_info['bp_json_url']
            }
            if isFIO:
                producer_info['fio_address'] = producer['fio_address']

            producers_array.append(producer_info)

        healthy_api_endpoints = list(set(healthy_api_endpoints))
        healthy_p2p_endpoints = list(set(healthy_p2p_endpoints))
        healthy_history_endpoints = list(set(healthy_history_endpoints))
        healthy_atomic_endpoints = list(set(healthy_atomic_endpoints))
        healthy_hyperion_endpoints = list(set(healthy_hyperion_endpoints))
        healthy_ipfs_endpoints = list(set(healthy_ipfs_endpoints))
        random.shuffle(producers_array)
        random.shuffle(healthy_api_endpoints)
        random.shuffle(healthy_p2p_endpoints)
        random.shuffle(healthy_history_endpoints)
        random.shuffle(healthy_atomic_endpoints)
        random.shuffle(healthy_hyperion_endpoints)
        random.shuffle(healthy_ipfs_endpoints)

        data = {
            'producers':
            producers_array,
            'last_update':
            datetime.datetime.utcnow().strftime("%d/%m/%y %H:%M:%S UTC"),
            'last_update_iso':
            datetime.datetime.utcnow().isoformat(),
            'healthy_api_endpoints':
            healthy_api_endpoints,
            'healthy_p2p_endpoints':
            healthy_p2p_endpoints,
            'healthy_history_endpoints':
            healthy_history_endpoints,
            'healthy_hyperion_endpoints':
            healthy_hyperion_endpoints,
            'healthy_atomic_endpoints':
            healthy_atomic_endpoints,
            'healthy_ipfs_endpoints':
            healthy_ipfs_endpoints

        }

        CURRENT_DATE = datetime.datetime.today().strftime('%Y-%m-%d')
        if not os.path.exists(pub_path):
            os.makedirs(pub_path)
        with open(f'{pub_path}/{chain_info["chain_id"]}.json', 'w') as fp:
            json.dump(data, fp, sort_keys=True, indent=4)
        with open(f'{pub_path}/{chain_info["chain_id"]}-{CURRENT_DATE}.json', 'w') as fp:
            json.dump(data, fp, sort_keys=True, indent=4)

