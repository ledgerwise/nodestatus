#!/usr/bin/env python3

import logging
import argparse
import os
import colorlog
import inspect
import pprint
import time
import random
import traceback
import json
import datetime
import eospy.cleos
from tenacity import retry, stop_after_attempt, wait_fixed
from urllib.parse import urljoin
from include.checker import Checker

pp = pprint.PrettyPrinter(indent=4)

SCRIPT_PATH = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))

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

VERBOSE = args.verbose
DEBUG = args.debug
LOG_FILE = args.log_file
CHAINS = [{
    'name': 'WAX',
    'chain_id':
    '1064487b3cd1a897ce03ae5b6a865651747e2e152090f99c1d19d44e01aea5a4',
    'api_node': 'https://waxapi.eosmetal.io',
    'testnet': False,
    'limit': 50
}]

if DEBUG:
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)
else:
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if VERBOSE:
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

fh = logging.FileHandler(LOG_FILE)
logger.addHandler(fh)
fh.setFormatter(formatter)

SCRIPT_PATH = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_producers(chain):
    try:
        LIMIT = 200
        CLEOS = eospy.cleos.Cleos(url=chain['api_node'])
        result = CLEOS.get_producers(limit=LIMIT)

        isFIO = 'producers' in result

        if isFIO:
            producers = result['producers']
        else:
            producers = result['rows']

        while result['more'] != '':
            result = CLEOS.get_producers(limit=LIMIT,
                                         lower_bound=result['more'])
            producers += result['rows']

        active_producers = [{
            'owner':
            x['owner'],
            'url':
            x['url'],
            'bp_json_url':
            urljoin(x['url'], '/bp.json'),
            'chains_json_url':
            urljoin(x['url'], '/chains.json')
        } for x in producers if x['is_active'] != 0]

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
        logging.critical('Error getting producers: {}'.format(e))
        raise


def main():
    CONFIG_PATH = SCRIPT_PATH + '/config.json'
    try:
        with open(CONFIG_PATH, 'r') as fp:
            CHAINS = json.load(fp)
    except Exception as e:
        logging.critical('Error getting config from {}: {}'.format(
            CONFIG_PATH, e))
        quit()

    for chain_info in CHAINS:
        logging.info('Inspecting chain {}'.format(chain_info))
        healthy_api_endpoints = []
        healthy_p2p_endpoints = []
        healthy_history_endpoints = []
        healthy_hyperion_endpoints = []
        producers_array = []
        testnet_producers = []

        try:
            producers = get_producers(chain_info)
            for testnet in chain_info['testnets']:
                testnet_producers_array = get_producers(testnet)
                testnet_producers += [
                    x['owner'] for x in testnet_producers_array
                ]
            print(testnet_producers)
        except Exception as e:
            logging.critical('Too many retries getting producers')
            continue

        for producer in producers:
            logging.info('Checking producer {}'.format(producer['owner']))
            checker = Checker(chain_info, producer, logging)
            checker.run_checks()

            #Check producer in testnet
            if producer['owner'] not in testnet_producers:
                msg = 'Producer {} might not be registered or registered with some other name in testnet'.format(
                    producer['owner'])
                logging.warning(msg)
                checker.warnings.append(msg)
            else:
                msg = 'Producer name {} is registered as producer in testnet'.format(
                    producer['owner'])
                logging.info(msg)
                checker.oks.append(msg)

            healthy_api_endpoints += checker.healthy_api_endpoints
            healthy_p2p_endpoints += checker.healthy_p2p_endpoints
            healthy_history_endpoints += checker.healthy_history_endpoints
            healthy_hyperion_endpoints += checker.healthy_hyperion_endpoints
            producers_array.append({
                'account':
                producer['owner'],
                'org_name':
                checker.org_name,
                'history':
                len(checker.healthy_history_endpoints),
                'hyperion':
                len(checker.healthy_hyperion_endpoints),
                'patroneos':
                checker.patroneos,
                'position':
                checker.producer_info['position'],
                'status':
                checker.status,
                'errors':
                checker.errors,
                'oks':
                checker.oks,
                'warnings':
                checker.warnings,
                'endpoint_errors':
                checker.endpoint_errors,
                'endpoint_oks':
                checker.endpoint_oks,
                'api_endpoints':
                checker.api_endpoints,
                'p2p_endpoints':
                checker.p2p_endpoints,
                'bp_json':
                checker.producer_info['bp_json_url']
            })

        healthy_api_endpoints = list(set(healthy_api_endpoints))
        healthy_p2p_endpoints = list(set(healthy_p2p_endpoints))
        healthy_history_endpoints = list(set(healthy_history_endpoints))
        random.shuffle(producers_array)
        random.shuffle(healthy_api_endpoints)
        random.shuffle(healthy_p2p_endpoints)
        random.shuffle(healthy_history_endpoints)

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
            healthy_hyperion_endpoints
        }

        PUB_PATH = '{}/pub'.format(SCRIPT_PATH)
        if not os.path.exists(PUB_PATH):
            os.makedirs(PUB_PATH)
        with open('{}/pub/{}.json'.format(SCRIPT_PATH, chain_info['chain_id']),
                  'w') as fp:
            json.dump(data, fp, sort_keys=True, indent=4)


if __name__ == "__main__":
    main()
