import requests
import humanize
import socket
import eospy.cleos
import datetime
import dateutil.parser
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_fixed


class Checker:
    def __init__(self, chain_info, producer, logging):
        self.chain_info = chain_info
        self.logging = logging
        self.producer_info = producer
        self.org_name = self.producer_info['owner']
        self.bp_json = None
        self.patroneos = 0
        self.status = 0
        self.errors = []
        self.oks = []
        self.endpoint_errors = {}
        self.endpoint_oks = {}
        self.api_endpoints = []
        self.p2p_endpoints = []
        self.healthy_api_endpoints = []
        self.healthy_p2p_endpoints = []
        self.healthy_history_endpoints = []
        self.healthy_hyperion_endpoints = []

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_producer_chainsjson_path(self, url, chain_id):
        try:
            chains_json_content = requests.get(url).json()
            return chains_json_content['chains'][chain_id]
        except Exception as e:
            self.logging.critical(
                'Error getting chains.json from {}: {}'.format(url, e))
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_bpjson(self):
        has_ssl_endpoints = False

        #Check if network is defined in chains.json
        chains_json_path = self.get_producer_chainsjson_path(
            self.producer_info['chains_json_url'], self.chain_info['chain_id'])

        if chains_json_path:
            self.producer_info['bp_json_url'] = urljoin(
                self.producer_info['url'], chains_json_path)

        try:
            headers = {
                'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
            }
            response = requests.get(self.producer_info['bp_json_url'],
                                    headers=headers,
                                    timeout=2)

            if response.status_code != 200:
                msg = ('Error getting bp.json: {} - {}'.format(
                    response.status_code,
                    (response.text[:75] +
                     '..') if len(response.text) > 75 else response.text))
                self.logging.critical(msg)
                self.errors.append(msg)
                return

            self.bp_json = response.json()

            nodes = self.bp_json['nodes']
            for node in nodes:
                if 'api_endpoint' in node:
                    if node['api_endpoint'] != '':
                        self.api_endpoints.append(node['api_endpoint'])
                        self.endpoint_errors[node['api_endpoint']] = []
                        self.endpoint_oks[node['api_endpoint']] = []
                if 'ssl_endpoint' in node:
                    has_ssl_endpoints = True
                    if node['ssl_endpoint'] != '':
                        self.api_endpoints.append(node['ssl_endpoint'])
                        self.endpoint_errors[node['ssl_endpoint']] = []
                        self.endpoint_oks[node['ssl_endpoint']] = []
                if 'p2p_endpoint' in node:
                    if node['p2p_endpoint'] != '':
                        self.p2p_endpoints.append(node['p2p_endpoint'])
                        self.endpoint_errors[node['p2p_endpoint']] = []
                        self.endpoint_oks[node['p2p_endpoint']] = []

            self.api_endpoints = list(set(self.api_endpoints))
            self.p2p_endpoint = list(set(self.p2p_endpoints))
            self.org_name = self.bp_json['org']['candidate_name']

            if not has_ssl_endpoints:
                msg = 'No SSL api nodes defined (ssl_endpoint)'
                self.errors.append(msg)
                self.logging.critical(msg)

            if not has_ssl_endpoints and len(self.api_endpoints) == 0:
                msg = 'No api nodes defined'
                self.errors.append(msg)
                self.logging.critical(msg)

            if len(self.p2p_endpoints) == 0:
                msg = 'No P2P nodes defined (p2p_endpoint)'
                self.errors.append(msg)
                self.logging.critical(msg)

        except Exception as e:
            self.status = 2
            msg = 'Error getting {} bp.json ({}): {}'.format(
                self.producer_info, self.producer_info['bp_json_url'], e)
            self.logging.critical(msg)
            self.errors.append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
    def check_p2p(self, url):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            host, port = url.split(':')
            result = sock.connect_ex((host, int(port)))
            if result != 0:
                self.status = 2
                self.endpoint_errors[url].append(
                    'Error connecting to {}'.format(url))
                self.logging.critical('Error connecting to {}'.format(url))
                return
        except Exception as e:
            self.status = 2
            self.endpoint_errors[url].append(
                'Error connecting to {}: {}'.format(url, e))
            self.logging.critical('Error connecting to {}: {}'.format(url, e))

        self.healthy_p2p_endpoints.append(url)
        msg = 'P2P node {} is responding'.format(url)
        self.logging.info(msg)
        self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def check_api(self, url, chain_id):
        try:
            api_url = url.rstrip('/')
            cleos = eospy.cleos.Cleos(url=api_url)
            info = cleos.get_info(timeout=2)

            head_block_time = info['head_block_time']
            head_block_time_dt = datetime.datetime.strptime(
                head_block_time, "%Y-%m-%dT%H:%M:%S.%f")
            now = datetime.datetime.utcnow()
            secs_diff = int((now - head_block_time_dt).total_seconds())

            if secs_diff > 300:
                self.status = 2
                msg = 'Last block synced {} ago'.format(
                    humanize.naturaldelta(secs_diff))
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)

            if info['chain_id'] != chain_id:
                self.status = 2
                msg = 'Wrong chain id'
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)

        except requests.exceptions.SSLError as e:
            self.status = 2
            msg = 'Error connecting to {}: {}'.format(url, 'Certificate error')
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)

        except requests.exceptions.Timeout as e:
            self.status = 2
            msg = 'Error connecting to {}: {}'.format(url,
                                                      'Connection timed out')
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)

        except Exception as e:
            print(type(Exception))
            self.status = 2
            msg = 'Error connecting to {}: {}'.format(url, e)
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)

        self.healthy_api_endpoints.append(url)
        msg = 'API node {} is responding correctly'.format(url)
        self.logging.info(msg)
        self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_history(self, url):
        try:
            history_url = url.rstrip('/')
            cleos = eospy.cleos.Cleos(url=history_url)
            result = cleos.get_actions('eosio')
            if not 'actions' in result:
                self.logging.info('No actions in response')
                return

            if len(result['actions']) == 0:
                self.logging.info('0 actions returned for eosio')
                return

        except Exception as e:
            msg = 'Error getting history from {}: {}'.format(url, e)
            self.logging.error(msg)
            return

        self.healthy_history_endpoints.append(url)
        msg = 'History ok for {}'.format(url)
        self.endpoint_oks[url].append(msg)
        self.logging.info(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_hyperion(self, url):
        try:
            history_url = '{}/v2/history/get_actions?limit=1'.format(
                url.rstrip('/'))
            response = requests.get(history_url)
            if response.status_code != 200:
                self.logging.info('No hyperion found ({})'.format(
                    response.status_code))
                return

            json = response.json()
            last_action_date = dateutil.parser.parse(
                json['actions'][0]['@timestamp']).replace(tzinfo=None)
            diff_secs = (datetime.datetime.utcnow() -
                         last_action_date).total_seconds()
            if diff_secs > 600:
                msg = 'Hyperion Last action {} ago'.format(
                    humanize.naturaldelta(diff_secs))
                self.logging.critical(msg)
                return

        except Exception as e:
            self.logging.error(
                'Error getting hyperion history from {}: {}'.format(url, e))
            return

        self.healthy_hyperion_endpoints.append(url)
        msg = 'Hyperion history ok for {}'.format(url)
        self.endpoint_oks[url].append(msg)
        self.logging.info(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_patroneos(self, url):
        try:
            url = url.rstrip('/')
            headers = {
                'Content-type': 'application/json',
                'Accept': 'text/plain'
            }
            r = requests.post('{}/v1/chain/get_account'.format(url),
                              data='{"account_name"="eosmetaliobp"}',
                              headers=headers,
                              timeout=2)
            r_json = r.json()

            if 'message' not in r_json:
                self.logging.info(
                    'Response doesn\'t look like patroneos (no message)')
                return
            else:
                if r_json['message'] != 'INVALID_JSON':
                    self.logging.info(
                        'Response doesn\'t look like patroneos  (message not INVALID_JSON)'
                    )
                    return

        except Exception as e:
            self.logging.critical(
                'Error verifyng patroneos from {}: {}'.format(url, e))
            return

        self.patroneos = 1
        msg = 'Patroneos ok for {}'.format(url)
        self.endpoint_oks[url].append(msg)
        self.logging.info(msg)

    def run_checks(self):
        self.get_bpjson()
        if self.bp_json:
            for p2p in self.p2p_endpoints:
                self.check_p2p(p2p)
            for api in self.api_endpoints:
                self.check_api(api, self.chain_info['chain_id'])
                self.check_history(api)
                self.check_hyperion(api)
                self.check_patroneos(api)