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
        self.warnings = []
        self.endpoint_errors = {}
        self.endpoint_oks = {}
        self.api_endpoints = []
        self.p2p_endpoints = []
        self.healthy_api_endpoints = []
        self.healthy_p2p_endpoints = []
        self.healthy_history_endpoints = []
        self.healthy_hyperion_endpoints = []

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_producer_chainsjson_path(self, url, chain_id, timeout):
        try:
            chains_json_content = requests.get(url, timeout=timeout).json()
            return chains_json_content['chains'][chain_id]
        except Exception as e:
            self.logging.critical(
                'Error getting chains.json from {}: {}'.format(url, e))
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_bpjson(self, timeout):
        has_ssl_endpoints = False

        #Check if network is defined in chains.json
        chains_json_path = self.get_producer_chainsjson_path(
            self.producer_info['chains_json_url'], self.chain_info['chain_id'],
            timeout)

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
                                    timeout=timeout)

            if response.status_code != 200:
                msg = ('Error getting bp.json: {} - {}'.format(
                    response.status_code,
                    (response.text[:75] +
                     '..') if len(response.text) > 75 else response.text))
                self.logging.critical(msg)
                self.errors.append(msg)
                self.status = 2
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
                self.status = 2

            if len(self.p2p_endpoints) == 0:
                self.status = 2
                msg = 'No P2P nodes defined (p2p_endpoint)'
                self.errors.append(msg)
                self.logging.critical(msg)

        except requests.exceptions.SSLError as e:
            self.status = 2
            msg = 'Error getting {} bp.json ({}): Certificate error'.format(
                self.producer_info['owner'], self.producer_info['bp_json_url'])
            self.logging.critical(msg)
            self.errors.append(msg)

        except Exception as e:
            self.status = 2
            msg = 'Error getting {} bp.json ({}): {} {}'.format(
                self.producer_info['owner'], self.producer_info['bp_json_url'],
                e, type(e))
            self.logging.critical(msg)
            self.errors.append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
    def check_p2p(self, url, timeout):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            host, port = url.split(':')
            result = sock.connect_ex((host, int(port)))
            if result != 0:
                self.status = 2
                self.endpoint_errors[url].append(
                    'Error connecting to {}'.format(url))
                self.logging.critical('Error connecting to {}'.format(url))
                return
        except ValueError as e:
            self.status = 2
            self.endpoint_errors[url].append(
                'Invalid p2p host:port value {}'.format(url))
            self.logging.critical('Invalid p2p host:port value {}'.format(url))
        except Exception as e:
            self.status = 2
            self.endpoint_errors[url].append(
                'Error connecting to {}: {}'.format(url, e))
            self.logging.critical('Error connecting to {}: {} {}'.format(
                url, type(e), e))

        self.healthy_p2p_endpoints.append(url)
        msg = 'P2P node {} is responding'.format(url)
        self.logging.info(msg)
        self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def check_api(self, url, chain_id, timeout):
        errors_found = False
        try:
            api_url = '{}/v1/chain/get_info'.format(url.rstrip('/'))
            response = requests.get(api_url, timeout=timeout)
            if response.status_code != 200:
                self.status = 2
                msg = 'Error connecting to {}: {}'.format(
                    api_url, 'Response error: {}'.format(response.status_code))
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                return

            #Check for appropriate CORS headers
            allow_origin = response.headers.get('access-control-allow-origin')
            if not allow_origin or allow_origin != '*':
                self.status = 2
                msg = 'Invalid value for CORS header access-control-allow-origin or header not present'
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
            else:
                msg = 'CORS headers properly configured'
                self.logging.info(msg)
                self.endpoint_oks[url].append(msg)

            info = response.json()

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
                errors_found = True

            if info['chain_id'] != chain_id:
                self.status = 2
                msg = 'Wrong chain id'
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                errors_found = True

        except requests.exceptions.SSLError as e:
            self.status = 2
            msg = 'Error connecting to {}: {}'.format(url, 'Certificate error')
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        except requests.exceptions.Timeout as e:
            self.status = 2
            msg = 'Error connecting to {}: {}'.format(url,
                                                      'Connection timed out')
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        except Exception as e:
            self.status = 2
            msg = 'Error connecting to {}: {} {}'.format(
                url, type(Exception), e)
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        if not errors_found:
            self.healthy_api_endpoints.append(url)
            msg = 'API node {} is responding correctly'.format(url)
            self.logging.info(msg)
            self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_history(self, url, timeout):
        try:
            history_url = url.rstrip('/')
            cleos = eospy.cleos.Cleos(url=history_url)
            result = cleos.get_actions('eosio', timeout=timeout)
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
    def check_hyperion(self, url, timeout):
        errors_found = False
        try:
            #Check last hyperion indexed action
            history_url = '{}/v2/history/get_actions?limit=1'.format(
                url.rstrip('/'))
            response = requests.get(history_url, timeout=timeout)
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
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True

            #Check hyperion service health
            health_url = '{}/v2/health'.format(url.rstrip('/'))
            response = requests.get(health_url, timeout=timeout)
            if response.status_code != 200:
                self.logging.info(
                    'Error {} trying to check hyperion health endpoint'.format(
                        response.status_code))
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True

            json = response.json()
            for item in json['health']:
                if item['status'] != 'OK':
                    msg = 'Hyperion service {} has status {}'.format(
                        item['service'], item['status'])
                    self.logging.critical(msg)
                    self.endpoint_errors[url].append(msg)
                    self.status = 2
                    errors_found = True

                if item['service'] == 'Elasticsearch':
                    if 'total_indexed_blocks' in item[
                            'service_data'] and 'last_indexed_block' in item[
                                'service_data']:
                        last_indexed_block = item['service_data'][
                            'last_indexed_block']
                        total_indexed_blocks = item['service_data'][
                            'total_indexed_blocks']
                        if last_indexed_block != total_indexed_blocks:
                            msg = 'Hyperion ElastiSearch last_indexed_block is different than total_indexed_blocks'
                            self.logging.critical(msg)
                            self.endpoint_errors[url].append(msg)
                            self.status = 2
                            errors_found = True

        except Exception as e:
            self.logging.error(
                'Error getting hyperion history from {}: {}'.format(url, e))
            return

        if not errors_found:
            self.healthy_hyperion_endpoints.append(url)
            msg = 'Hyperion history ok for {}'.format(url)
            self.endpoint_oks[url].append(msg)
            self.logging.info(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_patroneos(self, url, timeout):
        try:
            url = url.rstrip('/')
            headers = {
                'Content-type': 'application/json',
                'Accept': 'text/plain'
            }
            r = requests.post('{}/v1/chain/get_account'.format(url),
                              data='{"account_name"="eosmetaliobp"}',
                              headers=headers,
                              timeout=timeout)
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
        self.get_bpjson(timeout=self.chain_info['timeout'])
        if self.bp_json:
            for p2p in self.p2p_endpoints:
                self.check_p2p(p2p, self.chain_info['timeout'])
            for api in self.api_endpoints:
                self.check_api(api, self.chain_info['chain_id'],
                               self.chain_info['timeout'])
                self.check_history(api, self.chain_info['timeout'])
                self.check_hyperion(api, self.chain_info['timeout'])
                self.check_patroneos(api, self.chain_info['timeout'])