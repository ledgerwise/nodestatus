import requests
import humanize
import socket
import eospy.cleos
import datetime
import dateutil.parser
from urllib.parse import urljoin
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
import pprint
import time
import json
from deepdiff import DeepDiff

pp = pprint.PrettyPrinter(indent=4)
DELAY = 0.3


class Checker:
    def __init__(self, chain_info, producer, logging):
        self.chain_info = chain_info
        self.wrong_chain_id = False
        self.logging = logging
        self.producer_info = producer
        self.org_name = self.producer_info["owner"]
        self.bp_json = None
        self.bp_json_string = "{}"
        self.status = 0
        self.errors = []
        self.oks = []
        self.warnings = []
        self.endpoint_errors = {}
        self.endpoint_oks = {}
        self.healthy_api_endpoints = []
        self.healthy_p2p_endpoints = []
        self.healthy_history_endpoints = []
        self.healthy_hyperion_endpoints = []
        self.healthy_atomic_endpoints = []
        self.healthy_ipfs_endpoints = []
        self.healthy_lightapi_endpoints = []
        self.ipfs_errors = []
        self.lightapi_errors = []
        self.nodes = []
        self.endpoints = []
        self.onchain_bp_json = False

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_producer_chainsjson_path(self, url, chain_id, timeout):
        time.sleep(DELAY)
        try:
            chains_json_content = requests.get(url, timeout=timeout).json()
            return chains_json_content["chains"][chain_id]
        except Exception as e:
            self.logging.critical(
                "Error getting chains.json from {}: {}".format(url, e)
            )
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_onchain_bpjson(self, timeout):
        time.sleep(DELAY)
        API_NODE = self.chain_info["api_node"]
        ENDPOINT = f"{API_NODE}/v1/chain/get_table_rows"
        PRODUCER = self.producer_info["owner"]

        payload = {
            "json": True,
            "code": "producerjson",
            "scope": "producerjson",
            "table": "producerjson",
            "lower_bound": PRODUCER,
            "upper_bound": PRODUCER,
            "index_position": 1,
            "key_type": "",
            "limit": "1",
            "reverse": False,
            "show_payer": True,
        }

        response = requests.post(ENDPOINT, json=payload, timeout=timeout)
        if response.status_code != 200:
            msg = f"Error getting bpjson on chain for producer {PRODUCER}"
            self.logging.critical(msg)
            print(response.text)
            return
        else:
            result = response.json()
            if len(result["rows"]) < 1:
                msg = f"No bpjson on chain for producer {PRODUCER}"
                self.warnings.append(msg)
                self.logging.critical(msg)
            else:
                try:
                    onchain_bpjson = json.loads(result["rows"][0]["data"]["json"])
                except:
                    onchain_bpjson = {}
                online_bpjson = json.loads(self.bp_json_string)
                diff = DeepDiff(onchain_bpjson, online_bpjson)
                if not diff:
                    msg = f"bpjson on chain for producer {PRODUCER} matches the one online"
                    self.oks.append(msg)
                    self.logging.info(msg)
                else:
                    msg = f"bpjson on chain for producer {PRODUCER} doesnt match the one online"
                    self.warnings.append(msg)
                    self.logging.critical(msg, diff)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def get_bpjson(self, timeout):
        time.sleep(DELAY)
        has_ssl_endpoints = False
        has_p2p_endpoints = False
        has_api_endpoints = False

        # Check if network is defined in chains.json
        chains_json_path = self.get_producer_chainsjson_path(
            self.producer_info["chains_json_url"], self.chain_info["chain_id"], timeout
        )

        if chains_json_path:
            self.producer_info["bp_json_url"] = urljoin(
                self.producer_info["url"], chains_json_path
            )

        self.logging.info(f'Bp.json url: ${self.producer_info["bp_json_url"]}')

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
            }
            response = requests.get(
                self.producer_info["bp_json_url"], headers=headers, timeout=timeout
            )

            if response.status_code != 200:
                msg = "Error getting bp.json: {} - {}".format(
                    response.status_code,
                    (
                        (response.text[:75] + "..")
                        if len(response.text) > 75
                        else response.text
                    ),
                )
                self.logging.critical(msg)
                self.errors.append(msg)
                self.status = 2
                return

            self.bp_json = response.json()
            self.bp_json_string = response.text

            if "org" in self.bp_json:
                self.org = self.bp_json["org"]

            if not "github_user" in self.bp_json["org"]:
                msg = "github_user missing in bp.json"
                self.logging.warning(msg)
                self.warnings.append(msg)
            else:
                msg = "github_user present in bp.json"
                self.oks.append(msg)

            nodes = self.bp_json["nodes"]
            for index, node in enumerate(nodes):
                print(node)
                if not "node_type" in node:
                    msg = "node_type not present for node {}".format(index + 1)
                    self.logging.critical(msg)
                    self.errors.append(msg)
                    self.status = 2
                    continue
                node_type = node["node_type"]
                if type(node_type) is str:
                    node_type = [node_type]
                    node["node_type"] = node_type

                if "api_endpoint" in node:
                    self.endpoint_errors[node["api_endpoint"]] = []
                    self.endpoint_oks[node["api_endpoint"]] = []
                    self.endpoints.append(node["api_endpoint"])
                if "ssl_endpoint" in node:
                    has_ssl_endpoints = True
                    self.endpoint_errors[node["ssl_endpoint"]] = []
                    self.endpoint_oks[node["ssl_endpoint"]] = []
                    self.endpoints.append(node["ssl_endpoint"])
                if "p2p_endpoint" in node:
                    has_p2p_endpoints = True
                    self.endpoint_errors[node["p2p_endpoint"]] = []
                    self.endpoint_oks[node["p2p_endpoint"]] = []
                    self.endpoints.append(node["p2p_endpoint"])

                if not "features" in node and "query" in node_type:
                    msg = "features not present for node {} of type query".format(
                        index + 1
                    )
                    self.logging.critical(msg)
                    self.errors.append(msg)
                    self.status = 2
                    continue

                if "features" in node:
                    if "chain-api" in node["features"]:
                        has_api_endpoints = True
                if "query" in node_type or "seed" in node_type:
                    self.nodes.append(node)
            self.endpoints = list(set(self.endpoints))
            self.org_name = self.bp_json["org"]["candidate_name"]

            if not has_ssl_endpoints:
                msg = "No SSL api nodes defined (ssl_endpoint)"
                self.errors.append(msg)
                self.logging.critical(msg)

            if not has_p2p_endpoints:
                self.status = 2
                msg = "No P2P nodes defined (p2p_endpoint)"
                self.errors.append(msg)
                self.logging.critical(msg)

            if not has_api_endpoints:
                msg = "No chain api nodes defined"
                self.errors.append(msg)
                self.logging.critical(msg)
                self.status = 2

        except requests.exceptions.SSLError as e:
            self.status = 2
            msg = "Error getting {} bp.json ({}): Certificate error".format(
                self.producer_info["owner"], self.producer_info["bp_json_url"]
            )
            self.logging.critical(msg)
            self.errors.append(msg)

        except Exception as e:
            self.status = 2
            msg = "Error getting {} bp.json ({}): {} {}".format(
                self.producer_info["owner"],
                self.producer_info["bp_json_url"],
                e,
                type(e),
            )

            print("excepcion", e)
            self.logging.critical(msg)
            self.errors.append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
    def check_p2p(self, url, timeout):
        time.sleep(DELAY)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            host, port = url.split(":")
            result = sock.connect_ex((host, int(port)))
            if result != 0:
                self.status = 2
                self.endpoint_errors[url].append("Error connecting to {}".format(url))
                self.logging.critical("Error connecting to {}".format(url))
                return
        except ValueError as e:
            self.status = 2
            self.endpoint_errors[url].append(
                "Invalid p2p host:port value {}".format(url)
            )
            self.logging.critical("Invalid p2p host:port value {}".format(url))
        except Exception as e:
            self.status = 2
            self.endpoint_errors[url].append(
                "Error connecting to {}: {}".format(url, e)
            )
            self.logging.critical(
                "Error connecting to {}: {} {}".format(url, type(e), e)
            )

        self.healthy_p2p_endpoints.append(url)
        msg = "P2P node {} is responding".format(url)
        self.logging.info(msg)
        print(self.endpoint_oks)
        self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2), reraise=True)
    def check_api(self, url, chain_id, timeout):
        time.sleep(DELAY)
        errors_found = False
        try:
            api_url = f'{url.rstrip("/")}/v1/chain/get_info'
            response = requests.get(api_url, timeout=timeout)
            if response.status_code != 200:
                self.status = 2
                msg = "Error connecting to {}: {}".format(
                    api_url, "Response error: {}".format(response.status_code)
                )
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                return

            # Check for appropriate CORS headers
            allow_origin = response.headers.get("access-control-allow-origin")
            if not allow_origin or allow_origin != "*":
                self.status = 2
                msg = "Invalid value for CORS header access-control-allow-origin or header not present"
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
            else:
                msg = "CORS headers properly configured"
                self.logging.info(msg)
                self.endpoint_oks[url].append(msg)

            info = response.json()

            if info["chain_id"] != chain_id:
                self.wrong_chain_id = True
                self.status = 2
                msg = "Wrong chain id"
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                errors_found = True
                return

            head_block_time = info["head_block_time"]
            head_block_time_dt = datetime.datetime.strptime(
                head_block_time, "%Y-%m-%dT%H:%M:%S.%f"
            )
            now = datetime.datetime.utcnow()
            secs_diff = int((now - head_block_time_dt).total_seconds())

            if secs_diff > 300:
                self.status = 2
                msg = "Last block synced {} ago".format(
                    humanize.naturaldelta(secs_diff)
                )
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                errors_found = True

        except requests.exceptions.SSLError as e:
            self.status = 2
            msg = "Error connecting to {}: {}".format(url, "Certificate error")
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        except requests.exceptions.Timeout as e:
            self.status = 2
            msg = "Error connecting to {}: {}".format(url, "Connection timed out")
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        except Exception as e:
            self.status = 2
            msg = "Error connecting to {}: {} {}".format(url, type(Exception), e)
            self.endpoint_errors[url].append(msg)
            self.logging.critical(msg)
            errors_found = True

        if not errors_found:
            self.healthy_api_endpoints.append(url)
            msg = "API node {} is responding correctly".format(url)
            self.logging.info(msg)
            self.endpoint_oks[url].append(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_history(self, url, timeout):
        time.sleep(DELAY)
        try:
            history_url = f'{url.rstrip("/")}/v1/history/get_actions'
            payload = {"account_name":"eosio","pos":-1, "offset":-3}
            response = requests.post(history_url, timeout=timeout, json=payload)
            if not "actions" in response.json():
                self.logging.info("No actions in response")
                return

            if len(response.json()["actions"]) == 0:
                self.logging.info("0 actions returned for eosio")
                return

        except Exception as e:
            msg = "Error testing v1 history from {}: {}".format(url, e)
            self.logging.error(msg)
            self.endpoint_errors[url].append(msg)
            self.status = 2
            return

        self.healthy_history_endpoints.append(url)
        msg = "History v1 ok for {}".format(url)
        self.endpoint_oks[url].append(msg)
        self.logging.info(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_account_query(self, url, timeout):
        time.sleep(DELAY)
        try:
            account = "ledgerwiseio"
            api_url = "{}/v1/chain/get_accounts_by_authorizers".format(url.rstrip("/"))
            response = requests.post(
                api_url,
                json={
                    "json": True,
                    "accounts": [
                        account,
                    ],
                },
                timeout=timeout,
            )
            if response.status_code != 200:
                print(response.content)
                self.status = 2
                msg = "Error getting authorizers for account {} from {}: {}".format(
                    account, api_url, "Response error: {}".format(response.status_code)
                )
                self.endpoint_errors[url].append(msg)
                self.logging.critical(msg)
                return

            else:
                msg = "Get authorizers from account is ok on {}".format(url)
                self.endpoint_oks[url].append(msg)
                self.logging.info(msg)

        except Exception as e:
            msg = "Error getting authorizers from {}: {}".format(url, e)
            self.logging.error(msg)
            return

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_hyperion(self, url, timeout):
        time.sleep(DELAY)
        errors_found = False
        try:
            # Check last hyperion indexed action
            history_url = "{}/v2/history/get_actions?limit=1".format(url.rstrip("/"))
            response = requests.get(history_url, timeout=timeout)
            if response.status_code != 200:
                self.logging.info("No hyperion found ({})".format(response.status_code))
                self.endpoint_errors[url].append(
                    f"Error {response.status_code} testing hyperion"
                )
                self.status = 2
                errors_found = True
                return

            json = response.json()
            last_action_date = dateutil.parser.parse(
                json["actions"][0]["timestamp"]
            ).replace(tzinfo=None)
            diff_secs = (datetime.datetime.utcnow() - last_action_date).total_seconds()
            if diff_secs > 600:
                msg = "Hyperion Last action {} ago".format(
                    humanize.naturaldelta(diff_secs)
                )
                self.logging.critical(msg)
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True

            # Check hyperion service health
            health_url = "{}/v2/health".format(url.rstrip("/"))
            response = requests.get(health_url, timeout=timeout)
            if response.status_code != 200:
                msg = "Error {} trying to check hyperion health endpoint".format(
                    response.status_code
                )
                self.logging.info(msg)
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True
                return

            json = response.json()
            for item in json["health"]:
                if item["status"] != "OK":
                    msg = "Hyperion service {} has status {}".format(
                        item["service"], item["status"]
                    )
                    self.logging.critical(msg)
                    self.endpoint_errors[url].append(msg)
                    self.status = 2
                    errors_found = True

                if item["service"] == "Elasticsearch":
                    missing_blocks = 0
                    if "missing_blocks" in item["service_data"]:
                        missing_blocks = int(item["service_data"]["missing_blocks"])
                    elif (
                        "total_indexed_blocks" in item["service_data"]
                        and "last_indexed_block" in item["service_data"]
                    ):
                        last_indexed_block = item["service_data"]["last_indexed_block"]
                        total_indexed_blocks = item["service_data"][
                            "total_indexed_blocks"
                        ]
                        missing_blocks = abs(last_indexed_block - total_indexed_blocks)
                    if missing_blocks > 0:
                        msg = "Hyperion ElastiSearch missing some blocks"
                        self.logging.critical(msg)
                        self.endpoint_errors[url].append(msg)
                        self.status = 2
                        errors_found = True

        except Exception as e:
            msg = "Error getting hyperion history from {}: {}".format(url, e)
            self.logging.error(msg)
            self.endpoint_errors[url].append(msg)
            self.status = 2
            errors_found = True
            return

        if not errors_found:
            self.healthy_hyperion_endpoints.append(url)
            msg = "Hyperion history ok for {}".format(url)
            self.endpoint_oks[url].append(msg)
            self.logging.info(msg)

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(2), reraise=True)
    def check_atomic(self, url, timeout):
        time.sleep(DELAY)
        errors_found = False
        try:
            # Check atomic service health
            health_url = "{}/health".format(url.rstrip("/"))
            response = requests.get(health_url, timeout=timeout)
            if response.status_code != 200:
                msg = "Error {} trying to check atomic health endpoint".format(
                    response.status_code
                )
                self.logging.info(msg)
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True
                return

            json = response.json()
            for item in json["data"]:
                if "status" in item:
                    if item["status"] != "OK":
                        msg = "Atomic service {} has status {}".format(
                            item["service"], item["status"]
                        )
                        self.logging.critical(msg)
                        self.endpoint_errors[url].append(msg)
                        self.status = 2
                        errors_found = True

            head_block = json["data"]["chain"]["head_block"]
            last_indexed_block = 0
            for reader in json["data"]["postgres"]["readers"]:
                last_indexed_block = max(last_indexed_block, int(reader["block_num"]))
            if abs(last_indexed_block - head_block) > 100:
                msg = "Atomic API last_indexed_block is behind head_block"
                self.logging.critical(msg)
                self.endpoint_errors[url].append(msg)
                self.status = 2
                errors_found = True

        except Exception as e:
            msg = "Error getting atomic data from {}: {}".format(url, e)
            self.logging.error(msg)
            self.logging.critical(msg)
            self.endpoint_errors[url].append(msg)
            self.status = 2
            errors_found = True
            return

        if not errors_found:
            self.healthy_atomic_endpoints.append(url)
            msg = "Atomic API ok for {}".format(url)
            self.endpoint_oks[url].append(msg)
            self.logging.info(msg)


    @retry(stop=stop_after_attempt(1), wait=wait_fixed(3), reraise=True)
    def check_ipfs(self, url, timeout):
        time.sleep(DELAY)
        try:
            path = "/ipfs/QmWnfdZkwWJxabDUbimrtaweYF8u9TaESDBM8xvRxxbQxv"
            api_url = urljoin(url.rstrip("/"), path)
            response = requests.get(api_url, timeout=timeout)
            if response.status_code != 200:
                print(response.text)
                print(response.status_code)
                self.status = 2
                msg = f'Error getting ipfs image from {api_url}: Response error: {response.status_code}'

                self.ipfs_errors[url].append(msg)
                self.logging.critical(msg)
                return

            else:
                msg = "IPFS is ok on {}".format(url)
                self.endpoint_oks[url].append(msg)
                self.healthy_ipfs_endpoints.append(url)
                self.logging.info(msg)

        except Exception as e:
            print(e)
            msg = "Error getting ipfs image from {}: {}".format(url, e)
            self.logging.error(msg)
            return

    @retry(stop=stop_after_attempt(1), wait=wait_fixed(3), reraise=True)
    def check_lightapi(self, url, timeout):
        time.sleep(DELAY)
        try:
            path = "/api/status"
            api_url = urljoin(url.rstrip("/"), path)
            response = requests.get(api_url, timeout=timeout)
            if response.status_code != 200:
                self.status = 2
                msg = f"Light API error: {response.status_code}"

                self.lightapi_errors[url].append(msg)
                self.logging.critical(msg)
                return

            else:
                msg = f"Light API is ok on {url}"
                self.endpoint_oks[url].append(msg)
                self.healthy_lightapi_endpoints.append(url)
                self.logging.info(msg)

        except Exception as e:
            msg = "Error getting ipfs image from {}: {}".format(url, e)
            self.logging.error(msg)
            return

    def run_checks(self):
        self.get_bpjson(timeout=self.chain_info["timeout"])
        if self.chain_info["name"] == "WAX":
            self.get_onchain_bpjson(timeout=self.chain_info["timeout"])
        if self.nodes:
            for node in self.bp_json["nodes"]:
                if (
                    "node_type" in node
                    and "query" in node["node_type"]
                    and "features" in node
                ):
                    # Check API
                    if "chain-api" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_api(
                                node["api_endpoint"],
                                self.chain_info["chain_id"],
                                self.chain_info["timeout"],
                            )
                            if self.wrong_chain_id:
                                return
                        if "ssl_endpoint" in node:
                            self.check_api(
                                node["ssl_endpoint"],
                                self.chain_info["chain_id"],
                                self.chain_info["timeout"],
                            )
                            if self.wrong_chain_id:
                                return
                    # Check Account Query
                    if "account-query" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_account_query(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_account_query(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                    # Check History V1
                    if "history-v1" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_history(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_history(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                    # Check Hyperion
                    if "hyperion-v2" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_hyperion(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_hyperion(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                    # Check Hyperion
                    if "atomic-assets-api" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_atomic(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_atomic(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                    # Check IPFS
                    if "ipfs" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_ipfs(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_ipfs(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                    # Check lightapi
                    if "light-api" in node["features"]:
                        if "api_endpoint" in node:
                            self.check_lightapi(
                                node["api_endpoint"], self.chain_info["timeout"]
                            )
                        if "ssl_endpoint" in node:
                            self.check_lightapi(
                                node["ssl_endpoint"], self.chain_info["timeout"]
                            )

                if "node_type" in node and "seed" in node["node_type"]:
                    # Check P2P
                    if "p2p_endpoint" in node:
                        self.check_p2p(node["p2p_endpoint"], self.chain_info["timeout"])
