from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from eth_account import Account
from web3 import Web3

import argparse
import asyncio
import json
import logging
import random
import signal
import sys
import threading
import time
import websockets

from blockchain import BlockchainData, ChainId, Contract, Token

logging.basicConfig(format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


EXECUTION_STARTED = False
TERMINATION_REQUESTED = False
def signal_handler(_sig, _frame):
    logger.info('===== Termination requested =====')
    if not EXECUTION_STARTED:
        sys.exit(0)
    global TERMINATION_REQUESTED
    TERMINATION_REQUESTED = True


def generate_ethereum_accounts(mnemonic, count):
    Account.enable_unaudited_hdwallet_features()
    accounts = []
    for i in range(count):
        account = Account.from_mnemonic(mnemonic, account_path=f"m/44'/60'/0'/0/{i}")
        accounts.append(account)
    return accounts


def request_to_json(method, params, request_id=None):
    if request_id is None:
        request_id = random.randint(0, int(1e9))
    return {
        'jsonrpc': '2.0',
        'id': request_id,
        'method': method,
        'params': params,
    }


def retriable(method):
    def wrapper(self, *args, **kwargs):
        retry_secs = 0.1
        max_retries = 8
        retry_count = 0
        while not TERMINATION_REQUESTED:
            try:
                return method(self, *args, **kwargs)
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    return False
                logger.info(f"[{self.account.address}] Failed to execute {method.__name__}: {e}. Retry #{retry_count}")
                time.sleep(retry_secs)
                retry_secs *= 2
        return False
    return wrapper


class Trader:
    def __init__(self, chain_id: ChainId, account: Account, swap_txs_count=None):
        self.account = account
        self.swap_txs_count = swap_txs_count
        # Initialize web3:
        self.blockchain = BlockchainData(chain_id)
        self.w3 = Web3(Web3.WebsocketProvider(self.blockchain.ws_rpc_url()))
        self.nonce = self.w3.eth.get_transaction_count(account.address)
        self.chain_id = chain_id.value
        self.cake_address = self.blockchain.get_address(Token.CAKE)
        self.weth_address = self.blockchain.get_address(Token.WETH)
        self.smart_router_address = self.blockchain.get_address(Contract.PANCAKE_SMART_ROUTER)
        # Initialize gas price:
        self.gas_price = 2 * self.w3.eth.gas_price
        # Initialize variables:
        self.request_id = 1
        self.signed_txs_by_nonce = {}
        self.nonce_by_request_id = {}
        # Prefill signed txs:
        self.prefill_signed_txs()

    def swap_v2_prefill(self):
        # Swap 1e-9 WETH for CAKE using swapExactTokensForTokens:
        # calldata_v2_router02 = f"0x38ed1739000000000000000000000000000000000000000000000000000000003b9aca00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000{self.account.address.lower()[2:]}000000000000000000000000000000000000000000000000000000012a05f2000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000{self.weth_address.lower()[2:]}000000000000000000000000{self.cake_address.lower()[2:]}"
        calldata_v3_router02 = f"0x472b43f3000000000000000000000000000000000000000000000000000000003b9aca0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080000000000000000000000000{self.account.address.lower()[2:]}0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000{self.weth_address.lower()[2:]}000000000000000000000000{self.cake_address.lower()[2:]}"
        tx = {
            'value': 0,
            'chainId': self.chain_id,
            'from': self.account.address,
            'gas': 250000,
            'gasPrice': self.gas_price,
            'nonce': self.nonce,
            'to': self.smart_router_address,
            'data': calldata_v3_router02,
        }
        signed_tx = Account.sign_transaction(tx, self.account.key)
        self.signed_txs_by_nonce[self.nonce] = signed_tx
        self.nonce += 1
        return True

    def prefill_signed_txs(self):
        for _ in range(self.swap_txs_count):
            self.swap_v2_prefill()

    def start(self):
        logger.info(f'[{self.account.address}] Starting...')
        sending_thread = threading.Thread(target=self._sending_thread)
        sending_thread.start()
        sending_thread.join()

    @retriable
    def _sending_thread(self):
        asyncio.run(self._sending_thread_async())

    async def _sending_thread_async(self):
        ws_url = self.blockchain.ws_rpc_url()
        async with websockets.connect(ws_url) as ws:
            # 1. Send all transactions:
            for (nonce, signed_tx) in self.signed_txs_by_nonce.items():
                await self._send_transaction(ws, signed_tx, nonce)
            # 2. Wait for RPC acknowledgements and resend if necessary:
            while not TERMINATION_REQUESTED and len(self.signed_txs_by_nonce) > 0:
                message = await ws.recv()
                json_response = json.loads(message)
                request_id = json_response["id"]
                nonce = self.nonce_by_request_id[request_id]
                error_message = (json_response["error"].get("message") if "error" in json_response else None) or ""
                if "result" in json_response or error_message.startswith('known transaction'):
                    if nonce in self.signed_txs_by_nonce:
                        tx_hash = self.signed_txs_by_nonce[nonce].hash.hex()
                        del self.signed_txs_by_nonce[nonce]
                        logger.info(f"[{self.account.address}] Tx request accepted (swap): {tx_hash} | nonce={nonce} | id={request_id}")
                else:
                    # Error: RPC didn't accept transaction, resedning...
                    logger.info(f"[{self.account.address}] Recv: {message}")
                    signed_tx = self.signed_txs_by_nonce[nonce]
                    if "insufficient funds" in error_message or "transaction underpriced" in error_message:
                        # No need to resend, tx will fail:
                        logger.info(f"[{self.account.address}] Aborting tx: {signed_tx.hash.hex()} | nonce={nonce}")
                        del self.signed_txs_by_nonce[nonce]
                        continue
                    await self._send_transaction(ws, signed_tx, nonce)

    async def _send_transaction(self, ws, signed_tx, nonce):
        json_request = request_to_json("eth_sendRawTransaction", [signed_tx.rawTransaction.hex()], request_id=self.request_id)
        await ws.send(json.dumps(json_request))
        tx_hash = signed_tx.hash.hex()
        logger.info(f"[{self.account.address}] Tx request sent (swap): {tx_hash} | nonce={nonce} | id={self.request_id}")
        self.nonce_by_request_id[self.request_id] = nonce
        self.request_id += 1


def run_in_parallel(objects):
    def start_and_wait(obj):
        obj.start()
    global EXECUTION_STARTED
    EXECUTION_STARTED = True
    start_time = time.time()
    logger.info(f"Start time: {start_time}")
    if len(objects) > 1:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(start_and_wait, obj) for obj in objects]
        for future in as_completed(futures):
            future.result()
    elif len(objects) == 1:
        start_and_wait(objects[0])
    end_time = time.time()
    logger.info(f"End time: {end_time}")


def wait_until_target_time():
    now = datetime.now()
    logger.info(f"Time now: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    target = now.replace(minute=now.minute - (now.minute % 5), second=0, microsecond=0) + timedelta(minutes=5)
    wait_secs = (target - datetime.now()).total_seconds()
    if wait_secs < 60:
        logger.info("Launch is too soon. Aborting...")
        sys.exit(0)
    logger.info(f"Scheduled at: {target.strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(wait_secs)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    # Parse -n argument:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, required=True, help='index of trader')
    args = parser.parse_args()
    trader_index = args.n
    # Initialize accounts:
    mnemonic = open("mnemonic.txt", "r").read()
    start_index = 10 * trader_index
    accounts = generate_ethereum_accounts(mnemonic, count=100)[start_index:start_index+10]
    objects = [Trader(ChainId.ZKSYNC_ERA_MAINNET, account, swap_txs_count=20) for account in accounts]
    # Execute in parallel:
    wait_until_target_time()
    run_in_parallel(objects)
