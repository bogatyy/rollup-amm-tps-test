from concurrent.futures import ThreadPoolExecutor, as_completed
from eth_account import Account
from web3 import Web3

import json
import logging
import signal
import sys
import threading
import time

from blockchain import BlockchainData, ChainId, Contract, Token

logging.basicConfig(format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


NUM_ACCOUNTS = 100  # number of accounts to fund

WETH_ABI = json.loads(open('abis/WETH9.abi', 'r').read())


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


class Preparer:
    def __init__(self, chain_id: ChainId, account: Account):
        self.account = account
        # Initialize web3:
        self.blockchain = BlockchainData(chain_id)
        self.chain_id = chain_id.value
        self.w3 = Web3(Web3.HTTPProvider(self.blockchain.http_rpc_url()))
        self.nonce = self.w3.eth.get_transaction_count(account.address)
        weth_address = self.blockchain.get_address(Token.WETH)
        self.weth = self.w3.eth.contract(address=weth_address, abi=WETH_ABI)
        # Initialize variables:
        self.done_sending = False
        self.pending_tx_hashes = []
        # Thread to wait until all transactions are confirmed:
        self.waiting_thread = threading.Thread(target=self._wait)
        self.waiting_thread.start()

    @retriable
    def transfer_eth(self, to_address, amount_in_eth):
        tx = {
            'chainId': self.chain_id,
            'from': self.account.address,
            'to': to_address,
            'value': Web3.to_wei(amount_in_eth, 'ether'),
            'gas': 500000,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.nonce,
        }
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"[{self.account.address}] Tx sent (transfer_eth): {tx_hash.hex()} | nonce={self.nonce}")
        self.pending_tx_hashes.append(tx_hash.hex())
        self.nonce += 1
        return True

    @retriable
    def wrap_eth(self, amount_in_eth):
        tx = self.weth.functions.deposit().build_transaction({
            'chainId': self.chain_id,
            'from': self.account.address,
            'value': Web3.to_wei(amount_in_eth, 'ether'),
            'gas': 2000000,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.nonce,
        })
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"[{self.account.address}] Tx sent (wrap): {tx_hash.hex()} | nonce={self.nonce}")
        self.pending_tx_hashes.append(tx_hash.hex())
        self.nonce += 1
        return True

    @retriable
    def approve_token(self, token_contract, spender, amount_in_eth):
        amount_in_wei = Web3.to_wei(amount_in_eth, 'ether')
        tx = token_contract.functions.approve(spender, amount_in_wei).build_transaction({
            'chainId': self.chain_id,
            'from': self.account.address,
            'gas': 2000000,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.nonce,
        })
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        logger.info(f"[{self.account.address}] Tx sent (approve): {tx_hash.hex()} | nonce={self.nonce}")
        self.pending_tx_hashes.append(tx_hash.hex())
        self.nonce += 1
        return True

    def fund_accounts(self):
        mnemonic = open("mnemonic.txt", "r").read()
        accounts = generate_ethereum_accounts(mnemonic, NUM_ACCOUNTS)
        for account in accounts:
            self.transfer_eth(account.address, 0.002)

    def start(self):
        # logger.info(f'[{self.account.address}] Balance: {self.w3.eth.get_balance(self.account.address) / 1e18}')
        # logger.info(f'[{self.account.address}] WETH Balance: {self.weth.functions.balanceOf(self.account.address).call() / 1e18}')
        # 1. Fund all accounts:
        self.fund_accounts()
        # 2. Each account has to wrap enough WETH for swaps (each swap requires 1e-9 WETH)
        # self.wrap_eth(5e-8)
        # 3. Each account has to approve WETH spending to SMART_ROUTER
        # smart_router_address = self.blockchain.get_address(Contract.PANCAKE_SMART_ROUTER)
        # self.approve_token(self.weth, smart_router_address, 1)
        # 4. Check readiness:
        # logger.info(f'[{self.account.address}] Ready: {self.is_ready()}')
        self.done_sending = True

    def is_ready(self):
        smart_router_address = self.blockchain.get_address(Contract.PANCAKE_SMART_ROUTER)
        eth_balance = self.w3.eth.get_balance(self.account.address) / 1e18
        weth_balance = self.weth.functions.balanceOf(self.account.address).call() / 1e18
        weth_allowance = self.weth.functions.allowance(self.account.address, smart_router_address).call() / 1e18
        return eth_balance >= 0.001 and weth_balance >= 2e-8 and weth_allowance >= 2e-8

    def wait_for_transaction_receipt(self, tx_hash):
        retry_secs = 0.5
        max_retries = 20
        retry_count = 0
        while not TERMINATION_REQUESTED:
            try:
                return self.w3.eth.wait_for_transaction_receipt(tx_hash, poll_latency=0.5)
            except Exception as e:
                if retry_count > max_retries:
                    return False
                retry_count += 1
                logger.info(f"Failed to execute wait_for_transaction_receipt: {e}. Retry #{retry_count}")
                time.sleep(retry_secs)
        return False

    def _wait(self):
        while not self.done_sending:
            time.sleep(0.2)
        if len(self.pending_tx_hashes) == 0:
            return
        last_tx_hash = self.pending_tx_hashes[-1]
        receipt = self.wait_for_transaction_receipt(last_tx_hash)
        if not receipt:
            logger.info(f"[{self.account.address}] Unable to confirm all txs")
            return
        block_num = receipt['blockNumber']
        tx_status = 'SUCCESS' if receipt['status'] == 1 else 'FAILED'
        logger.info(f"[{self.account.address}] Tx confirmed: {last_tx_hash} | Block: {block_num} | Status: {tx_status}")

    def wait(self):
        if hasattr(self, 'waiting_thread') and self.waiting_thread:
            self.waiting_thread.join()


def run_in_parallel(objects):
    def start_and_wait(obj):
        obj.start()
        obj.wait()
        return "Done"
    global EXECUTION_STARTED
    EXECUTION_STARTED = True
    if len(objects) > 1:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(start_and_wait, obj) for obj in objects]
        for future in as_completed(futures):
            future.result()
    elif len(objects) == 1:
        start_and_wait(objects[0])


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    funder = Account.from_key("<PRIVATE_KEY>")
    mnemonic = open("mnemonic.txt", "r").read()
    accounts = generate_ethereum_accounts(mnemonic, count=NUM_ACCOUNTS)
    # 1. Fund all accounts:
    objects = [Preparer(ChainId.ZKSYNC_ERA_MAINNET, funder)]
    # 2. Each account has to wrap enough WETH for swaps (each swap requires 1e-9 WETH)
    # 3. Each account has to approve WETH spending to SMART_ROUTER
    # 4. Check readiness
    # objects = [Preparer(ChainId.ZKSYNC_ERA_MAINNET, account) for account in accounts]

    # Execute in parallel:
    run_in_parallel(objects)
