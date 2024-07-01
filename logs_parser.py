from datetime import datetime
from web3 import Web3
import asyncio
import json
import random
import time
import websockets

from blockchain import BlockchainData, ChainId


def request_to_json(method, params, request_id=None):
    if request_id is None:
        request_id = random.randint(0, int(1e9))
    return {
        'jsonrpc': '2.0',
        'id': request_id,
        'method': method,
        'params': params,
    }


async def _get_block_numbers_by_tx_hash(tx_hashes, ws_url):
    block_num_by_tx_hash = {}
    tx_hash_by_request_id = {}
    tx_hashes_left = set(tx_hashes)
    request_id = 1
    while len(block_num_by_tx_hash) != len(tx_hashes):
        async with websockets.connect(ws_url) as ws:
            while len(block_num_by_tx_hash) != len(tx_hashes):
                txs_in_batch = min(80, len(tx_hashes_left))
                # Send requests in batch:
                requests_left = txs_in_batch
                for tx_hash in tx_hashes_left:
                    json_request = request_to_json("eth_getTransactionByHash", [tx_hash], request_id=request_id)
                    await ws.send(json.dumps(json_request))
                    tx_hash_by_request_id[request_id] = tx_hash
                    request_id += 1
                    requests_left -= 1
                    if requests_left == 0:
                        break
                    time.sleep(0.01)
                # Recv responses in batch:
                for _ in range(txs_in_batch):
                    message = await ws.recv()
                    json_response = json.loads(message)
                    if "result" in json_response:
                        result = json_response["result"]
                        tx_hash = tx_hash_by_request_id[json_response["id"]]
                        tx_hashes_left.remove(tx_hash)
                        block_num_by_tx_hash[tx_hash] = int(result["blockNumber"], 16)
                time.sleep(1)
            time.sleep(1)
    return block_num_by_tx_hash


def get_block_numbers_by_tx_hash(tx_hashes, ws_url):
    return asyncio.run(_get_block_numbers_by_tx_hash(tx_hashes, ws_url))


def parse_combined_logs(filename, chain_id):
    blockchain = BlockchainData(chain_id)
    http_rpc_url = blockchain.http_rpc_url()
    ws_rpc_url = blockchain.ws_rpc_url()
    w3 = Web3(Web3.HTTPProvider(http_rpc_url))
    lines = open(filename, 'r').readlines()
    tx_last_sent_at = {}
    tx_sent_at = {}
    blocks = {}
    for line in lines:
        if "Tx request accepted (swap): " in line:
            tx_hash = line.split(" |")[0].split("Tx request accepted (swap): ")[1]
            tx_sent_at[tx_hash] = tx_last_sent_at[tx_hash]
        elif "Tx request sent (swap): " in line:
            tx_hash = line.split(" |")[0].split("Tx request sent (swap): ")[1]
            dt_str = line.split("]")[0][1:]
            dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = dt_obj.timestamp()
            tx_last_sent_at[tx_hash] = timestamp
    our_txs = [tx for (tx, ts) in sorted(tx_sent_at.items(), key=lambda x: x[1])]
    block_num_by_tx_hash = get_block_numbers_by_tx_hash(our_txs, ws_rpc_url)
    for tx_hash in our_txs:
        block_number = block_num_by_tx_hash[tx_hash]
        if block_number is not None and block_number not in blocks:
            blocks[block_number] = w3.eth.get_block(block_number)
        block = blocks[block_number] if block_number is not None else None
        block_timestamp = block['timestamp'] if block_number is not None else None
        block_txs_cnt = len(block['transactions'] if block_number is not None else [])
        print(f"sent_at={tx_sent_at[tx_hash]} hash={tx_hash} block_num={block_number} block_timestamp={block_timestamp} block_all_txs={block_txs_cnt}")


def parse_swaps(filename):
    lines = open(filename, 'r').readlines()
    # Fill tx_infos and blocks:
    tx_infos = []
    block_num_by_ts = {}
    block_all_txs_by_ts = {}
    block_our_txs_by_ts = {}  # block_ts -> # of our included txs in that block
    for line in lines:
        current_ts = float(line.split('sent_at=')[1].split(" ")[0])
        tx_hash = line.split('hash=')[1].split(" ")[0]
        block_ts_str = line.split('block_timestamp=')[1].split(" ")[0]
        block_num_str = line.split('block_num=')[1].split(" ")[0]
        block_all_txs_str = line.split('block_all_txs=')[1].split(" ")[0]
        if "None" in block_ts_str:
            continue
        block_ts = int(block_ts_str)
        block_num_by_ts[block_ts] = block_num_str
        block_all_txs_by_ts[block_ts] = int(block_all_txs_str)
        block_our_txs_by_ts[block_ts] = (block_our_txs_by_ts.get(block_ts) or 0) + 1
        tx_infos.append({
            "tx_hash": tx_hash,
            "sent_ts": current_ts,
            "block_ts": block_ts,
            "block_num": int(block_num_str),
        })
    # Sort blocks:
    sorted_blocks = sorted(block_our_txs_by_ts.items(), key=lambda x: x[0])
    # TPS estimate:
    BLOCK_TX_OFFSET_SECS = 1.5
    (min_block_ts, max_block_ts) = (sorted_blocks[0][0], sorted_blocks[-1][0])
    total_secs = (max_block_ts - min_block_ts)
    tps_str = f"{len(tx_infos) / total_secs :.2f}" if total_secs > 0 else "∞"
    print(f"TPS: {tps_str} txs/s ({len(tx_infos)} txs in {total_secs}s)")
    # For blocks in ASC order provide txs included and cumulative infromation
    cum_txs = 0
    for (block_ts, block_our_txs) in sorted_blocks:
        block_num = block_num_by_ts[block_ts]
        block_all_txs = block_all_txs_by_ts[block_ts]
        tx_sent_by_that_time = len(list(filter(lambda tx: tx['sent_ts'] < block_ts + BLOCK_TX_OFFSET_SECS, tx_infos)))  # approx
        cum_txs += block_our_txs
        elapsed_secs = block_ts - min_block_ts
        print(f"Block #{block_num} with ts={block_ts} | all_txs_in_block={block_all_txs:4}, our_txs_in_block={block_our_txs:4}, " + \
              f"~tx_sent_by_that_time={tx_sent_by_that_time:4}, cum_txs_confirmed={cum_txs:4}, " + \
              f"cum_elapsed_secs={elapsed_secs:3} | cum_tps={'-' if elapsed_secs == 0 else f'{cum_txs / elapsed_secs:.2f}'}")


if __name__ == '__main__':
    # 1. Combine all logs into one file:
    # cat tps0{0..9}.log > tps.log

    # 2. Create swaps.log: all swaps sorted by timestamp with block information
    # python3 logs_parser.py > logs/swaps.log
    parse_combined_logs('logs/tps.log', ChainId.ZKSYNC_ERA_MAINNET)

    # 3. Create tps-results.log: all blocks sorted by timestamp with infromation of txs included
    # python3 logs_parser.py > logs/tps-results.log
    # parse_swaps('logs/swaps.log')
    pass
