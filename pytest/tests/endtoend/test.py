#!/usr/bin/env python3
import argparse
import random
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import account as account_mod
import key as key_mod
import mocknet
import mocknet_helpers
from concurrent.futures import ThreadPoolExecutor

from configured_logger import logger


def watcher(master_account):
    while True:
        res = master_account.view_function_call('minimum', '')
        logger.info(
            f'Result of calling {master_account.key.account_id} method "minimum" is {res}'
        )
        time.sleep(10)


def pinger(account, interval, master_account_id, rpc_server):
    time.sleep(random.random() * interval)
    while True:
        base_block_hash = mocknet_helpers.get_latest_block_hash(
            addr=rpc_server[0], port=rpc_server[1])
        timestamp = int(time.time())
        args = f'{{"value": {timestamp} }}'
        logger.info(
            f'Calling function "set" with arguments {args} on account {account.key.account_id} contract {master_account_id}'
        )
        tx_res = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_call_contract_raw_tx(master_account_id,
                                                      'set',
                                                      args.encode('utf-8'),
                                                      0,
                                                      base_block_hash=
                                                      base_block_hash))
        logger.info(f'{account.key.account_id} set {timestamp}: {tx_res}')

        time.sleep(interval)


def run_tasks_in_parallel(tasks):
    with ThreadPoolExecutor() as executor:
        running_tasks = [executor.submit(task) for task in tasks]
        for running_task in running_tasks:
            running_task.result()


if __name__ == '__main__':
    logger.info('Starting end-to-end test.')
    parser = argparse.ArgumentParser(description='Run an end-to-end test')
    parser.add_argument('--project', required=True)
    parser.add_argument('--nodes', required=True)
    parser.add_argument('--accounts', required=True)
    parser.add_argument('--master-account', required=True)
    parser.add_argument('--interval-sec', type=float, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--public-key', required=True)
    parser.add_argument('--private-key', required=True)
    parser.add_argument('--rpc-server-addr', required=True)
    parser.add_argument('--rpc-server-port', required=True)

    args = parser.parse_args()

    project = args.project
    assert args.nodes
    nodes = args.nodes.split(',')
    assert nodes, 'Need at least one node'
    assert args.accounts
    account_ids = args.accounts.split(',')
    assert len(account_ids) == len(
        nodes), 'List of test accounts must match the list of nodes'
    master_account_id = args.master_account
    interval_sec = args.interval_sec
    assert interval_sec > 1, 'Need at least 1 second between pings'
    port = args.port
    pk, sk = args.public_key, args.private_key
    rpc_server = (args.rpc_server_addr, args.rpc_server_port)

    ips = [
        mocknet.get_nodes(pattern=node, project=project)[0].ip for node in nodes
    ]

    keys = [key_mod.Key(account_id, pk, sk) for account_id in account_ids]
    base_block_hash = mocknet_helpers.get_latest_block_hash(addr=rpc_server[0],
                                                            port=rpc_server[1])
    accounts = [
        account_mod.Account(
            keys[i],
            mocknet_helpers.get_nonce_for_pk(keys[i].account_id,
                                             keys[i].pk,
                                             addr=rpc_server[0],
                                             port=rpc_server[1]),
            base_block_hash,
            rpc_infos=[(ips[i], port)]) for i in range(len(keys))
    ]

    master_key = key_mod.Key(master_account_id, pk, sk)
    master_account = account_mod.Account(master_key,
                                         mocknet_helpers.get_nonce_for_pk(
                                             master_key.account_id,
                                             master_key.pk,
                                             addr=rpc_server[0],
                                             port=rpc_server[1]),
                                         base_block_hash,
                                         rpc_info=rpc_server)

    tasks = [
        lambda: pinger(account, interval_sec, master_account.key.account_id,
                       rpc_server) for account in accounts
    ]
    tasks.append(lambda: watcher(master_account))
    unreachable = run_tasks_in_parallel(tasks)
