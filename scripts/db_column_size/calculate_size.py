import argparse
import os
import prometheus_client
import subprocess
from collections import defaultdict
from time import sleep


DUMP_FILENAME = 'dump_result'
COLUMNS_FILENAME = 'db.rs'
METRICS_PORT = 4404


def compute_dump(args):
    subprocess.call(('sudo', 'bash', '-x',
                     args.script, args.data_dir, args.res_dir))


def get_col_to_name(column_file):
    lines = open(column_file).readlines()
    col_to_name = {}
    for line in lines:
        parts = line.split()
        if parts and parts[-1]:
            last = parts[-1][:-1]
            if last.isnumeric() and 'col' + last not in col_to_name:
                col_to_name['col' + last] = parts[0]
    return col_to_name


def calculate_size(dump_file):
    lines = open(dump_file).readlines()
    size = defaultdict(int)

    data = None
    for line in lines:
        if "table size" in line:
            data = line.split()[-1]
        if "column family name" in line:
            name = line.split()[-1]
            size[name] += int(data) / 1024 / 1024 / 1024
            data = None

    return size


def write_size(size, metrics):
    for col, value in size.items():
        metrics[col].set(value)


def main(args, metrics):
    compute_dump(args)
    size = calculate_size(os.path.join(args.res_dir, DUMP_FILENAME))
    write_size(size, metrics)


def setup_prometheus(args):
    metrics = {}

    col_to_name = get_col_to_name(os.path.join(args.res_dir, COLUMNS_FILENAME))
    for col, name in col_to_name.items():
        if 'Col' in name:
            metrics[col] =\
                prometheus_client.Gauge(name, '{} size'.format(name))

    return metrics


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collect column sizes')
    parser.add_argument('--data-dir', required=True, type=str)
    parser.add_argument('--res-dir', required=True, type=str)
    parser.add_argument('--script', required=True, type=str)
    args = parser.parse_args()

    # to get db.rs file
    compute_dump(args)

    prometheus_client.start_http_server(METRICS_PORT)
    metrics = setup_prometheus(args)

    while True:
        main(args, metrics)
        mins = 10
        print('Waiting {} minutes'.format(mins))
        sleep(mins * 60)
