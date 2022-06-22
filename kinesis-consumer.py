import time
import argparse
import json
from pprint import pprint
from utils import get_client


def get_data(stream_name):
    client = get_client(service_name='kinesis')
    for shard in client.list_shards(StreamName=stream_name)['Shards']:
        pre_shard_it = client.get_shard_iterator(StreamName=stream_name,
                                                 ShardId=shard['ShardId'],
                                                 ShardIteratorType='LATEST')
        shard_it = pre_shard_it['ShardIterator']
        while True:
            out = client.get_records(ShardIterator=shard_it, Limit=5)
            shard_it = out['NextShardIterator']
            records = out.get("Records")
            print(records)
            time.sleep(1.0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='kinesis simple consumer')
    parser.add_argument('--stream-name')
    args = parser.parse_args()

    if args.stream_name is not None:
        get_data(args.stream_name)
