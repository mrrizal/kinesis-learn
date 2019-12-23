import json
import argparse
from utils import get_client


def create_stream(stream_name, shard_count=1):
    client = get_client(service_name='kinesis')
    resp = client.create_stream(StreamName=stream_name, ShardCount=shard_count)
    return resp


def put_data(stream_name, partitionkey):
    client = get_client(service_name='kinesis')
    for i in range(50):
        data = {
            'name': 'user {}'.format(i + 1),
            'username': 'user{}'.format(i + 1)
        }
        resp = client.put_record(StreamName=stream_name,
                          Data=json.dumps(data),
                          PartitionKey=partitionkey)
        print(resp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='kinesis producer')
    parser.add_argument('--create-stream',
                        action='store_true',
                        help='create kinesis stream')
    parser.add_argument('--put-data',
                        action='store_true',
                        help='put sample data into kinesis stream')
    parser.add_argument('--stream-name', help='stream name')
    parser.add_argument('--shard-count',
                        default=1,
                        type=int,
                        help='number of shard')
    parser.add_argument('--partitionkey', help='paritition key')
    args = parser.parse_args()

    if args.stream_name is not None and args.create_stream:
        resp = create_stream(args.stream_name, args.shard_count)
        print(resp)

    conditions = [
        not args.create_stream,
        args.put_data,
        args.stream_name is not None,
        args.partitionkey is not None,
    ]

    if False not in conditions:
        put_data(args.stream_name, args.partitionkey)
