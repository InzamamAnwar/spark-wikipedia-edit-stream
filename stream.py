import os

os.environ["PYWIKIBOT_NO_USER_CONFIG"] = "1"

import socket
import time
import json

import argparse
from datetime import datetime
from pywikibot.comms.eventstreams import EventStreams
from random import randint


def get_stream(stream_date, stream_server):
    stream = EventStreams(streams=["recentchange", "revision-create"], since=stream_date)
    stream.register_filter(server_name=stream_server, type="edit")
    return stream


def get_json(change):
    end_list = [{
        "id": change["id"],
        "title": change["title"],
        "user": change["user"],
        "bot": change["bot"],
        "length": change["length"]["new"] - change["length"]["old"],
        "wiki": change["wiki"],
        "timestamp": datetime.fromtimestamp(change["timestamp"]).strftime("%m/%d/%Y, %H:%M:%S")
    }]
    return (json.dumps(end_list) + "\n")


parser = argparse.ArgumentParser()
parser.add_argument("stream_name", help="provide stream name e.g. 'en.wikipedia.com'", type=str)
parser.add_argument("port", help="enter port name e.g. 5050", type=int)
parser.add_argument("date", help="enter date for edit stream, 'YYYYMMDD'", type=str)

args = parser.parse_args()

# ip of the host
host = '0.0.0.0'

port = args.port

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((host, port))
s.listen(1)

stream = get_stream(args.date, args.stream_name)
get_json(next(iter(stream)))

while True:
    conn, addr = s.accept()
    print("Client connection accepted ", addr)
    while True:
        try:
            data = get_json(next(iter(stream))).encode()
            conn.send(data)

            # this can be variable, change with discretion
            time.sleep(0.5)
        except socket.error as msg:
            print("client connection closed", addr)
            break

conn.close()

