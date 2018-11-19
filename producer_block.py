import argparse
import logging
import os
import json
import mysql
from kafka import KafkaConsumer, KafkaProducer
from mysql.connector import errorcode

import logging
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import datetime
import time

mysql_configs = {
  'user': 'admin1',
  'password': 'password',
  'host': '127.0.0.1',
  'database': 'aionv4',
  'raise_on_warnings': True,
}

def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**mysql_configs)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            print(err)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
            return;
    else:
        return conn;

def get_blocks():
    conn = connect_to_mysql()
    if None != conn:
        cursor = conn.cursor()
        sql = "SELECT * FROM block"
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows;
        conn.close()


def send_produced_messages(messages_list):
   # return
    producer = KafkaProducer(bootstrap_servers='localhost:9092',api_version=(0, 10, 1),
                             batch_size=16384)
    topic='aionv4_block'
    for message in messages_list:
        # print 'Sending message: %s' % message;
        # producer.send('test', b'%s' % message)
        # Block until a single message is sent (or timeout)
        future = producer.send(topic, json.dumps(message).encode('utf-8'))
        print("producer_block_block_number:{}".format(message[1]))
        result = future.get(timeout=30)

        
# SET UP PRODUCER
logger = logging.getLogger(__name__)


# USE PRODUCEER
producer_list = list()
rows = get_blocks()

if producer_list is not None:
    for r in rows:
        producer_list.append(r)
        send_produced_messages(producer_list)