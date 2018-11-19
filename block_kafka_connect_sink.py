import argparse
import logging
import os
import json
import mysql
from mysql.connector import errorcode

import logging
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

from kafka.client import KafkaClient
from cassandra.policies import DCAwareRoundRobinPolicy
import datetime
import time
import sys
from datetime import datetime
import pprint
import gc
from pdb import set_trace

import findspark
findspark.init('/usr/local/spark/spark-2.3.2-bin-hadoop2.7')

import pyspark
from pyspark.sql.functions import *
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.conf import SparkConf

SparkContext.setSystemProperty('spark.executor.memory', '3g')

'''
conf = SparkConf()\
    .setAppName('map')\
    .set('executor.memory', '3g')\
    .set('spark.driver.memory','3g')

'''


from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

class PythonCassandra:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    # How about Adding some log info to see what went wrong
    def setlogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    # Create Keyspace based on Given Name
    def createkeyspace(self, keyspace):
        """
        :param keyspace:  The Name of Keyspace to be created
        :return:
        """
        # Before we create new lets check if exiting keyspace; we will drop that and create new
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspace_exists = False
        if keyspace in [row[0] for row in rows]:
            keyspace_exists = True
        if keyspace_exists is False:
            # self.log.info("dropping existing keyspace...")
            # self.session.execute("DROP KEYSPACE " + keyspace)
            self.log.info("creating keyspace...")
            self.session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
                """ % keyspace)
            self.log.info("setting keyspace...")
        self.session.set_keyspace(keyspace)

    def create_table_block(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS block (block_number bigint, block_hash varchar,
                                              miner_address varchar, parent_hash varchar, receipt_tx_root varchar,
                                              state_root varchar, tx_trie_root varchar, extra_data varchar, 
                                              nonce varchar, bloom varchar, solution varchar, difficulty varchar, 
                                              total_difficulty varchar, nrg_consumed bigint, nrg_limit bigint,
                                              block_size bigint, block_timestamp bigint, num_transactions bigint,
                                              block_time bigint, nrg_reward varchar, transaction_id bigint,
                                              transaction_list varchar,
                                              PRIMARY KEY (block_timestamp,block_number)
                                              );
                 """
        self.session.execute(c_sql)
        self.log.info("Block Table Created !!!")
    
    

    
    def create_table_transaction(self):
        c_sql = """
                CREATE TABLE IF NOT EXISTS transaction (id bigint,
                                              transaction_hash varchar, block_hash varchar, block_number bigint,
                                              transaction_index bigint, from_addr varchar, to_addr varchar, 
                                              nrg_consumed bigint, nrg_price bigint, transaction_timestamp bigint,
                                              block_timestamp bigint, tx_value varchar, transaction_log varchar,
                                              tx_data varchar, nonce varchar, tx_error varchar, contract_addr varchar,
                                              PRIMARY KEY (block_timestamp,block_number,transaction_timestamp)
                                              );
                 """
        self.session.execute(c_sql)
        self.log.info("Transaction Table Created !!!")

    def insert_data_transaction(self, message):
        insert_sql = self.session.prepare(
            """ INSERT INTO transaction(
            id,transaction_hash, block_hash, block_number,
            transaction_index, from_addr, to_addr, 
            nrg_consumed, nrg_price, transaction_timestamp,
            block_timestamp, tx_value, transaction_log,
            tx_data, nonce, tx_error, contract_addr)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
        )
        batch = BatchStatement()
        # batch.add(insert_sql, (1, 'LyubovK'))
        batch.add(insert_sql, message)
        self.session.execute(batch)
        self.log.info('Batch Insert Completed')

    # message is a list
    def insert_data_block(self, messages):
        insert_sql = self.session.prepare("""
                                            INSERT INTO block(block_number, block_hash, miner_address, 
                                            parent_hash, receipt_tx_root,
                                            state_root, tx_trie_root, extra_data, 
                                            nonce, bloom, solution, difficulty, 
                                            total_difficulty, nrg_consumed, nrg_limit,
                                            size, block_timestamp, num_transactions,
                                            block_time, nrg_reward, transaction_id, transaction_list) 
                                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                            """)
        
        
        
        batch = BatchStatement()
        for message in messages:
            batch.add(insert_sql, message)

        self.session.execute(batch)
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print('###############################  DATA INSERTED ########################################')
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        self.log.info('Block Batch Insert Completed')

    def select_data(self, table):
        rows = self.session.execute('select * from ' + table)
        for row in rows:
            print(row)

    def update_data(self):
        pass

    def delete_data(self):
        pass

class KafkaConnectPyspark:
    def __init__(self):
        sparkContext = SparkContext()
        spark = SparkSession \
            .builder \
            .appName('poolminer') \
            .master('local[2]') \
            .getOrCreate()

        self.ssc = StreamingContext(sparkContext,.5) #
        self.ssc.checkpoint('sparkcheckpoint')
        self.sqlContext = SQLContext(sparkContext)
        self.kafkaStream=None
        self.query=None


    def connect(self):
        def toTuple(taken,pc):
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            for mess in taken:
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                message = (mess["block_number"], mess["block_hash"], mess["miner_address"],
                           mess["parent_hash"], mess["receipt_tx_root"],
                           mess["state_root"], mess["tx_trie_root"], mess["extra_data"],
                           mess["nonce"], mess["bloom"], mess["solution"], mess["difficulty"],
                           mess["total_difficulty"], mess["nrg_consumed"], mess["nrg_limit"],
                           mess["size"], mess["block_timestamp"], mess["num_transactions"],
                           mess["block_time"], mess["nrg_reward"], mess["transaction_id"], mess["transaction_list"])
                print(message)
                pc.insert_data_block([message])

                del message
                del mess
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        def f(rdd):
            if rdd is None:
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print('################  RDD IS NONE')
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                return
            '''
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode('append') \
                .options(table="block", keyspace="aionv4") \
                .save()

            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print('Datawritten for rdd')
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

            '''

            message_list = list()
            # cassandra setup
            pc = PythonCassandra()
            pc.setlogger()
            pc.createsession()
            pc.createkeyspace('aionv4')
            pc.create_table_block()
            taken = rdd.take(20000)
            toTuple(taken,pc)


        topic='aionv4server.aionv4.block'
        kafkaParams = {"metadata.broker.list": "localhost:9092",
                       "auto.offset.reset": "smallest"}
        kafkaStream = KafkaUtils.createDirectStream(self.ssc,
                                                         [topic], kafkaParams)
        kafkaStream.checkpoint(30)

        kafkaStream = kafkaStream.map(lambda x: json.loads(x[1]))
        kafkaStream = kafkaStream.map(lambda x: x['payload']['after'])
        #kafkaStream.pprint()

        kafkaStream.foreachRDD(f)

        self.ssc.start()
        self.ssc.awaitTermination()

    def struct_block(self):
        string_columns = ['block_hash', 'miner_address', 'parent_hash', 'receipt_tx_root',
                          'state_root', 'tx_trie_root', 'extra_data',
                          'nonce', 'bloom', 'solution', 'difficulty',
                          'total_difficulty', 'transaction_list']

        int_columns = [
            'block_number','block_size', 'nrg_consumed', 'nrg_limit',
            'block_timestamp', 'num_transactions',
            'block_time', 'nrg_reward varchar', 'transaction_id']

        columns = [
            'block_number', 'block_hash', 'miner_address',
            'parent_hash', 'receipt_tx_root',
            'state_root', 'tx_trie_root', 'extra_data',
            'nonce', 'bloom', 'solution', 'difficulty',
            'total_difficulty', 'nrg_consumed', 'nrg_limit',
            'size', 'block_timestamp', 'num_transactions',
            'block_time', 'nrg_reward', 'transaction_id', 'transaction_list'
        ]
        '''
        columns_struct_fields = [StructField('block_number', DoubleType(), True)]
        columns_struct_fields.append(StructField('block_hash', StringType(), True))
        columns_struct_fields.append(StructField('miner_address', DoubleType(), True))
        columns_struct_fields.append(StructField('parent_hash', StringType(), True))
        columns_struct_fields.append(StructField('receipt_tx_root', StringType(), True))
        columns_struct_fields.append(StructField('state_root', StringType(), True))
        columns_struct_fields.append(StructField('tx_trie_root', StringType(), True))
        columns_struct_fields.append(StructField('extra_data', StringType(), True))
        columns_struct_fields.append(StructField('nonce', StringType(), True))
        columns_struct_fields.append(StructField('bloom', StringType(), True))
        columns_struct_fields.append(StructField('solution', StringType(), True))
        columns_struct_fields.append(StructField('difficulty', StringType(), True))
        columns_struct_fields.append(StructField('total_difficulty', StringType(), True))
        columns_struct_fields.append(StructField('nrg_consumed', DoubleType(), True))
        columns_struct_fields.append(StructField('nrg_limit', DoubleType(), True))
        columns_struct_fields.append(StructField('size', DoubleType(), True))
        columns_struct_fields.append(StructField('block_timestamp', DoubleType(), True))
        columns_struct_fields.append(StructField('num_transactions', DoubleType(), True))
        columns_struct_fields.append(StructField('block_time', DoubleType(), True))
        columns_struct_fields.append(StructField('nrg_reward', DoubleType(), True))
        columns_struct_fields.append(StructField('transaction_id', DoubleType(), True))
        columns_struct_fields.append(StructField('transaction_list', StringType(), True))
        '''


        schema3 = StructType([
            StructField('block_number', DoubleType(), True),
            StructField('block_hash', StringType(), True),
            StructField('miner_address', StringType(), True),
            StructField('parent_hash', StringType(),True),
            StructField('receipt_tx_root', StringType(), True),
            StructField('state_root', StringType(), True),
            StructField('tx_trie_root', StringType(), True),
            StructField('extra_data', StringType(), True),
            StructField('nonce', StringType(), True),
            StructField('bloom', StringType(), True),
            StructField('solution', StringType(), True),
            StructField('difficulty', StringType(), True),
            StructField('total_difficulty', StringType(), True),
            StructField('nrg_consumed', DoubleType(), True),
            StructField('nrg_limit', DoubleType(), True),
            StructField('size', DoubleType(), True),
            StructField('block_timestamp', DoubleType(), True),
            StructField('num_transactions', DoubleType(), True),
            StructField('block_time', DoubleType(), True),
            StructField('nrg_reward', DoubleType(), True),
            StructField('transaction_id', DoubleType(), True),
            StructField('transaction_list', StringType(), True)
        ])

        schema2 = StructType([
            StructField('before', StringType()),
            StructField('after', schema3),
            StructField('op',StringType()),
            StructField('source',DataType()),
            StructField('ts_ms', DoubleType())
        ])

        schema1 = StructType([
            StructField('schema', StructType()),
            StructField('payload',schema2)
        ])

        return schema3

    def connect1(self):

        # parse the raw data
        df = spark\
            .readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers','localhost:9092')\
            .option('subscribe','aionv4server.aionv4.block')\
            .option('startingOffsets','earliest')\
            .load()

        schemas = self.struct_block()
        df = df.selectExpr("CAST(value AS STRING) as json")
        df1 = df.select(from_json('json',))
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        df.printSchema()
        df1.printSchema()
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++')


        query = df1.writeStream\
            .format('console')\
            .option('truncate','false')\
            .option('numRows',2)\
            .option('checkpointLocation', 'sparkcheckpoint')\

        query.start()

        query.awaitTermination()


kcp = KafkaConnectPyspark()
kcp.connect()
