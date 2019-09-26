#!/usr/bin/env python
#encoding:utf8
'''
kafka data use python change to sql,and  install hive
'''
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys,time
import getopt
import json
import logging
from pprint import pformat
from collections import OrderedDict

#use hive cli
# from pyhive import hive
from TCLIService.ttypes import TOperationState
#? UnicodeDecodeError: 'ascii' codec can't decode byte
reload(sys)
sys.setdefaultencoding('utf-8')
##

def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <hdfshost> <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


def insert_local_file(database,table,datastr):
    hour = time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))
    localfile = '/mnt/var/%s.%s.%s' % (database,table,hour)
    with open(localfile,'a') as writer:
        writer.write(datastr+'\n')

def insert_hive_file(database,table,datastr,client):
    day = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    hivefile = '/user/hive/warehouse/{database}.db/{table}/{table}-{day}'.format(database=database,table=table,day=day)
    try:
        with client.write(hivefile, append=True, encoding='utf-8') as writer:
            writer.write(datastr+'\n')
            # json.dump(data, writer)
    except Exception, e:
        with client.write(hivefile, encoding='utf-8') as writer:
            writer.write(datastr+'\n')

def kafka_msg(broker,group,topic,*args):
    hdfshost = args[0]
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    # conf['statistics.interval.ms'] = int(opt[1])

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)
    # Subscribe to topic
    c.subscribe([topic])
    # hdfs login
    if hdfshost:
        from hdfs import InsecureClient
        client = InsecureClient('http://%s:50070' % (hdfshost),user='hadoop')
    # Read messages from Kafka, print to stdout

    try:
        while True:
            logtime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%s %s [%d] reached end at offset %d\n' %
                                     (logtime, msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                msgstr = msg.value().decode('utf-8')
                msgdict = json.loads(msgstr,encoding="uft-8",object_pairs_hook=OrderedDict)

                database = msgdict.get('database').encode()
                table = msgdict.get('table').encode()
                sqltype = msgdict.get('type').encode()
                if sqltype == 'insert':
                    data = msgdict.get('data')
                    datalist = data.values()
                    datastr = ','.join('%s' % id for id in datalist).encode()
                    sys.stderr.write('%s %s [%d] at offset %d with key %s data %s:\n' % (logtime,msg.topic(),msg.partition(),msg.offset(),msg.key(), msgstr))

                    if hdfshost:
                        insert_hive_file(database,table,datastr,client)
                    else:
                        insert_local_file(database,table,datastr)
                else:
                    sys.stderr.write('%s %s [%d] at offset %d with key %s:\n' % (logtime,msg.topic(),msg.partition(),msg.offset(),sqltype))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    # Close down consumer to commit final offsets.
    c.close()

def usage():
    print "Usage: " + sys.argv[0]+ " [broker] [consumer_group] [topic] [hdfshost]"
    exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 3:
        usage()
    broker = args[1]
    consumer_group = args[2]
    topic = args[3]

    if len(args) > 4:
        hdfshost = args[4]
        kafka_msg(broker,consumer_group,topic,hdfshost)
    else:
        kafka_msg(broker,consumer_group,topic,'')


