#!/usr/bin/env python
#encoding:utf8
'''
kafka data use python change to sql,and  install hive
'''
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import Producer
import sys,time
import getopt
import json
import logging
from pprint import pformat
from collections import OrderedDict
# import hdfs
# from hdfs import InsecureClient
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


def kafka_consumer_producer(opticons=None,broker='',group='',topics=''):

    broker = argv[0]
    group = argv[1]
    topics = argv[2:]
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    # Check to see if -T option exists
    for opt in optlist:
        if opt[0] != '-T':
            continue
        try:
            intval = int(opt[1])
        except ValueError:
            sys.stderr.write("Invalid option value for -T: %s\n" % opt[1])
            sys.exit(1)

        if intval <= 0:
            sys.stderr.write("-T option value needs to be larger than zero: %s\n" % opt[1])
            sys.exit(1)

        conf['stats_cb'] = stats_cb
        conf['statistics.interval.ms'] = int(opt[1])

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)
    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    p = Producer({'bootstrap.servers': broker})
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

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
                type = msgdict.get('type').encode()
                if type == 'insert':
                    data = msgdict.get('data')
                    datalist = data.values()
                    datastr = ','.join('%s' % id for id in datalist).encode()
                    # hour = time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))
                    # localfile = '/mnt/var/%s.%s.%s.%s' % (database,table,type,hour)
                    sys.stderr.write('%s %s [%d] at offset %d with key %s:\n' % (logtime,msg.topic(),msg.partition(),msg.offset(),msgstr))

                    try:
                        p.produce(table, datastr, callback=delivery_callback)
                    except BufferError as e:
                        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
                    # p.poll(0)
                    # p.flush()
                else:
                    sys.stderr.write('%s %s [%d] at offset %d with key %s:\n' % (logtime,msg.topic(),msg.partition(),msg.offset(),type))

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()

if __name__ == '__main__':
    optlist, argv = getopt.getopt(sys.argv[1:], 'T:')
    if len(argv) < 3:
        print_usage_and_exit(sys.argv[0])
    # hdfshost = argv[0]
    # client = InsecureClient('http://%s:50070' % (hdfshost),user='hadoop')
    kafka_consumer_producer()
