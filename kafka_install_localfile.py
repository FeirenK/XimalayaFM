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


def kafka_local_file(opticons=None,broker='',group='',topics=''):

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
    # hdfs login
    #client = hdfs.Client('http://%s:50070' % (hdfshost))
    # client = InsecureClient('http://%s:50070' % (hdfshost),user='hadoop')
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
                type = msgdict.get('type').encode()
                if type == 'insert':
                    data = msgdict.get('data')
                    datalist = data.values()
                    datastr = ','.join('%s' % id for id in datalist).encode()
                    hour = time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))
                    localfile = '/mnt/var/%s.%s.%s.%s' % (database,table,type,hour)
                    sys.stderr.write('%s %s [%d] at offset %d with key %s:\n' % (logtime,msg.topic(),msg.partition(),msg.offset(),msgstr))

                    with open(localfile,'a') as writer:
                        writer.write(datastr+'\n')
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
    kafka_local_file()



#!/bin/bash
## hive load local
hour=$(date +%F-%H)
#file=hft.hft_trade.insert.2018-04-25-10
dir="/mnt/var"
hourfile=$(ls $dir/*.${hour})
#hourfile=$(ls $dir/*.2018-04-26-02)
loadfile="$dir/hiveload_$hour"
logfile="$dir/hiveload.log"
#echo $hourfile

for i in $hourfile;do
   db=$(echo $i|cut -c 10-|awk -F'[.]' "{print \$1}")
   table=$(echo $i|cut -c 10-|awk -F'[.]' "{print \$2}")
   echo "LOAD DATA LOCAL INPATH '$i' INTO TABLE $db.$table;" >> $loadfile
done

#hive -f $loadfile >> $logfile 2>&1
beeline -u "jdbc:hive2://127.0.0.1:10000/hft" -f $loadfile >> $logfile 2>&1
# LOAD DATA LOCAL INPATH '/mnt/var/test_details.txt' INTO TABLE hft.table;
# LOAD DATA INPATH '/home/hadoop/sqoop_mysql/test_details.txt' INTO TABLE hft.table;


#!/bin/bash
# current_time=$(date -d "$(date +'%Y-%m-%d %H:%M:%S')" +%s)
current_time=$(date +%s)
starttime=$(expr $current_time - 7200)000
endtime=$(expr $current_time - 3600)000
cubesname="hft_order_view_5_cube"
logfile="/var/log/kylin_cron_cube.log"
#command=

for cubename in $cubesname;do
# curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"startTime":'1423526400000', "endTime":'1524702900000', "buildType":"BUILD"}' http://52.53.238.57:7070/kylin/api/cubes/hft_order_view_5_cube/build
    echo "$cubename, $current_time $starttime, $endtime" > $logfile
    curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"startTime":'${starttime}', "endTime":'${endtime}', "buildType":"BUILD"}' http://127.0.0.1:7070/kylin/api/cubes/$cubename/build | python -m json.tool >> $logfile

done


curl -X GET -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' http://127.0.0.1:7070/kylin/api/cubes/hft_order_view_5_cube | python -m json.tool
