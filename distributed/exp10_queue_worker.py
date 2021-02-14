import os
import pika
import sys
import datetime
from timeit import default_timer as timer

import environ
from environ import *

from deeplens.constants import *
from deeplens.error import CorruptedOrMissingVideo
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import NullSplitter
from deeplens.struct import CustomTagger, Box
from deeplens.utils.testing_utils import get_size
from deeplens.utils.utils import get_local_ip


def fixed_tagger(vstream, batch_size):
    count = 0
    for frame in vstream:
        count += 1
        if count >= batch_size:
            break
    if count == 0:
        raise StopIteration("Iterator is closed")
    return {'label': 'foreground', 'bb': Box(1600, 1600, 2175, 1800)}


def runFullPut(src, batch_size=20):
    # local_folder = '/var/www/html/videos'
    # ip_addr = get_local_ip()
    # remote_folder = 'http://' + ip_addr + '/videos'
    local_folder = '/shared/db/'
    remote_folder = '/shared/db/'
    manager = FullStorageManager(CustomTagger(fixed_tagger, batch_size=batch_size), NullSplitter(),
                                 local_folder, remote_folder, dsn='dbname=header user=postgres password=deeplens host=10.0.0.4')

    def put():
        now = timer()
        manager.put(src, 'test',
                    args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1,
                          'batch_size': batch_size, 'num_processes': 4, 'background_scale': 1})
        put_time = timer() - now
        print("Put time for full:", put_time)
        # print("Batch size:", batch_size, "Folder size:", get_size(local_folder))

    put()


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.0.0.4', heartbeat=600))
    channel = connection.channel()

    channel.queue_declare(queue='deeplens')

    def callback(ch, method, properties, body):
        print(datetime.datetime.now(), "[x] Received %r" % body)
        runFullPut(body.decode("utf-8"), batch_size=72)
        print(datetime.datetime.now(), "[x] Done %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='deeplens', on_message_callback=callback, auto_ack=False)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
