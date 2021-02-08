import os
import pika
import sys

import environ

from deeplens.utils.utils import get_local_ip


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='deeplens')

# call ffmpeg to partition video into blocks
blocks = ['./brooklyn_720.mp4']
# output: blocks = ['http://10.0.0.4/vid1.mp4, 'http://10.0.0.4/vid2.mp4', ...]

for url in blocks:
    channel.basic_publish(exchange='', routing_key='deeplens', body=url)
    print(" [x] Sent " + url)
connection.close()
