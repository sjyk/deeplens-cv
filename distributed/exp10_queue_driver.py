import os
import sys
import pika
import subprocess
import datetime

import environ

def get_video_length(filename):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    return float(result.stdout)

def main(inputs, block_size=60):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', heartbeat=7200))
    channel = connection.channel()
    channel.queue_declare(queue='deeplens')

    for input_file in inputs:
        count = 0
        length = get_video_length(input_file)
        while count * block_size < length:
            input_name = os.path.splitext(input_file)[0] # remove extension of input filename
            extension = os.path.splitext(input_file)[1] if len(os.path.splitext(input_file)) > 1 else ''
            output_file = input_name + '_part' + str(count + 1) + extension
            subprocess.call(['ffmpeg', '-ss', str(count * block_size), '-i', input_file,
                             '-t', str(block_size), '-c', 'copy', '-strict', '-2', output_file])
            channel.basic_publish(exchange='', routing_key='deeplens', body=output_file)
            print(" [x] Sent " + output_file)
            count += 1
    connection.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("This script will partition a list of videos into blocks of the same block size (in seconds).")
        print("Usage: python3 exp10_queue_driver.py block_size file1.mp4 file2.mp4 ...")
        sys.exit(0)
    try:
        print(datetime.datetime.now())
        inputs = sys.argv[2:]
        main(inputs=inputs, block_size=int(sys.argv[1]))
        print(datetime.datetime.now())
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
