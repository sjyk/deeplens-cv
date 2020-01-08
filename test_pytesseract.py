import logging
import sys

from deeplens.struct import VideoStream
from deeplens.dataflow.map import Sample
from deeplens.ocr.pytesseract import PyTesseractOCR
from timeit import default_timer as timer


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start = timer()

    if len(sys.argv) < 2:
        print("Enter filename as argv[1]")
        exit(1)
    filename = sys.argv[1]
    v = VideoStream(filename, limit=500)
    pipeline = v[Sample(1/50)][PyTesseractOCR()]

    labels = [text for text in pipeline]
    print(labels)

    end = timer()
    print(end - start)
