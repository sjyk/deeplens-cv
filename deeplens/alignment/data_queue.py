class DataQueue():
    """The video stream class opens a stream of video
       from a source.

    Frames are structured in the following way: (1) each frame 
    is a dictionary where frame['data'] is a numpy array representing
    the image content, (2) all the other keys represent derived data.

    All geometric information (detections, countours) go into a list called
    frame['bounding_boxes'] each element of the list is structured as:
    (label, box).
    """

    def __init__(self):
        """Constructs a pipeline object linked with a FullManager
        """
        self.video_stream = Queue()
        self.data_streams = {}
        self.labels = []
    
    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        return self

    def __next__(self):
        # Get VideoStream from queue
        try:
            current_videostream = self.dequeue_videostream()
        except Empty():
            logging.debug("self.video_stream queue is empty!")
            raise StopIteration("Iterator is closed")

        # Get DataStream from queue
        try:
            current_datastreams = [self.dequeue_datastream(label) for label in self.labels]
        except Empty():
            logging.debug("self.data_streams queue is empty!")
            raise StopIteration("Iterator is closed")

        return (current_videostream, current_datastreams)

    def add_datastream(self, label, data_stream):
        if label in self.labels:
            raise KeyError("Label '%s' already in pipeline!" % label)

        self.labels.append(label)

        self.data_streams[label] = Queue()
        self.data_streams[label].put(data_stream)

    def enqueue_datastream(self, label, data_stream):
        if label not in self.labels:
            raise KeyError("Label '%s' does not exist in pipeline!" % label)

        self.data_streams[label].put(data_stream)

    def dequeue_datastream(self, label):
        return self.data_streams[label].get_nowait()

    def enqueue_videostream(self, video_stream):
        self.video_stream.put(video_stream)

    def dequeue_videostream(self):
        return self.video_stream.get_nowait()