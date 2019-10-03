"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

detect.py defines an interface to tensorflows object detection library.
"""

import numpy as np
import tensorflow as tf

import cv2

from dlcv.object_detection.tensorflow_detect.utils import label_map_util
from dlcv.dataflow.buffer_map import *

class TensorFlowObjectDetect(BufferMap):

    def __init__(self, \
                 model_file, \
                 label_file, \
                 num_classes, \
                 confidence=0.90,
                 allowed_labels=None, \
                 args={}):

        self.model_file = model_file
        self.label_file = label_file
        self.num_classes = num_classes
        self.graph = self.loadGraph()
        self.category_index = self.getLabelMaps()

        self.confidence = confidence
        self.allowed_labels = allowed_labels

        super(TensorFlowObjectDetect, self).__init__(**args)


    def loadGraph(self):
        detection_graph = tf.Graph()
        with detection_graph.as_default():
          od_graph_def = tf.GraphDef()
          with tf.gfile.GFile(self.model_file + "/frozen_inference_graph.pb", 'rb') as fid:
            serialized_graph = fid.read()
            od_graph_def.ParseFromString(serialized_graph)
            tf.import_graph_def(od_graph_def, name='')

        return detection_graph


    def getLabelMaps(self):
        label_map = label_map_util.load_labelmap(self.label_file)
        categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=self.num_classes, use_display_name=True)
        category_index = label_map_util.create_category_index(categories)
        return category_index


    def run_inference(self, image_array):
      graph = self.graph
      with graph.as_default():
        with tf.Session() as sess:
          # Get handles to input and output tensors
          ops = tf.get_default_graph().get_operations()
          all_tensor_names = {output.name for op in ops for output in op.outputs}
          tensor_dict = {}
          for key in [
              'num_detections', 'detection_boxes', 'detection_scores',
              'detection_classes', 'detection_masks'
          ]:
            tensor_name = key + ':0'
            if tensor_name in all_tensor_names:
              tensor_dict[key] = tf.get_default_graph().get_tensor_by_name(
                  tensor_name)
          if 'detection_masks' in tensor_dict:
            # The following processing is only for single image
            detection_boxes = tf.squeeze(tensor_dict['detection_boxes'], [0])
            detection_masks = tf.squeeze(tensor_dict['detection_masks'], [0])
            # Reframe is required to translate mask from box coordinates to image coordinates and fit the image size.
            real_num_detection = tf.cast(tensor_dict['num_detections'][0], tf.int32)
            detection_boxes = tf.slice(detection_boxes, [0, 0], [real_num_detection, -1])
            detection_masks = tf.slice(detection_masks, [0, 0, 0], [real_num_detection, -1, -1])
            detection_masks_reframed = utils_ops.reframe_box_masks_to_image_masks(
                detection_masks, detection_boxes, image.shape[0], image.shape[1])
            detection_masks_reframed = tf.cast(
                tf.greater(detection_masks_reframed, self.confidence), tf.uint8)
            # Follow the convention by adding back the batch dimension
            tensor_dict['detection_masks'] = tf.expand_dims(
                detection_masks_reframed, 0)
          image_tensor = tf.get_default_graph().get_tensor_by_name('image_tensor:0')

          # Run inference
          output_dict = sess.run(tensor_dict,
                                 feed_dict={image_tensor: image_array})

          rtn = []
          for i in range(image_array.shape[0]):
            buff = {}
            buff['num_detections'] = int(output_dict['num_detections'][i])

            buff['detection_classes'] = [self.category_index[c]['name'] for c in output_dict['detection_classes'][i].astype(np.uint8)]
            
            buff['detection_boxes'] = output_dict['detection_boxes'][i]
            buff['detection_scores'] = output_dict['detection_scores'][i]

            if 'detection_masks' in output_dict:
              buff['detection_masks'] = output_dict['detection_masks'][i]

            rtn.append(self._process(buff))

      return rtn


    def _process(self, detection_dict):
      tags = []
      for j in range(detection_dict['num_detections']):
        y0 = int(detection_dict['detection_boxes'][j][0]*self.video_stream.height)
        y1 = int(detection_dict['detection_boxes'][j][2]*self.video_stream.height)
        x0 = int(detection_dict['detection_boxes'][j][1]*self.video_stream.width)
        x1 = int(detection_dict['detection_boxes'][j][3]*self.video_stream.width)

        box = (x0,x1,y0,y1)

        label = detection_dict['detection_classes'][j]

        if self.allowed_labels != None \
           and label not in self.allowed_labels:
          continue

        if detection_dict['detection_scores'][j] >= self.confidence:
          tags.append((label, box))

      return {'bounding_boxes': tags}


    def map(self, buffer):
      
      if len(buffer) > 0:
        return self.run_inference(np.stack(buffer))
      else:
        return []


