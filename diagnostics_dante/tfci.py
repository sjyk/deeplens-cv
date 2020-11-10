import argparse
import os
import sys
import urllib

from absl import app
from absl.flags import argparse_flags
import tensorflow.compat.v1 as tf

import tensorflow_compression as tfc  # pylint:disable=unused-import

# Default URL to fetch metagraphs from.
URL_PREFIX = "https://storage.googleapis.com/tensorflow_compression/metagraphs"
# Default location to store cached metagraphs.
METAGRAPH_CACHE = "/tmp/tfc_metagraphs"

def write_array(image):
  """Creates graph to write a PNG image file."""
  image = tf.squeeze(image, 0)
  if image.dtype.is_floating:
    image = tf.round(image)
  if image.dtype != tf.uint8:
    image = tf.saturate_cast(image, tf.uint8)
  # string = tf.image.encode_png(image)
  return image


def load_cached(filename):
  """Downloads and caches files from web storage."""
  pathname = os.path.join(METAGRAPH_CACHE, filename)
  try:
    with tf.io.gfile.GFile(pathname, "rb") as f:
      string = f.read()
  except tf.errors.NotFoundError:
    url = URL_PREFIX + "/" + filename
    try:
      request = urllib.request.urlopen(url)
      string = request.read()
    finally:
      request.close()
    tf.io.gfile.makedirs(os.path.dirname(pathname))
    with tf.io.gfile.GFile(pathname, "wb") as f:
      f.write(string)
  return string


def import_metagraph(model):
  """Imports a trained model metagraph into the current graph."""
  string = load_cached(model + ".metagraph")
  metagraph = tf.MetaGraphDef()
  metagraph.ParseFromString(string)
  tf.train.import_meta_graph(metagraph)
  return metagraph.signature_def


def instantiate_signature(signature_def):
  """Fetches tensors defined in a signature from the graph."""
  graph = tf.get_default_graph()
  inputs = {
      k: graph.get_tensor_by_name(v.name)
      for k, v in signature_def.inputs.items()
  }
  outputs = {
      k: graph.get_tensor_by_name(v.name)
      for k, v in signature_def.outputs.items()
  }
  return inputs, outputs


def compress_image(model, input_image):
  """Compresses an image array into a bitstring."""
  with tf.Graph().as_default():
    # Load model metagraph.
    signature_defs = import_metagraph(model)
    inputs, outputs = instantiate_signature(signature_defs["sender"])

    # Just one input tensor.
    inputs = inputs["input_image"]
    # Multiple output tensors, ordered alphabetically, without names.
    outputs = [outputs[k] for k in sorted(outputs) if k.startswith("channel:")]

    # Run encoder.
    with tf.Session() as sess:
      arrays = sess.run(outputs, feed_dict={inputs: input_image})

    # Pack data into bitstring.
    packed = tfc.PackedTensors()
    packed.model = model
    packed.pack(outputs, arrays)
    return packed.string


def compress(model, input_array, output_file, target_bpp=None, bpp_strict=False):
  # Load image.
  with tf.Graph().as_default():
    with tf.Session() as sess:
      input_image = sess.run(tf.expand_dims(input_array, 0))
      num_pixels = input_image.shape[-2] * input_image.shape[-3]

  if not target_bpp:
    # Just compress with a specific model.
    bitstring = compress_image(model, input_image)
  else:
    # Get model list.
    models = load_cached(model + ".models")
    models = models.decode("ascii").split()

    # Do a binary search over all RD points.
    lower = -1
    upper = len(models)
    bpp = None
    best_bitstring = None
    best_bpp = None
    while bpp != target_bpp and upper - lower > 1:
      i = (upper + lower) // 2
      bitstring = compress_image(models[i], input_image)
      bpp = 8 * len(bitstring) / num_pixels
      is_admissible = bpp <= target_bpp or not bpp_strict
      is_better = (best_bpp is None or
                   abs(bpp - target_bpp) < abs(best_bpp - target_bpp))
      if is_admissible and is_better:
        best_bitstring = bitstring
        best_bpp = bpp
      if bpp < target_bpp:
        lower = i
      if bpp > target_bpp:
        upper = i
    if best_bpp is None:
      assert bpp_strict
      raise RuntimeError(
          "Could not compress image to less than {} bpp.".format(target_bpp))
    bitstring = best_bitstring

  # Write bitstring to disk.
  with tf.io.gfile.GFile(output_file, "wb") as f:
    f.write(bitstring)


def decompress(input_file):
  with tf.Graph().as_default():
    # Unserialize packed data from disk.
    with tf.io.gfile.GFile(input_file, "rb") as f:
      packed = tfc.PackedTensors(f.read())

    # Load model metagraph.
    signature_defs = import_metagraph(packed.model)
    inputs, outputs = instantiate_signature(signature_defs["receiver"])

    # Multiple input tensors, ordered alphabetically, without names.
    inputs = [inputs[k] for k in sorted(inputs) if k.startswith("channel:")]
    # Just one output operation.
    outputs = write_array(outputs["output_image"])

    # Unpack data.
    arrays = packed.unpack(inputs)

    # Run decoder.
    with tf.Session() as sess:
      sess.run(outputs, feed_dict=dict(zip(inputs, arrays)))
  return outputs