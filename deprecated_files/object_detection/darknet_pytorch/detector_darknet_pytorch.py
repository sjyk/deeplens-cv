from torchvision import transforms
from torch.autograd import Variable
import torch
from PIL import Image
from object_detection.interface_detector import Detector

from .utils import non_max_suppression

if torch.cuda.is_available():
    Tensor = torch.cuda.FloatTensor
else:
    Tensor = torch.FloatTensor

import numpy as np


class DetectorDarknetPytorch(Detector):

    def __init__(self, args, model):
        self.args = args
        self.model = model

    def detect_image(self, img):
        # scale and pad image
        if self.args.scale_image == "yes":
            self.ratio = min(self.args.img_size / img.size[0],
                             self.args.img_size / img.size[1])
        else:
            self.ratio = 1
        imw = round(img.size[0] * self.ratio)
        imh = round(img.size[1] * self.ratio)
        transformations = []
        transformations.append(transforms.Resize((imh, imw)))
        if self.args.pad_image == "yes":
            transformations.append(transforms.Pad((max(
                int((imh - imw) / 2), 0), max(
                int((imw - imh) / 2), 0), max(
                int((imh - imw) / 2), 0), max(
                int((imw - imh) / 2), 0)),
                (128, 128, 128)))
        transformations.append(transforms.ToTensor())
        img_transforms = transforms.Compose(transformations)
        # convert image to Tensor
        image_tensor = img_transforms(img).float()
        # print("image tensor: ", image_tensor.size())
        image_tensor = image_tensor.unsqueeze_(0)
        input_img = Variable(image_tensor.type(Tensor))
        # run inference on the model and get detections
        with torch.no_grad():
            detections = self.model(input_img)
            detections = non_max_suppression(detections,
                                             self.args.num_classes,
                                             self.args.conf_thres,
                                             self.args.nms_thres)
        detections = detections[0]
        # print("type of detections: ", type(detections))
        # print("detections: ", detections)
        if detections is not None:
            return self.revert_box_merge_conf(detections=detections, img=img)
        else:
            # return an empty tensor
            return torch.tensor([])



    def revert_box_merge_conf(self, detections, img):
        """
        Revert the box coordinates to the initial state (so that the bounding
        box can be drawn in the image) and merge the confidence levels.

        :param detections: detections by YOLO
        :return: bounding boxes in detections aligned with the image and the
        scores confidence levels merged
        return format of a single detection:
        (x1, y1, box_w, box_h, conf_level, class_pred)
        """
        # pilimg = Image.fromarray(img)  # it is already PIL image
        img = np.array(img)
        pad_x = max(img.shape[0] - img.shape[1], 0) * (
                self.args.img_size / max(img.shape))
        pad_y = max(img.shape[1] - img.shape[0], 0) * (
                self.args.img_size / max(img.shape))
        unpad_h = self.args.img_size - pad_y
        unpad_w = self.args.img_size - pad_x

        for detection in detections:
            x1, y1, x2, y2, obj_conf, class_conf, class_pred = detection

            # transform the representation of the bounding box
            box_h = int(((y2 - y1) / unpad_h) * img.shape[0])
            box_w = int(((x2 - x1) / unpad_w) * img.shape[1])
            y1 = int(((y1 - pad_y // 2) / unpad_h) * img.shape[0])
            x1 = int(((x1 - pad_x // 2) / unpad_w) * img.shape[1])
            detection[0] = x1
            detection[1] = y1
            detection[2] = x1 + box_w
            detection[3] = y1 + box_h

            # merge the scores / confidence levels
            detection[4] = obj_conf * class_conf
            detection[5] = class_pred

        # Get rid of the last element (the repeated class prediction).
        detections = detections[:,:-1]

        return detections

    def predict_for_mot(self, frame):
        pilimg = Image.fromarray(frame)
        return self.detect_image(pilimg).cpu()

    def predict_for_motchallenge(self, image):
        return self.detect_image(image).cpu()

    def detect(self, img):
        """
        Implement the interface from the Detector abstract class.

        :param img: the input image
        :return: detections, each in the following format:
        (x1, y1, x2, y2, conf_level, class_pred)
        """
        pilimg = Image.fromarray(img)
        return self.detect_image(pilimg).cpu().numpy()
