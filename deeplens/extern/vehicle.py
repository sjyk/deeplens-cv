import numpy as np
import os
import cv2

from tensorflow.keras.applications.resnet50 import ResNet50
from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.resnet50 import preprocess_input, decode_predictions
from tensorflow.keras import Model

from joblib import dump, load

from deeplens.tracking.event import *


##handle vehicle classification

class VehicleType(ActivityClassifier):

    def __init__(self, trigger, tag, region):
        model = ResNet50(weights='imagenet')
        layer = model.get_layer(name='avg_pool')
        self.em = model

        super(VehicleType, self).__init__(trigger, tag, region)

    def classify(self, img):

        x = cv2.resize(img, dsize=(224,224))
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)

        preds = self.em.predict(x)
        decoded = decode_predictions(preds, top=10)
        for _, cl, v in decoded[0]:
            if 'van' in cl or 'trailer' in cl or 'ambulence' in cl:
                return 'VAN' 
            elif 'car' in cl or 'cab' in cl or 'racer' in cl:
                return 'CAR'
        
        return 'None' 