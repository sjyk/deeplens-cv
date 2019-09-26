#  MIT License
#
#  Copyright (c) 2019. Adam Dziedzic and Sanjay Krishnan
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
#  Written by Adam Dziedzic.

import abc

class Detector(abc.ABC):

    @abc.abstractmethod
    def detect(self, img):
        """
        Detect object in the image.
        :return: detections, each in the following format:
        (x1, y1, x2, y2, conf_level, class_pred), where (x1,y1) is the top-left
        corner of the img and the (x2,y2) is the bottom left corner of the img.

        [[x1, y1, x2, y2, score, class_id],
        [x1, y1, x2, y2, score, class_id],...]

        -
        """
        pass