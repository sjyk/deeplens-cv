try:
    from PIL import Image
except ImportError:
    import Image
import pytesseract

from deeplens.dataflow.map import *


class PyTesseractOCR(Map):

    def __init__(self,
                 lang=None,
                 custom_oem_psm_config='',
                 args={}):

        self.lang = lang
        self.custom_oem_psm_config = custom_oem_psm_config  # example: r'--oem 3 --psm 6'
        super(PyTesseractOCR, self).__init__(**args)

    def map(self, img):
        img_rgb = cv2.cvtColor(img['data'], cv2.COLOR_BGR2RGB)
        return pytesseract.image_to_string(img_rgb, lang=self.lang, config=self.custom_oem_psm_config)

