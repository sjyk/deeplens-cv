from dlcv.dataflow.map import Map
import cv2

class KeyPoints(Map):

	def __init__(self, \
				 blur=5, \
				 edge_low=225, \
				 edge_high=250, \
				 area_thresh=10,
				 label="object"):

		self.blur = blur
		self.edge_low = edge_low
		self.edge_high = edge_high
		self.area_thresh = area_thresh
		self.label = label

	def map(self, data):
		ff = data
		gray = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		blurred = cv2.GaussianBlur(gray, (self.blur, self.blur), 0)
		tight = cv2.Canny(blurred, self.edge_low, self.edge_high)
		contours, _ = cv2.findContours(tight.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
		
		rtn = []
		for cnt in contours:
			if cv2.contourArea(cnt) > self.area_thresh:
				M = cv2.moments(cnt)
				cx = int(M['m10']/M['m00'])
				cy = int(M['m01']/M['m00'])

				try:
					rtn.append((self.label,(cx+self.crop.x0,\
											cy+self.crop.y0,\
											cx+self.crop.x0,\
											cy+self.crop.y0)))
				except:
					rtn.append((self.label,(cx,cy,cx,cy)))


		ff['bounding_boxes'] = rtn
		return ff
