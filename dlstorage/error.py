
class CorruptedOrMissingVideo(Exception):
   """Raised when opencv cannot open a video"""
   pass

class VideoNotFound(Exception):
   """Video with the specified name not found in the manager"""
   pass

class ManagerIOError(Exception):
   """Unspecified error with the manager"""
   pass