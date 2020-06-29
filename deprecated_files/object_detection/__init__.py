import sys
import os

relative_import = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.abspath(relative_import))

