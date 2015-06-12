import os
from common import load_json

filedir = os.path.dirname(os.path.abspath(__file__))
config = load_json(os.path.join(filedir, 'config'))
