import sys
import os

# Add root directory to sys.path so app, config, etc. can be imported cleanly on Vercel
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from main import app
