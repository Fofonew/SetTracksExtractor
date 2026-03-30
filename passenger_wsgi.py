import os
import sys

# Ajoute le binaire ffmpeg au PATH
os.environ["PATH"] = os.path.expanduser("~/bin") + ":" + os.environ.get("PATH", "")

# Charge le .env
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from app import app

# Passenger attend une variable 'application' WSGI
# FastAPI est ASGI, on utilise l'adaptateur a]sync de Passenger
# qui supporte ASGI depuis cPanel Python App
application = app
