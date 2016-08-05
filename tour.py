# Python imports
import io
import json
import sys
import time

# Google APIs imports
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseUpload

# BigQuery e2e imports
import auth

