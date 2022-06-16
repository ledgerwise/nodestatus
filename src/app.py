import os
import inspect
import json
from timeloop import Timeloop
from datetime import timedelta, datetime
import time
from flask import Flask, send_from_directory
from include.nodestatus import run_nodestatus
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
tl = Timeloop()

ENVIRONMENT = os.getenv('ENVIRONMENT')
PORT = int(os.getenv('PORT') or 3000)
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL') or 30)
BASE_PATH = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
CONFIG_PATH = BASE_PATH + '/config.json'
PUB_PATH = '{}/../pub'.format(BASE_PATH)

@app.route('/health')
def health():
    return 'OK'

@app.route('/<path:path>')
def send_json(path):
    return send_from_directory(PUB_PATH, path)
def run_checks():
   try:
      with open(CONFIG_PATH, 'r') as fp:
         CHAINS = json.load(fp)
   except Exception as e:
      print(f'Error getting config from {CONFIG_PATH}: {e}')
      quit()
   run_nodestatus(CHAINS, PUB_PATH)

scheduler = BackgroundScheduler()
job = scheduler.add_job(run_checks, 'interval', minutes=CHECK_INTERVAL, jitter = 10, next_run_time=datetime.now())
scheduler.start()

if __name__ == "__main__":
   #tl.start()
   if ENVIRONMENT == 'PROD':
      from waitress import serve
      serve(app, host="0.0.0.0", port=PORT)
   else:
      app.run(debug=True, host="0.0.0.0", port=PORT) 
   
