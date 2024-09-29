from tornado.web import Application, RequestHandler, ErrorHandler, StaticFileHandler
from tornado.ioloop import IOLoop
from function.db import dbmongo
from routes import init as route_init
from bson import ObjectId
import traceback
import json 
import sys
import asyncio
import os

from logger import setup_logger
logger = setup_logger(to_file=False)
logger.info("Setup logger")

from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")

port = config["SERVER"]["port"]
db = dbmongo()
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
def make_app(): 
  urls = route_init.list_url
  urls.append( (r"/data/(.*)", StaticFileHandler, {"path": os.path.join(os.path.dirname(__file__), 'data')}), )
  return Application(urls, debug=False)
  
if __name__ == '__main__':
  app = make_app()
  app.listen(port)
  logger.info("***********************")
  logger.info ("Restart App")
  logger.info("***********************")
  sys.stdout.flush()
  IOLoop.instance().start()