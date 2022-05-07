
import os
import sys
import time
 
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from tornado.options import define, options
 
class HasBlockTaskHandler(tornado.web.RequestHandler):
  executor = ThreadPoolExecutor(20)  # Start the thread pool, which is determined by the current RequestHandler Hold 
   
  @tornado.gen.coroutine
  def get(self):
    strTime = time.strftime("%Y-%m-%d %H:%M:%S")
    print("in get before block_task %s" % strTime)
    result = yield self.block_task(strTime)
    print("in get after block_task")
    self.write("%s" % (result))
 
  @run_on_executor
  def block_task(self, strTime):
    print("in block_task %s") % strTime
    for i in range(1, 16):
      time.sleep(1)
      print ("step %d : %s" % (i, strTime))
    return ("Finish %s" % strTime)
 
if __name__ == "__main__":
  tornado.options.parse_command_line()
  app = tornado.web.Application(handlers=[(r"/sleep", HasBlockTaskHandler)], autoreload=False, debug=False)
  http_server = tornado.httpserver.HTTPServer(app)
  http_server.bind(8888)
  tornado.ioloop.IOLoop.instance().start()