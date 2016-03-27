import tornado.ioloop
import tornado.web
import tornado.httpclient

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class AnotherHandler(tornado.web.RequestHandler):
	def get(self):
		value = self.get_query_argument('user', default="")
		channel_id = self.get_query_argument('channel_id', default="")		
		print value, channel_id
		response = {
			"status" : 400, 
			"message" : "I am here another message!!",
			"user" : self.get_current_user()
			}
		self.write(response)
		self.flush()


class PageLoadHandler(tornado.web.RequestHandler):
	@tornado.web.asynchronous
	def get(self):		
		link = self.get_query_argument('link')
		if link is not None:
			http = tornado.httpclient.AsyncHTTPClient()
			http.fetch(link, self._on_download) 	
				
	def _on_download(self, response):
		self.write("Downloaded")
		self.write(response.body)
		self.finish()		
			
				
application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/another", AnotherHandler), 
    (r"/getlink", PageLoadHandler)
])

if __name__ == "__main__":
    application.listen(8989)
    tornado.ioloop.IOLoop.current().start()

