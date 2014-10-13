#coding=UTF-8
import urllib2
import json
from mysql import MySql


class CrawlTopMovie:

	def __init__(self,	start_num,	count_num):
		# Init function:
		# Add request Header
		# Set start&count number
		# Connect MySQL
		self.movie_url = "http://api.douban.com/v2/movie/top250?"
		self.header = {'User-Agent': 'Mozilla/5.0'}
		self.start = "start=" + str(start_num)
		self.count = "count=" + str(count_num)
		self.sql = MySql("Your MySQL setting")
		self.data = []

	def getDataFromUrl(self, url):
		# Get data
		request = urllib2.Request(url, None, self.header)
		data = urllib2.urlopen(request).read()
		return json.loads(data)

	def getSubjects(self):
		url = self.movie_url + self.start + "&" + self.count
		print url
		return self.getDataFromUrl(url)

	def crawlData(self):
		subject_data = self.getSubjects()
		if subject_data:
			for subject in subject_data["subjects"]:
				self.data = subject
				self.importToMySQL("movie")
			self.sql.quit()
		else:
			print "no subject"

	def	importToMySQL(self,	table):
		id = self.sql.getMaxID(table)
		self.sql.insert(table,
					(id,
						self.getTitle(),
						self.getMovieID(),
						self.getDoubanURL(),
						self.getRating(),
						self.getDirector(),
						self.getGenres(),
						self.getImage(),
						self.getDate()
					))

	def	getRating(self):
		return float(self.data["rating"]["average"])

	def	getDoubanURL(self):
		return self.data["alt"]

	def	getDirector(self):
		return self.data["directors"][0]["name"]

	def	getGenres(self):
		return str(self.data["genres"])

	def	getMovieID(self):
		return int(self.data["id"])

	def	getImage(self):
		return	self.data["images"]["large"]

	def	getTitle(self):
		return	self.data["title"]

	def	getDate(self):
		return	self.data["year"]

if	__name__	==	'__main__':
	run	= CrawlTopMovie(1, 100)
	run.crawlData()
