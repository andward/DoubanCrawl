import urllib2
from mysql import MySql
import time
import datetime
import random
import re


class BookCrawl:

    """Crawl book data douban.com by API"""

    def __init__(self, book_num):
        self.douban_url = "https://api.douban.com/v2/book/"
        self.start_num = book_num[0]
        self.end_num = book_num[1]
        self.header = {'User-Agent': 'Mozilla/5.0'}
        self.sql = MySql("Your MySQL setting")
        self.data = []

    def __iter__(self):
        return self

    def next(self):
        if self.start_num < self.end_num:
            try:
                self.crawlData(self.douban_url + str(self.start_num))
            except:
                print self.start_num
            self.start_num += 1
            return self.start_num
        else:
            self.sql.quit()
        raise StopIteration()

    def test(self, url):
        self.data = self.getDataFromUrl(url)
        print self.getPrice()

    def crawlData(self, url):
        self.data = self.getDataFromUrl(url)
        self.insertBookInfo("bookinfo")
        self.insertBookTag("booktag")

    def getDataFromUrl(self, url):
        request = urllib2.Request(url, None, self.header)
        data = urllib2.urlopen(request).read()
        return eval(data)

    def getRating(self):
        return float(self.data["rating"]["average"])

    def getAuthor(self):
        if self.data["author"] == []:
            return ""
        else:
            return self.data["author"][0]

    def getPublishDate(self):
        date_str = self.data["pubdate"].split("-")
        date_format = ["%y", "%m", "%d"]
        format_type = "-".join(date_format[:len(date_str)])
        date = datetime.datetime.strptime(self.data["pubdate"][2:],
                                          format_type).date()
        return date

    def getImage(self):
        return self.data["image"]

    def getBookID(self):
        return int(self.data["id"])

    def getPublisher(self):
        return self.data["publisher"]

    def getTitle(self):
        return self.data["title"]

    def getUrl(self):
        return self.data["alt"]

    def getSummary(self):
        return self.data["summary"]

    def getPrice(self):
        price = self.data["price"]
        return float(re.search(r'\d+\.?\d*', price).group(0))

    def getBookTag(self):
        tags = self.data["tags"]
        if tags:
            return [item["name"] for item in tags]
        else:
            return []

    def insertBookInfo(self, table):
        id = self.sql.getMaxID(table)
        self.sql.insert(table,
                        (id,
                         self.getBookID(),
                         self.getTitle(),
                         self.getAuthor(),
                         self.getPublisher(),
                         self.getPublishDate(),
                         self.getPrice(),
                         self.getRating(),
                         1,
                         self.getSummary(),
                         self.getUrl(),
                         self.getImage())
                        )

    def insertBookTag(self, table):
        if self.getBookTag():
            for item in self.getBookTag():
                self.sql.insert(table,
                                (item,
                                 self.getBookID())
                                )


if __name__ == "__main__":
    for item in BookCrawl((1001000, 1010000)):
        pending_time = random.random() * 5
        time.sleep(pending_time)
