# -*- coding: utf-8 -*-
import os
from BeautifulSoup import BeautifulSoup
from MailmanArchiveScraper import MailmanArchiveScraper

"""
Download the gzip text file with the month's messages.
"""
class MailmanGzTextScraper(MailmanArchiveScraper):

    def __init__(self):
        super(MailmanGzTextScraper, self).__init__()
        self.local_dir = self.publish_dir + 'text'
        if not os.path.exists(self.local_dir):
            os.mkdir(self.local_dir)

    """
    fetch the the whole month's message as gzipped text
    """
    def scrapeList(self):
        source = self.fetchPage(self.list_url)
        filtered_source = self.filterPage(source)
        soup = BeautifulSoup(source)


        for row in soup.first('table')('tr')[1:]:
            rel_url = row('td')[2]('a')[0].get('href')
            source = self.fetchPage(self.list_url + '/' + rel_url)

            local_month = open(self.local_dir + '/' + rel_url, 'w')
            local_month.write(source)
            local_month.close()



def main():
    scraper = MailmanGzTextScraper()
    scraper.scrape()


if __name__ == "__main__":
    main()
    

