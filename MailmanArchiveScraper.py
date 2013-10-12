"""
* Scrapes the archive pages of one or more lists in a Mailman installation and republishes the contents, with an optional RSS feed.
* v1.2, 2013-10-12
* http://github.com/philgyford/mailman-archive-scraper/
* 
* Only works with Monthly archives at the moment.
* Could do with more error checking, especially around loadConfig().
* Hasn't had a huge amount of testing -- use with care.
"""
import ClientForm, ConfigParser, datetime, email.utils, mechanize, os, PyRSS2Gen, re, sys, time, urlparse
from BeautifulSoup import BeautifulSoup


class FullRSSItem(PyRSS2Gen.RSSItem):
    """
    Extending the basic RSSItem class in order to allow for an extra 'content:encoded' element.
    This should be passed in to the class in the initial dictionary keyed with 'content'.
    The text can be HTML.
    """
    
    def __init__(self, **kwargs):
        if 'content' in kwargs:
            self.content = kwargs['content']
            del kwargs['content']
        else:
            self.content = None
        PyRSS2Gen.RSSItem.__init__(self, **kwargs)


    def publish_extensions(self, handler):
        # handler.startElement("content:encoded")
        # handler.endElement("content:encoded")
        PyRSS2Gen._opt_element(handler, "content:encoded", self.content)


class MailmanArchiveScraper(object):
    """
    Scrapes the archive pages of one or more lists in a Mailman installation and republishes the contents.
    """
    
    def __init__(self):
        self.loadConfig()

        # We need to know if this is a public or private list.
        # We assume it's public if there's no username set.
        self.public_list = True
        if self.username:
            self.public_list = False

        # Set the URL for all the archive's pages.
        if self.public_list:
            self.list_url = self.protocol + '://' + self.domain + '/pipermail/' + self.list_name
        else:
            self.list_url = self.protocol + '://' + self.domain + '/mailman/private/' + self.list_name

        # Make the directory in which we'll save all the files on the local machine.
        if not os.path.exists(self.publish_dir):
            os.mkdir(self.publish_dir)
            
        # We'll keep track of how many items (emails) we fetch with this.
        self.messages_fetched = 0
        
        self.prepareRSS()
        
        self.prepareRegExps()
        

    def loadConfig(self):
        "Loads configuration from the MailmanArchiveScraper.cfg file"
        config_file = sys.path[0]+'/MailmanArchiveScraper.cfg'
        config = ConfigParser.SafeConfigParser({'protocol': 'http'})
        
        try:
            config.readfp(open(config_file))
        except IOError:
            self.error("Can't read config file: " + config_file)
            
        self.username = config.get('Mailman', 'email')

        self.password = config.get('Mailman', 'password')
        self.domain = config.get('Mailman', 'domain')
        self.protocol = config.get('Mailman', 'protocol')
                
        self.list_name = config.get('Mailman', 'list_name')
        
        self.filter_email_addresses = config.getboolean('Conversion', 'filter_email_addresses')
        self.list_info_url = config.get('Conversion', 'list_info_url')
        
        self.strip_quotes = config.getint('Conversion', 'strip_quotes')

        head_html_path = config.get('Conversion', 'head_html')
        if head_html_path:
            fp = open(head_html_path, 'r')
            self.head_html = fp.read()
            fp.close()
        else:
            self.head_html = ''

        # search_replace will be a dictionary of searchstring : replacestring
        self.search_replace = {}
        if config.get('Conversion', 'search_replace') != '':
            for sr in config.get('Conversion', 'search_replace').split('\n'):
                try:
                    (search, replace) = sr.split('//')
                except:
                    self.error("'"+sr+"' is not a valid search_replace string.")
                self.search_replace[search] = replace
        
        
        self.rss_file = config.get('RSS', 'rss_file')
        self.items_for_rss = int(config.get('RSS', 'items_for_rss'))
        self.rss_title = config.get('RSS', 'rss_title')
        self.rss_description = config.get('RSS', 'rss_description')
        
        self.publish_dir = config.get('Local', 'publish_dir')
        self.publish_url = config.get('Local', 'publish_url')
        self.hours_to_go_back = int(config.get('Local', 'hours_to_go_back'))
        self.verbose = config.getboolean('Local', 'verbose')


    def prepareRegExps(self):
        """"
        All the regular expressions we'll use in filterPage().
        If I understand correctly, I think it's best to compile these once, rather than 
        doing so every time we need to use them.
        Although the regexps are set here, they might not be used in filterPages(),
        depending on the config settings.
        """

        # Remove all standard emails, eg "billy@nomates.com" or "<billy@nomates.com>"
        self.match_email = re.compile(r'\b<?[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}>?\b', re.IGNORECASE)

        # Remove all email addresses obscured by Mailman, eg "billy at nomates.com"
        self.match_text_email = re.compile(r"""
            (&lt;)?                     # Optional &lt;
            <A\sHREF\="[^>]*?>         # <A HREF="...">
            [A-Z0-9._%+-]+              # The name part of an email address
            \sat\s
            [A-Z0-9.-]+\.[A-Z]{2,4}     # Domain and TLD of an email address
            \s*?<\/A>
            (&gt;)?                     # Optional &gt;
            """, re.IGNORECASE | re.VERBOSE | re.MULTILINE)

        # Remove all mailto: links.
        self.match_mailto = re.compile(r'<A\sHREF\="mailto\:[^>]*?>([^<]*?)</A>', re.IGNORECASE | re.MULTILINE)
        # A bit that's left over
        self.match_mailto_label = re.compile(r'\[mailto\:\]')

        # Replace any remaining links to the original list pages with #
        # A bit messy, but just in case.
        # eg, for links to message attachments.
        self.match_list_url = re.compile(r''+self.list_url, re.IGNORECASE)

        # Replace the list info url with our custom one from the config
        self.match_list_info_url = re.compile(self.protocol + '://' + self.domain + '/mailman/listinfo/' + self.list_name, re.IGNORECASE)

        # Matches lines that beging with </I>&gt;<i>
        # With the number of '&gt;' depending on the level of self.strip_quotes.
        if self.strip_quotes > 0:
            min_level_to_strip = self.strip_quotes + 1
            self.match_strip_quotes = re.compile(r'</I>(&gt;){' + str(min_level_to_strip) + ',}<i>\s.*?\n', re.IGNORECASE)

        # Prepare a dictionary of regexp => replacement for each of the search_replace terms.
        self.match_search_replace = {}
        for search, replace in self.search_replace.iteritems():
            regexp = re.compile(r''+search, re.IGNORECASE)
            self.match_search_replace[regexp] = replace

        # For inserting custom HTML just before the end of the <head></head> section.
        self.match_head_html = re.compile(r'</head>', re.IGNORECASE)

        # For removing anything before the subject of the message.
        # Probably something like "[List name]  Subject of the message".
        self.match_subject = re.compile(r'^(?:\[.*?\]\s+)?', re.IGNORECASE)


    def scrape(self):
        if not self.public_list:
            self.logIn()

        self.scrapeList()

        self.publishRSS()
        
    
    def prepareRSS(self):
        """Prepare things for the RSS feed."""
        
        if self.rss_file == '':
            # We're not generating an RSS feed.
            return
            
        self.rss = PyRSS2Gen.RSS2(
            title = self.rss_title,
            link = self.list_info_url,
            description = self.rss_description,
            lastBuildDate = datetime.datetime.now()
        )
        
        self.rss.rss_attrs['xmlns:content'] = 'http://purl.org/rss/1.0/modules/content/'
        
        # Items will be added in self.scrapeMessage().
        self.rss_items = []
    
    
    def addRSSItem(self, message_url, message_time, soup):
        """
        Add an item to the RSS feed.
        message_url - The local, newly-published, URL to this item on the web.
        message_time - The timestamp of when this email was sent.
        soup - A BeautifulSoup object of the HTML page the message was originally on.
        """

        if self.rss_file == '':
            # We're not generating an RSS feed.
            return

        # Get the subject of the message
        subject = soup.h1.string
        # Remove any preliminary "[List name] " stuff.
        subject = self.match_subject.sub(r'', subject)

        # Get who this email was from (the first <b> after the <h1>).
        sender = soup.h1.findNextSibling('b').string
        
        # Body of the message (everything within <pre></pre> tags) with all HTML tags stripped.
        body_text = ''.join(soup.pre.findAll(text=True))
        
        # Body of the message including HTML tags.
        #body_html = str(soup.pre.contents[0]).replace("\n", "<br />\n")
        body_html = ''.join(soup.pre.findAll(text=True)).replace("\n", "<br />\n")
        if sender:
            # Just in case sender is empty because the contents have been stripped
            # by the filtering process.
            body_text = 'From: '+sender+'. '+ body_text
            
            # Add some introductory text to the HTML version.
            body_html = """
<div class="email-meta">
    <strong>From:</strong> %s<br />
    <strong>Subject:</strong> %s<br />
    <strong>Date:</strong> %s
</div><br />
%s
"""  % (sender, 
        subject, 
        datetime.datetime.fromtimestamp(message_time).strftime('%d %B %Y, %H:%M'), 
        body_html) 

        # Add this message to the RSS feed.
        self.rss_items.append(
            FullRSSItem(
                title = sender + ' > ' + subject,
                link = message_url,
                description = self.smartTruncate(body_text, 500),
                pubDate = datetime.datetime.fromtimestamp(message_time),
                content = body_html
            )
        )
    
    
    def publishRSS(self):
        """Publish the accumulated RSS items."""

        if self.rss_file == '':
            # We're not generating an RSS feed.
            return

        self.rss.items = self.rss_items
        self.rss.write_xml(open(self.rss_file, "w"), 'utf-8')
        
        
    def logIn(self):
        """
        Logs in to private archives using the supplied email and password.
        Stores the cookie so we can continue to get subsequent pages.
        """
        
        cookieJar = mechanize.CookieJar()

        opener = mechanize.build_opener(mechanize.HTTPCookieProcessor(cookieJar))
        opener.addheaders = [("User-agent","Mozilla/5.0 (compatible)")]
        mechanize.install_opener(opener)
        
        self.message('Logging in to '+self.list_url)
        fp = mechanize.urlopen(self.list_url)
        forms = ClientForm.ParseResponse(fp, backwards_compat=False)
        fp.close()

        form = forms[0]
        form['username'] = self.username
        form['password'] = self.password
        fp = mechanize.urlopen(form.click())
        fp.close()


    def scrapeList(self):
        """
        Scrapes the pages for a list.
        Saves the list index page locally.
        Sends for scraping of each month's pages (and then on to the individual messages).
        """
        
        # Get the page that list the months of archive pages.
        source = self.fetchPage(self.list_url)
        
        # The copy of the page we save is filtered for email addresses, links, etc.
        filtered_source = self.filterPage(source)

        # Save our local copy.
        # eg /Users/phil/Sites/examplesite/html/list-name/index.html
        local_index = open(self.publish_dir + '/index.html', 'w')
        local_index.write(filtered_source)
        local_index.close()
        
        soup = BeautifulSoup(source)
        
        # Go through each row in the table except the first (which is column headers).
        for row in soup.first('table')('tr')[1:]:
            # Get the text in the first column: "February 2009:"
            (month, year) = row('td')[0].string.split()
            # Strip the colon off.
            year = year[:-1]

            # Scrape the date page for this month and get all its messages.
            # keep_fetching will be True or False, depending on whether we need to keep getting older months.
            keep_fetching = self.scrapeMonth(year+'-'+month)
            
            if not keep_fetching:
                break;
        
        
        
    def scrapeMonth(self, date):
        """
        Scrapes a monthly archive date page and follows through to all the messages listed
        date is a string of the form '2009-February'
        """
        
        # eg http://lists.example.com/mailman/private/list-name/2009-February
        month_url = self.list_url + '/' + date
        
        # Get the directory the month files will be saved in.
        # eg /Users/phil/Sites/examplesite/html/list-name/2009-February
        url_parts = month_url.split('/')
        month_dir = self.publish_dir + '/' + url_parts[-1]
        if not os.path.exists(month_dir):
            os.mkdir(month_dir)

        source = self.fetchMonthFile(month_url, month_dir, 'date.html')
        
        # This will be the source of the date.html page for this month.
        soup = BeautifulSoup(source)

        # Get all the anchors from the list of messages.
        anchors = soup.h1.findNextSibling('ul').findNext('ul').fetch('a')
        anchors.reverse()
        
        # Get all the links to individual message pages.
        keep_fetching = True
        messages_fetched_this_month = 0
        for a in anchors:
            link = a.get('href', '')
            if link:
                # Fetch this message's page and save it.
                # hours will be how many hours ago this message was sent.
                hours = self.scrapeMessage(urlparse.urljoin(month_url+'/', link))
                messages_fetched_this_month += 1  # Count just for this month.
                self.messages_fetched += 1  # Overall count.

                if self.hours_to_go_back > 0 and hours > self.hours_to_go_back and self.messages_fetched >= self.items_for_rss:
                    # We'll send a signal back to scrapeList() that we don't want to get any previous months.
                    keep_fetching = False
                    break
                else:
                    # Pause for half a second so as not to hammer servers too much
                    time.sleep(0.5)
                    
        # Fetch all the non-date index files for this month and save copies.
        # There's been at least one new message, so get new copies of the other index pages.
        if (messages_fetched_this_month == 1 and keep_fetching) or (messages_fetched_this_month > 1):
            for file in ['thread', 'subject', 'author']:
                source = self.fetchMonthFile(month_url, month_dir, file+'.html')
            
            # Get the gzipped file.
            source = self.fetchMonthFile(self.list_url, self.publish_dir, date+'.txt.gz')

        return keep_fetching
        
        
    def fetchMonthFile(self, remote_dir, local_dir, file_name):
        """
        Fetches one of the monthly index pages (date.html, author.html, subject.html, thread.html).
        remote_dir is like http://lists.example.com/mailman/private/list-name/2009-February
        local_dir is like /Users/phil/Sites/examplesite/html/list-name/2009-February
        file_name is like date.html
        """
        
        source = self.fetchPage(remote_dir+'/' + file_name)

        # The copy of the page we save is filtered for email addresses, links, etc.
        filtered_source = self.filterPage(source)

        # Save our local copy.
        # eg /Users/phil/Sites/examplesite/html/list-name/2009-February/date.html
        local_month = open(local_dir + '/' + file_name, 'w')
        local_month.write(filtered_source)
        local_month.close()
        
        # Return the original so that (if it's date.html) we can scrape it for links to messages.
        return source
        
        
    def scrapeMessage(self, message_url):
        """
        Fetches the page for a single message and saves it locally.
        Adds the message to the RSS feed items.
        Returns the number of hours old this message is.
        """
        
        source = self.fetchPage(message_url)

        # Remove all the stuff we don't want.
        source = self.filterPage(source)
        
        # Work out how many hours ago this message was.
        soup = BeautifulSoup(source)

        # The time is in the first <I></I> after the <H1>.
        message_time = time.mktime(email.utils.parsedate(soup.h1.findNextSibling('i').string))
        hours_ago = (time.time() - message_time) / 3600

        # Get the directory the message file is in.
        # It should already have been created in scrapeMonth()
        # eg http://lists.example.com/mailman/private/list-name/2009-February/000042.html
        url_parts = message_url.split('/')
        # eg /Users/phil/Sites/examplesite/html/list-name/2009-February
        message_dir = self.publish_dir + url_parts[-2]
        
        # Save our local copy.
        # eg /Users/phil/Sites/examplesite/html/list-name/2009-February/000042.html
        local_message = open(message_dir + '/' + url_parts[-1], 'w')
        local_message.write(source)
        local_message.close()
        
        # Create the URL for linking to this message from the RSS feed.
        # eg http://www.example.com/list-name/2009-February/000042.html
        local_message_url = self.publish_url + url_parts[-2] + '/' + url_parts[-1]

        if self.messages_fetched < self.items_for_rss:
            # Add this message to the RSS feed items...
            self.addRSSItem(local_message_url, message_time, soup)
        
        return hours_ago
        
        
    def filterPage(self, source):
        "Does all the filtering, removing email addresses, removing quoted portions, etc."

        # Do all the custom search/replaces specified in the config.
        for match, replace in self.match_search_replace.iteritems():
            source = match.sub(replace, source)
            
        if self.filter_email_addresses:
            # Remove all standard emails, eg "billy@nomates.com"
            source = self.match_email.sub(r'', source)
            
            # Remove all email addresses obscured by Mailman, eg "billy at nomates.com"
            source = self.match_text_email.sub(r'', source)
            
            # Remove all mailto: links. Replaces them with '#'
            source = self.match_mailto.sub(r'\1', source)
            source = self.match_mailto_label.sub(r'', source)
        
        # Replace any remaining links to the original list pages with #
        # A bit messy, but just in case.
        # eg, for links to message attachments.
        source = self.match_list_url.sub('#', source)
        
        # Replace the list info url with our custom one from the config
        source = self.match_list_info_url.sub(self.list_info_url, source)
        
        # Strip all the necessary quoted lines
        if self.strip_quotes > 0:
            source = self.match_strip_quotes.sub('', source)

        # Put the custom HTML <head> code in.
        if self.head_html:
            source = self.match_head_html.sub(self.head_html+'</head>', source)
        
        return source
        
       
    def fetchPage(self, url):
        "Used for fetching all the remote pages."
        
        self.message("Fetching " + url)
        
        fp = mechanize.urlopen(url)
        source = fp.read()
        fp.close()
        
        return source


    def smartTruncate(self, content, length=100, suffix='...'):
        "Truncates a string at a word boundary."
        if len(content) <= length:
            return content
        else:
            return content[:length].rsplit(' ',1)[0] + suffix
        
        
    def message(self, text):
        "Output debugging info."
        if self.verbose:
            print text
            
    def error(self, text, fatal=True):
        print text
        if fatal:
            exit()

def main():
    scraper = MailmanArchiveScraper()
    
    scraper.scrape()


if __name__ == "__main__":
    main()
    

