import urllib.parse
import urllib.request
import urllib.error
import requests
import sqlite3
from bs4 import BeautifulSoup
import datetime
import re
import hashlib
from retry import retry
import os
import sys
import time

class UpolCrawler:
    def __init__(self, url, results_path, logs_path, ignored_domains = [], debug = False,):
        self.start_url = self.url_clean(url)
        self.url_queue = set()
        self.url_queue.add(self.start_url)
        self.debug = debug
        self.regex = re.compile(self.url_regex(self.start_url))
        self.logs_path = logs_path
        self.results_path = results_path
        self._db_init()
        self.user_agent = 'Mozilla/5.0'
        self.sleep_time = 0.5
        self.ignored_domains = ignored_domains

    def _db_init(self):
        """Inicializuje pripojeni do databaze"""
        path = os.path.join(self.results_path, "results/"+urllib.parse.urlsplit(self.start_url).netloc + ".db")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.db_connection = sqlite3.connect(path)
        self.db_cursor = self.db_connection.cursor()
        self.db_cursor.execute('''CREATE TABLE IF NOT EXISTS urls
                 (id INTEGER PRIMARY KEY, url TEXT, url_hash TEXT, number_of_inlink INTEGER DEFAULT 0)''')
        self.db_cursor.execute('''CREATE INDEX IF NOT EXISTS index_hash ON urls (url_hash)''')
        self.db_connection.commit()

    def _db_close(self):
        """Ukoncuje spojeni s databazi"""
        self.db_connection.close()

    def db_url_insert(self, url):
        """Vklada url do databaze"""
        url = self.url_decode(url)
        url_hash = self.url_hash(url)
        query = (url, url_hash)

        self.db_cursor.execute('''INSERT INTO urls (url, url_hash) VALUES (?,?)''', query)
        self.db_connection.commit()

    def db_url_exists(self, url):
        """Zjistuje zda je url v databazi"""
        url = self.url_decode(url)
        url_hash = self.url_hash(url)
        query = (url_hash,)

        self.db_cursor.execute('SELECT EXISTS(SELECT 1 FROM urls WHERE url_hash=? LIMIT 1)', query)
        result = self.db_cursor.fetchone()

        return result[0] == 1

    def db_url_iterate_inlinks(self, url):
        """Zvetsuje pocet inlinku o 1 pro danou url v databazi"""
        url = self.url_decode(url)
        url_hash = self.url_hash(url)
        query = (url_hash,)

        self.db_cursor.execute('UPDATE urls SET number_of_inlink = number_of_inlink + 1 WHERE url_hash = ?', query)
        self.db_connection.commit()

    # @retry(urllib.error.URLError, tries=2, delay=3, backoff=2)
    # def url_request(self, url):
    #     return urllib.request.Request(
    #         url,
    #         headers={'User-Agent': self.user_agent}
    #     )

    # @retry(urllib.error.URLError, tries=2, delay=3, backoff=2)
    # def url_open(self, request):
    #     return urllib.request.urlopen(request)

    # def url_connect(self, url):
    #     """Ziska request, response a url pro danou url, resi i pripadny redirect"""
    #     url = self.url_clean(self.url_decode(url))
    #     request = self.url_request(url)
    #     response = self.url_open(request)
    #     response_url = response.geturl()
    #     response_url = self.url_clean(response_url)
    #     return request, response, response_url

    def url_request(self, url):
        headers = {'user-agent': self.user_agent}
        response = requests.get(url, headers=headers, verify=False)
        return response

    def url_connect(self, url):
        request = None
        response = self.url_request(url)
        response_url = self.url_clean(response.url)
        return request, response, response_url

    def url_remove_www(self, url):
        """Odstrani z url WWW"""
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        netloc = netloc.replace("www.","")
        return urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))

    def url_encode(self, url):
        """Enkoduje zvlastni znaky v URL a hradi je %"""
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        path = urllib.parse.quote(path, '/%')
        qs = urllib.parse.quote_plus(qs, ':&=')
        return urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))

    def url_decode(self, url):
        """Dekoduje url adresu zpet na specialni znaky"""
        # decoded_url = urllib.parse.unquote(url)
        # return decoded_url
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        path = urllib.parse.unquote(path)
        qs = urllib.parse.unquote_plus(qs)
        return urllib.parse.urlunsplit((scheme, netloc, path, qs, anchor))

    def url_clean(self, url):
        """Vycisti URL, odstrani www a konecne lomitko"""
        url = self.url_remove_www(url)
        url = url.rstrip('/')
        return url

    # def url_remove_path(self, url):
    #     scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
    #     return scheme + "://" + netloc

    def url_hash(self, url):
        """Vrati hash odpovidajici url"""
        return hashlib.sha1(url.encode('utf-8')).hexdigest()

    def url_regex(self, url):
        """Generuje regex pro urcitou url"""
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        netloc = netloc.replace(".", "\.")

        return "^https?:\/\/([a-z0-9]+[.])*"+netloc+".*$"

    def url_validator(self, url):
        """Validuje url, regex, typ souboru, kotva a dalsi"""
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        path_split = path.split('/')

        for ignored_domain in self.ignored_domains:
            if ignored_domain in url:
                return False

        if not self.regex.match(url):
            return False

        if ("." in path_split[-1]):
            if (not ".php" in path_split[-1]) and (not ".html" in path_split[-1]) and (not ".xml" in path_split[-1]):
                return False

        if anchor:
            return False

        return True

    def url_validator_phpbb(self, url):
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        path = path+qs+anchor

        url_keywords = ["posting.php", "ucp.php", "view=print", "memberlist.php", "mark"]

        for url_keyword in url_keywords:
            if url_keyword in path:
                return False

        return True

    def url_validator_wiki(self, url):
        scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(url)
        path = path+qs+anchor

        url_keywords = ["&"]

        for url_keyword in url_keywords:
            if url_keyword in path:
                return False

        return True

    def is_url_absolute(self, url):
        """Testuje zda je url absolutni"""
        return bool(urllib.parse.urlparse(url).netloc)

    def is_wiki(self, soup):
        """Zjistuje zda je soup(parsovana stranka) wikipedie"""
        # <meta name="generator" content="MediaWiki 1.26.2">
        meta_generators = soup.find_all('meta', {'name':'generator'})

        for meta_generator in meta_generators:
            content = meta_generator['content']
            if "MediaWiki" in content:
                return True

        return False

    def is_phpbb(self, soup):
        """Zjistuje zda je soup(parsovana stranka) phpbb forum"""
        return (soup.find('body', id='phpbb') != None)


    def page_base_url(self, soup, url):
        """Overuje zda soup(parsovana stranka) obsahuje tag base_tag a pripadne ho pouzije jako base_url"""
        base_url = url
        base_tag = soup.find_all('base', href=True)

        if len(base_tag) > 0:
            base_url = base_tag[0]['href']

        return base_url

    def page_urls(self, soup, url):
        """Vraci vsechny odkazy na strance po vyfiltrovani"""
        urls_on_page = set()
        base_url = self.page_base_url(soup, url)

        if self.is_wiki(soup):
            print("IS WIKI!")
            content_div = soup.find('div', id='content')

            for div in content_div.find_all('div', {'class':'printfooter'}):
                div.decompose()

            links_tmp = content_div.find_all('a', href=True)
            links = set()

            for link in links_tmp:
                if self.url_validator_wiki(link['href']):
                    links.add(link)


        elif self.is_phpbb(soup):
            print("IS PHPBB!")
            content_div = soup.find('div', id='page-body')

            for p in content_div.find_all('p', {'class':'jumpbox-return'}):
                p.decompose()

            links_tmp = content_div.find_all('a', href=True)
            links = set()

            for link in links_tmp:
                if self.url_validator_phpbb(link['href']):
                    links.add(link)

        else:
            links = soup.find_all('a', href=True)

        for a in links:
            page_url = a['href']

            if not self.is_url_absolute(page_url):
                page_url = urllib.parse.urljoin(base_url, page_url)

            page_url = self.url_clean(page_url)

            if self.url_validator(page_url):
                urls_on_page.add(page_url)
            else:
                if self.debug:
                    path = os.path.join(self.logs_path, "logs/"+urllib.parse.urlsplit(self.start_url).netloc+"_not_valid.txt")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "a") as not_valid_file:
                        not_valid_file.write(page_url + "\n")

        return urls_on_page

    def crawl_url(self, url):
        # print("URL: ", url)
        try:
            request, response, url = self.url_connect(url)
        # except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.InvalidURL, requests.exceptions.ChunkedEncodingError, requests.exceptions.SSLError, requests.exceptions.ConnectTimeout, requests.exceptions.URLRequired, ) as exception:
        except:
            if self.debug:
                path = os.path.join(self.logs_path, "logs/"+urllib.parse.urlsplit(self.start_url).netloc+"_error.txt")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "a") as error_file:
                    error_file.write(url + "\n")
                    # if hasattr(exception, 'reason'):
                    #     error_file.write('We failed to reach a server.')
                    #     error_file.write("\n")
                    #     error_file.write(str(exception.reason))
                    #     error_file.write("\n")
                    # elif hasattr(exception, 'code'):
                    #     error_file.write('The server couldn\'t fulfill the request.')
                    #     error_file.write("\n")
                    #     error_file.write(str(exception.code))
                    #     error_file.write("\n")
        else:
            # print("RESPONSE: ", url)
            if not self.url_validator(url):
                # Pokud neni validni preskoc, muze nastat po redirectu
                return

            if self.db_url_exists(url):
                self.db_url_iterate_inlinks(url)
                return
            else:
                self.db_url_insert(url)

                if self.debug:
                    # print("URL: " + self.url_decode(url))
                    path = os.path.join(self.logs_path, "logs/"+urllib.parse.urlsplit(self.start_url).netloc+"_logs.txt")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "a") as log_file:
                        log_file.write("URL: " + self.url_decode(url))
                        log_file.write("\n")
                    # print("URL: " + url)

            # html = response.read()
            html = response.text
            soup = BeautifulSoup(html, "lxml")

            urls_on_page = self.page_urls(soup, url)

            for page_url in urls_on_page:
                if (self.db_url_exists(page_url)) or (page_url in self.url_queue):
                    self.db_url_iterate_inlinks(page_url)
                else:
                    self.url_queue.add(page_url)

    def start(self):
        start_time = datetime.datetime.now()

        if self.debug:
            number = 0

        while (len(self.url_queue) != 0):
            url = self.url_queue.pop()
            self.crawl_url(url)
            if self.debug:
                number = number + 1
                path = os.path.join(self.logs_path, "logs/"+urllib.parse.urlsplit(self.start_url).netloc+"_logs.txt")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "a") as log_file:
                    log_file.write("#" + str(number))
                    log_file.write("\n")

            time.sleep(self.sleep_time)

        end_time = datetime.datetime.now()
        elapsed = end_time - start_time

        if self.debug:
            path = os.path.join(self.logs_path, "logs/"+urllib.parse.urlsplit(self.start_url).netloc+"_result.txt")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as result_file:
                result_file.write("Number of pages:")
                result_file.write(str(number))
                result_file.write("\nTime:")
                result_file.write(str(elapsed))

        print("DONE")
        print("TIME: " + str(elapsed))

        self._db_close()
