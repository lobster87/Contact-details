import logging
import os
import pandas as pd
import re
import scrapy
import string
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from googlesearch import search
logging.getLogger('scrapy').propagate = False
import sys
sys.path.append('contactDetails/Database')
from database import *


def get_urls(tag, n, language):
    urls = [url for url in search(tag, stop=n, lang=language)][:n]
    return urls

class MailSpider(scrapy.Spider):
    
    name = 'email'
    
    def parse(self, response):
        
        links = LxmlLinkExtractor(allow=()).extract_links(response)
        links = [str(link.url) for link in links]
        links.append(str(response.url))
        
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_link) 
            
    def parse_link(self, response):
        
        for word in self.reject:
            if word in str(response.url):
                return

        #text = str(response.text)    
        html_text = str(response.text)
        mail_list = re.findall('\w+@\w+\.{1}\w+.{1}\w+', html_text)
        """
        text = text.translate(str.maketrans('','', string.punctuation))
        text = text.split()


        words_found = []

        for word in text:
            found = querydb('Job_names',word)
            try:
                words_found.append(found[0][0])
            except:
                pass

        words_found = list(dict.fromkeys(words_found))
        """
        dic = {'email': mail_list, 'link': '=HYPERLINK("{}")'.format(response.url)}
        df = pd.DataFrame(dic)
        
        df.to_csv(self.path, mode='a', header=False)
        df.to_csv(self.path, mode='a', header=False)

def ask_user(question):
    response = input(question + ' y/n' + '\n')
    if response == 'y':
        return True
    else:
        return False
def create_file(path):
    response = False
    if os.path.exists(path):
        response = ask_user('File already exists, replace?')
        if response == False: return 
    
    with open(path, 'wb') as file: 
        file.close()

def allowed_domains(urls):
    domains = []
    
    for url in urls:
        link = url.split('/')
        domain = link[2]
        domain = domain.split('www.')
        if len(domain) == 1:
            domains.append(domain[0])
        else:
            domains.append(domain[1])
    
    return domains


def get_info(path, reject=[]):
    
    create_file(path)
    df = pd.DataFrame(columns=['email', 'link'], index=[0])
    df.to_csv(path, mode='w', header=True)
    
    print('Collecting Google urls...')

    
    urls = pd.read_csv('contactDetails/urls.csv')
    google_urls = []
    [google_urls.append(url) for url in urls.iloc[:,0]]

    domains = allowed_domains(google_urls)
    print('Searching for emails...')
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/5.0'})
    process.crawl(MailSpider, start_urls=google_urls, path=path, reject=reject, allowed_domains=domains)
    process.start()
    
    print('Cleaning emails...')
    df = pd.read_csv(path, index_col=0)
    df.columns = ['email', 'link']
    df = df.drop_duplicates(subset=['email'])
    df = df.sort_values(by='link')
    df = df.reset_index(drop=True)
    df.to_csv(path, mode='w', header=True)
    
    return df

bad_words = ['facebook', 'instagram', 'youtube', 'twitter', 'wiki']
df = get_info('contact.csv', reject=bad_words)
df.head()