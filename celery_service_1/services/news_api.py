#from newsapi import NewsApiClient
from newsapi.newsapi_client import NewsApiClient
from dotenv import load_dotenv
import os
import json
import redis

load_dotenv()

class NewsAPIFetch:
    def __init__(self):
        self.api_key = os.environ.get("NEWS_API_KEY")
        self.newsapi = NewsApiClient(self.api_key)

    def fetch_new(self, keyword_list):
        all_articles_new = self.newsapi.get_everything(q='bitcoin',
                                      language='en',
                                      sort_by='publishedAt',
                                      page=1)
        return all_articles_new

    def fetch_top(self, keyword_list):
        all_articles_top = self.newsapi.get_everything(q='bitcoin',
                                      language='en',
                                      sort_by='popularity',
                                      page=1)

        return all_articles_top
