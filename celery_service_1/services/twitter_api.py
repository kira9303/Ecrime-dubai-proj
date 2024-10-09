import requests
import os
import json
from dotenv import load_dotenv
import datetime as dt
load_dotenv()


class TwitterAPIFetch:
    def __init__(self, keyword_list):
        self.bearer_token = os.environ.get("TWIITER_BEARER_TOKEN")
        print(self.bearer_token)  
        self.search_url = "https://api.twitter.com/2/tweets/search/recent"
        self.query_params_new = {
            'query': f"{keyword_list}",
            'tweet.fields': 'id,text,attachments,author_id,created_at,public_metrics,note_tweet,possibly_sensitive',
            'media.fields': 'media_key,preview_image_url,url',
            'expansions': 'author_id,attachments.media_keys',
            'sort_order': 'recency',
            'user.fields': 'username,name,url,public_metrics',
            'max_results': '10'
        }


        self.query_params_top = {
            'query': f"{keyword_list}",
            'tweet.fields': 'id,text,attachments,author_id,created_at,public_metrics,note_tweet,possibly_sensitive',
            'media.fields': 'media_key,preview_image_url,url',
            'expansions': 'author_id,attachments.media_keys',
            'sort_order': 'relevancy',
            'user.fields': 'username,name,url,public_metrics',
            'max_results': '10'
        }

    
    def bearer_oauth(self, r):
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2RecentSearchPython"
        return r
    
    def connect_to_endpoint(self, url, params):
        response = requests.get(url, auth=self.bearer_oauth, params=params)
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()
    

    def fetch_new(self):
        json_response_new = self.connect_to_endpoint(self.search_url, self.query_params_new)
        return json_response_new
    
    def fetch_top(self):
        json_response_top = self.connect_to_endpoint(self.search_url, self.query_params_top)
        return json_response_top
    
