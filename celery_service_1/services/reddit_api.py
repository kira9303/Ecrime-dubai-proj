import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 
import praw
import os
from dotenv import load_dotenv
from logging_config import appLogger as logger

load_dotenv()

recent_articles:dict = {}
top_articles:dict = {}
top_authors:dict = {}

class RedditFetch:
    def __init__(self):
        def create_custom_session():
            session = requests.Session()
            retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            return session
        custom_session = create_custom_session()
        self.reddit = praw.Reddit(
                client_id=os.environ.get("REDDIT_CLIENT_ID"),
                client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
                password=os.environ.get("REDDIT_PASSWORD"),
                user_agent="script:praw:v1.0 (by u/Thick-Analyst-3061)",
                username="Thick-Analyst-3061",
                requestor_kwargs={'session': custom_session}
                )
        # logger.info(f"{os.environ["REDDIT_CLIENT_ID"]}")
        # logger.info(f"{os.environ["REDDIT_CLIENT_SECRET"]}")
        # logger.info(f"{os.environ["REDDIT_PASSWORD"]}")
        self.subreddit_name = "all"
        self.limit = 10
        self.timeframe = 'week'

    def fetch_top(self, keyword_list):  
        print("keyword_list: ", keyword_list)
        subreddit = self.reddit.subreddit(self.subreddit_name)
        posts = subreddit.search(query=' OR '.join(keyword_list), sort='top', time_filter=self.timeframe, limit=self.limit)
        result = {'data': []}
        for post in posts:
            post_data = {
                'data': {
                    'created': post.created_utc,
                    'subreddit': post.subreddit.display_name,
                    'subreddit_id': post.subreddit_id,
                    'title': post.title,
                    'id': post.id,
                    'ups': post.ups,
                    'selftext': post.selftext,
                    'author': post.author.name if post.author else '[deleted]',
                    'over_18': post.over_18,
                    'url': post.url,
                    'author_id': post.author.id
                }
            }
            result['data'].append(post_data)
        # logger.info(f"results: {len(result['data'])}")
        return result
        
        
    def fetch_new(self, keyword_list):
        subreddit = self.reddit.subreddit(self.subreddit_name)
        posts = subreddit.search(query=' OR '.join(keyword_list), sort='new', time_filter=self.timeframe, limit=self.limit)
        result = {'data': []}
        for post in posts:
            logger.info(f"post: {post}")
            post_data = {
                'data': {
                    'created': post.created_utc,
                    'subreddit': post.subreddit.display_name,
                    'subreddit_id': post.subreddit_id,
                    'title': post.title,
                    'id': post.id,
                    'ups': post.ups,
                    'selftext': post.selftext,
                    'author': post.author.name if post.author else '[deleted]',
                    'over_18': post.over_18,
                    'url': post.url,
                    'author_id': post.author.id
                }
            }
            result['data'].append(post_data)

        return result
    
    
    
