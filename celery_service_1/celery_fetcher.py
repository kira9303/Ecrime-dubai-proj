from celery import Celery
from services.reddit_api import RedditFetch
from services.news_api import NewsAPIFetch
from services.twitter_api import TwitterAPIFetch

from logging_config import appLogger as logger


celery_fetcher = Celery('celery_fetch', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
celery_fetcher.conf.task_routes = {
    'reddit_fetch_queue': {
        'queue': 'reddit_fetch_queue',
        'routing_key': 'reddit_fetch_queue'
    },
    'twitter_fetch_queue': {
        'queue': 'twitter_fetch_queue',
        'routing_key': 'twitter_fetch_queue'
    },
    'news_fetch_queue': {
        'queue': 'news_fetch_queue',
        'routing_key': 'news_fetch_queue'
    },
    'facebook_fetch_queue': {
        'queue': 'facebook_fetch_queue',
        'routing_key': 'facebook_fetch_queue'
    },
    'instagram_fetch_queue': {
        'queue': 'instagram_fetch_queue',
        'routing_key': 'instagram_fetch_queue'
    }
}

celery_fetcher.conf.update(
    worker_prefetch_multiplier=6
)

@celery_fetcher.task(name="celery_fetcher.fetch_rapi", queue="reddit_fetch_queue")
def fetch_rapi(user_id, keyword):
    reddit_fetch_obj = RedditFetch()

    result_top = reddit_fetch_obj.fetch_top(keyword)
    result_new = reddit_fetch_obj.fetch_new(keyword)

    logger.info(f"Reddit data fetched for {len(result_top['data'])} and {len(result_new['data'])}")

    # Start cleaning tasks and return their IDs
    cleaned_data_top_task = celery_fetcher.send_task("celery_cleaner.clean_reddit_api", args=(result_top, 'top', user_id), queue="reddit_cleaning_queue")
    cleaned_data_recent_task = celery_fetcher.send_task("celery_cleaner.clean_reddit_api", args=(result_new, 'new', user_id), queue="reddit_cleaning_queue")  


    while not cleaned_data_recent_task.ready() or not cleaned_data_top_task.ready():
        pass
    
    

@celery_fetcher.task(name="celery_fetcher.fetch_news_api", queue='news_fetch_queue')
def fetch_news_api(user_id, keyword):
    news_api_obj = NewsAPIFetch()
    
    result_new = news_api_obj.fetch_new(keyword)
    result_top = news_api_obj.fetch_top(keyword)

    cleaned_data_top = celery_fetcher.send_task("celery_cleaner.clean_news_api", args=(result_top, "top", user_id), queue="news_cleaning_queue")
    cleaned_data_new = celery_fetcher.send_task("celery_cleaner.clean_news_api", args=(result_new, "new", user_id), queue="news_cleaning_queue")
    
    while not cleaned_data_top.ready() and not cleaned_data_new.ready():
        pass
    

@celery_fetcher.task(name="celery_fetcher.fetch_twitter", queue='twitter_fetch_queue')
def fetch_twitter_api(user_id, keyword):
    news_api_obj = TwitterAPIFetch(keyword)
    
    result_new = news_api_obj.fetch_new()
    result_top = news_api_obj.fetch_top()

    cleaned_data_top = celery_fetcher.send_task("celery_cleaner.clean_twitter_api", args=(result_top, 'top', user_id), queue="twitter_cleaning_queue")
    cleaned_data_new = celery_fetcher.send_task("celery_cleaner.clean_twitter_api", args=(result_new, 'new', user_id), queue="twitter_cleaning_queue")
    
    
    while not cleaned_data_new.ready() or not cleaned_data_top.ready():
        pass
    
    

