import redis
import datetime as dt   
from celery import Celery
import json, re
import redis
import datetime as dt
import uuid
from logging_config import appLogger as logger


celery_cleaner = Celery('celery_clean', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
celery_cleaner.conf.task_routes = {
    'reddit_cleaning_queue': {
        'queue': 'reddit_cleaning_queue',
        'routing_key': 'reddit_cleaning_queue'
    },
    'twitter_cleaning_queue': {
        'queue': 'twitter_cleaning_queue',
        'routing_key': 'twitter_cleaning_queue'
    },
    'news_cleaning_queue': {
        'queue': 'news_cleaning_queue',
        'routing_key': 'news_cleaning_queue'
    },
    'combined_save_queue': {
        'queue': 'combined_save_queue',
        'routing_key': 'combined_save_queue'
    }
}

celery_cleaner.conf.update(
    worker_prefetch_multiplier=12
)
redis_client = redis.Redis(host='redis', port=6379, db=0)

        
reddit_recent_articles, reddit_top_articles, reddit_top_authors = [], [], []
@celery_cleaner.task(name="celery_cleaner.clean_reddit_api", queue='reddit_cleaning_queue')
def clean_reddit_api(r, sorting, user_id):
    if sorting == "top":
        reddit_top_articles.clear()
        reddit_top_authors.clear()
    elif sorting == "new":
        reddit_recent_articles.clear()
        
    for post in r['data']:
        post_data = {
            'created': dt.datetime.fromtimestamp(post['data']['created']).strftime('%Y-%m-%d %H:%M:%S'),
            'subreddit': post['data']['subreddit'],
            'upvote': post['data']['ups'],
            'title': post['data']['title'],
            'self_text': post['data']['selftext'],
            'author': post['data']['author'],
            'author_id': post['data']['author_id'],
            'possbily_sensitive': post['data']['over_18'],
            'media': [post['data']['url']]
        }
        
        if sorting == "top":
            reddit_top_articles.append({
            'id': post['data']['id'],
            'created': post_data['created'],
            'title': re.sub(r'\n', '<br/>', post_data['title']),
            'text': re.sub(r'\n', '<br/>', post_data['self_text']), 
            'upvote': post_data['upvote'], 
            'media': post_data['media'],
            'possibly_sensitive': post_data['possbily_sensitive'],
            'sentiment': None
            })
            reddit_top_authors.append({
            'id': post_data['author_id'],
            'username': post_data['author'],
            'subreddit': post_data['subreddit'],
            'upvote': post_data['upvote'],
            'url': f"https://www.reddit.com/user/{post_data['author']}",
            'platform': "Reddit",
            'followers': None,
            'following': None,
            'posts': None,
            'full_name': None
            })
        elif sorting == "new":
            reddit_recent_articles.append({
            'id': post['data']['id'],
            'created': post_data['created'],
            'title': re.sub(r'\n', '<br/>', post_data['title']),
            'text': re.sub(r'\n', '<br/>', post_data['self_text']), 
            'upvote': post_data['upvote'], 
            'media': post_data['media'],
            'possibly_sensitive': post_data['possbily_sensitive'],
            'sentiment': None
            })
            

    sorted_top_authors = sorted(reddit_top_authors, key=lambda x: x['upvote'], reverse=True)
    sorted_recent_articles = sorted(reddit_recent_articles, key=lambda x: x['created'], reverse=True)
    sorted_top_articles = sorted(reddit_top_articles, key=lambda x: x['upvote'], reverse=True)
    
    
    
    logger.info(f"sorted_top_articles: {len(sorted_top_articles)}, sorted_recent_articles: {len(sorted_recent_articles)}, sorted_top_authors: {len(sorted_top_authors)}")
        
    combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles, "top_authors": sorted_top_authors}, indent=4)      
    redis_client.hset(str(user_id), mapping={"r_api": combined_data})
    
    return sorted_top_articles, sorted_recent_articles, sorted_top_authors
    


news_top_authors, news_top_articles, news_recent_articles = [], [], []
@celery_cleaner.task(name="celery_cleaner.clean_news_api", queue='news_cleaning_queue')
def clean_news_api(r, sorting, user_id):
    if sorting == "top":
        news_top_articles.clear()
        news_top_authors.clear()
    elif sorting == "new":
        news_recent_articles.clear()
        
    logger.info(f"Cleaning news data started")
    for article in r['articles']:
        if sorting == "new":
            article_data_new = {
                'id': str(uuid.uuid4()),
                'created': dt.datetime.fromisoformat(article['publishedAt']).strftime('%Y-%m-%d %H:%M:%S'),
                'title': article['title'] if article['title'] != "[Removed]" else None,
                'description': article['description'] if article['description'] != "[Removed]" else None,
                'media': [article['urlToImage']] if article['urlToImage'] != "[Removed]" else [],
                'source': article['source']['name'] if article['source']['name'] != "[Removed]" else None,
                'sentiment': None
                }
            news_recent_articles.append(article_data_new)
        elif sorting == "top":
            news_top_authors.append({
                'id': str(uuid.uuid4()),
                'username': article['author'] if article['author'] != "[Removed]" else None,
                'platform': "News",
                'full_name': None,
                'followers': None,
                'following': None,
                'posts': None,
                'url': None,        
                                  
            })
            
            article_data_top = {
                'id': str(uuid.uuid4()),
                'created': dt.datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
                'title': article['title'] if article['title'] != "[Removed]" else None,
                'description': article['description'] if article['description'] != "[Removed]" else None,
                'media': article['urlToImage'],
                'media': [article['urlToImage']] if article['urlToImage'] != None else [],
                'source': article['source']['name'] if article['source']['name'] != "[Removed]" else None,
                'sentiment': None
                }
        
            news_top_articles.append(article_data_top)

    
    # logger.info(f"Cleaning news data completed")
    # sentiment_top_data = sentiment_analysis_news(news_top_articles, "top")
    # sentiment_new_data = sentiment_analysis_news(news_recent_articles, "new")
    # logger.info(f"Sentiment analysis for news data completed")

    combined_data = json.dumps({"top_posts": news_top_articles, "recent_posts": news_recent_articles, "top_authors": news_top_authors}, indent=4)      
        
    redis_client.hset(str(user_id), mapping={"news_api": combined_data})
    
    return news_top_articles, news_recent_articles, news_top_authors
        
    

twitter_top_authors, twitter_top_articles, twitter_recent_articles = [], [], []
@celery_cleaner.task(name="celery_cleaner.clean_twitter_api", queue='twitter_cleaning_queue')
def clean_twitter_api(r, sorting, user_id):
    logger.info("Cleaning twitter data started")
    if sorting == "top":
        twitter_top_articles.clear()
        twitter_top_authors.clear()
    elif sorting == "new":
        twitter_recent_articles.clear()
        
    length_of_data = len(r['data'])
    author_map = {user['id']: user for user in r['includes']['users']}
    if 'media' in r['includes']:
        media_map = {}
        for media in r['includes']['media']:
            if 'preview_image_url' in media:
                media_map[media['media_key']] = media['preview_image_url']
            else:
                continue
    else:
        media_map = []
    
    parsed_data = {}

    for i in range(length_of_data):
        flag = r['data'][i]['id'] in parsed_data.keys()
        if not flag:
            tweet_id = r['data'][i]['id']
            author_id = r['data'][i]['author_id']
            parsed_data[tweet_id] = r['data'][i]

            try:
                list_img_url = []
                media_keys = r['data'][i]['attachments']['media_keys']
                if media_map is not None:
                    for media_key in media_keys:
                        list_img_url.append(media_map[media_key])
                    parsed_data[tweet_id]['media'] = list_img_url

            except Exception:
                print('No media key found')
            
            parsed_data[tweet_id]['author'] = author_map[author_id]
        tweet_data = {
            'created': dt.datetime.fromisoformat(parsed_data[tweet_id]['created_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
            'id': parsed_data[tweet_id]['id'],
            'likes': parsed_data[tweet_id]['public_metrics']['like_count'],
            'note_tweet': parsed_data[tweet_id]['note_tweet']['text'] if 'note_tweet' in parsed_data[tweet_id] else None,
            'text': parsed_data[tweet_id]['text'] if parsed_data[tweet_id]['text'] != None else None,
            'media': parsed_data[tweet_id]['media'] if 'media' in parsed_data[tweet_id] else [],
            'author': parsed_data[tweet_id]['author'],
            'possibly_sensitive': parsed_data[tweet_id]['possibly_sensitive'],
            'author_id': author_id,
            
        }

        if sorting == "top":
            twitter_top_articles.append({
                'id': tweet_data['id'],
                'created': tweet_data['created'],
                'text': tweet_data['text'], 
                'note_tweet': tweet_data['note_tweet'],
                'likes': tweet_data['likes'], 
                'media': tweet_data['media'],
                'possibly_sensitive': tweet_data['possibly_sensitive'],
                'sentiment': None
            })
            twitter_top_authors.append({
                'id': tweet_data['author_id'],
                'username': tweet_data['author']['username'],
                'likes': tweet_data['likes'],
                'url': f"https://x.com/{tweet_data['author']['username']}",
                'followers': parsed_data[tweet_id]['author']['public_metrics']['followers_count'],
                'following': parsed_data[tweet_id]['author']['public_metrics']['following_count'],
                'posts': parsed_data[tweet_id]['author']['public_metrics']['tweet_count'],
                'full_name': parsed_data[tweet_id]['author']['name'],
                'platform': "X"
                
            })
        elif sorting == "new":
            twitter_recent_articles.append({
                'id': tweet_data['id'],
                'created': tweet_data['created'],
                'text': tweet_data['text'], 
                'note_tweet': tweet_data['note_tweet'],
                'likes': tweet_data['likes'], 
                'media': tweet_data['media'],
                'possibly_sensitive': tweet_data['possibly_sensitive'],
                'sentiment': None
            })
    
    sorted_top_articles = sorted(twitter_top_articles, key=lambda x: x['likes'], reverse=True)
    sorted_top_authors = sorted(twitter_top_authors, key=lambda x: x['likes'], reverse=True)
    sorted_recent_articles = sorted(twitter_recent_articles, key=lambda x: x['created'], reverse=True)
    logger.info("Cleaning twitter data completed")
    
    # sentiment_top_data = sentiment_analysis_twitter(sorted_top_articles, "top")
    # sentiment_new_data = sentiment_analysis_twitter(sorted_recent_articles, "new")
    logger.info("Sentiment analysis for twitter data completed")

    combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles, "top_authors": sorted_top_authors}, indent=4)

    redis_client.hset(str(user_id), mapping={"x_api": combined_data})
    logger.info("Twitter data saved to redis")
    
    return sorted_top_articles, sorted_recent_articles
    


