from ast import parse
from typing import final

import redis
import datetime as dt   
from celery import Celery
import json, re
import redis
import datetime as dt
import uuid
from logging_config import appLogger as logger
import os
from openai import OpenAI
import uuid




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
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def check_reddit(data):
    if(data["title"] is not None and data["self_text"] is not None and data["media"] is not None and data["author"] is not None and data["subreddit"] is not None):
        return True
    else:
        return False



#def filter_text(data):

def gen_title(data, tweet_id):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    final_text = None
    if 'note_tweet' in data[tweet_id]:
        if(data[tweet_id]["text"] is not None and data[tweet_id]["note_tweet"]["text"] is not None):
            final_text = data[tweet_id]["note_tweet"]


        elif ( data[tweet_id]["note_tweet"]["text"] is None and data[tweet_id]["text"] is not None):
            final_text = data[tweet_id]["text"]
    if 'note_tweet' not in data[tweet_id]:
        final_text = data[tweet_id]["text"]

    prompt_template = f"""Return appropriate title for the below summary in only 10 words max strictly.

                          Summary: {final_text}

    """

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo-0125",
        messages=[
            {"role": "user", "content": prompt_template}
        ]
    )
    response = completion.choices[0].message.content

    return response


        
# reddit_recent_articles, reddit_top_articles, reddit_top_authors = [], [], []
@celery_cleaner.task(name="celery_cleaner.clean_reddit_api", queue='reddit_cleaning_queue')
def clean_reddit_api(r, sorting, user_id):
    reddit_recent_articles, reddit_top_articles, reddit_top_authors = [], [], []

    if sorting == "top":
        reddit_top_articles.clear()
        reddit_top_authors.clear()
    elif sorting == "new":
        reddit_recent_articles.clear()
        
    for post in r['data']:
        post_data = {
            #'created': dt.datetime.fromtimestamp(post['data']['created']).strftime('%Y-%m-%d %H:%M:%S'),
            'created': post['data']['created'],
            'subreddit': post['data']['subreddit'],
            'upvote': post['data']['ups'],
            'title': post['data']['title'],
            'self_text': post['data']['selftext'],
            'author': post['data']['author'],
            #'author_id': post['data']['author_id'],
            'possbily_sensitive': post['data']['over_18'],
            'media': [post['data']['url']],
            'url': [post['data']['url']]
        }
        
        if sorting == "top":
            reddit_top_articles.append({
            'id': post['data']['id'],
            'created_at': post_data['created'],
            'title': re.sub(r'\n', '<br/>', post_data['title']),
            'text': re.sub(r'\n', '<br/>', post_data['self_text']), 
            'upvote': post_data['upvote'], 
            'media': post_data['media'],
            'possibly_sensitive': post_data['possbily_sensitive'],
            'sentiment': None,
            'url': post_data['url']
            })
            reddit_top_authors.append({
            'id': str(uuid.uuid4()),
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
            logger.info(f"reddit_top_articles: {len(reddit_top_authors)} reddit_top_authors: {len(reddit_top_authors)}")
        elif sorting == "new":
            reddit_recent_articles.append({
            'id': post['data']['id'],
            'created_at': post_data['created'],
            'title': re.sub(r'\n', '<br/>', post_data['title']),
            'text': re.sub(r'\n', '<br/>', post_data['self_text']), 
            'upvote': post_data['upvote'], 
            'media': post_data['media'],
            'possibly_sensitive': post_data['possbily_sensitive'],
            'sentiment': None,
            'url': post_data['url']
            })
            logger.info(f"reddit_recent_articles: {len(reddit_recent_articles)}")


    data_fetch = redis_client.hgetall(str(user_id))
    # logger.info(f"printing the entire fetched data: {data_fetch}")




    sorted_top_authors = sorted(reddit_top_authors, key=lambda x: x['upvote'], reverse=True)
    sorted_top_articles = sorted(reddit_top_articles, key=lambda x: x['upvote'], reverse=True)
    sorted_recent_articles = sorted(reddit_recent_articles, key=lambda x: x['created_at'], reverse=True)
    # logger.info(f"Write the entire recent arcticles: {sorted_recent_articles}")
    logger.info(f"length of the sorted_top_authors: {len(sorted_top_authors)} sorted_top_articles: {len(sorted_top_articles)} sorted_recent_articles: {len(sorted_recent_articles)}")

    fetch_data = redis_client.hget(str(user_id), 'r_api')

    if fetch_data:
        fetch_data = json.loads(fetch_data)  # Convert JSON string to a Python dictionary

        # Step 2: Modify the dictionary based on sorting type
        if sorting == "top":
            fetch_data["top_posts"][:] = sorted_top_articles
            fetch_data["top_authors"][:] = sorted_top_authors
        elif sorting == "new":
            fetch_data["recent_posts"][:] = sorted_recent_articles
        else:
            print(f"Invalid sorting type: {sorting}")
            return

        # Step 3: Serialize the updated dictionary back to a JSON string and store it in Redis
        redis_client.hset(str(user_id), "r_api", json.dumps(fetch_data, indent=4))
    else:
        print(f"No data found for user_id: {user_id}")

    #logger.info(f"sorted_top_articles: {len(sorted_top_articles)}, sorted_recent_articles: {len(sorted_recent_articles)}, sorted_top_authors: {len(sorted_top_authors)}")

    '''key = str(user_id)
    if redis_client.exists(key):
        reddit_data = redis_client.hget(str(user_id), "r_api")
        logger.info(f"print current hgetall data {reddit_data}")
        if(sorting == "top"):
            reddit_data["top_posts"] = sorted_top_articles
            reddit_data["top_authors"] = sorted_top_authors

            combined_data = json.dumps(reddit_data)

            #redis_client.hset(str(user_id), mapping={"r_api": combined_data})

            redis_client.hset(str(user_id), "r_api", combined_data)

        if(sorting == "new"):
            reddit_data["recent_posts"] = sorted_recent_articles

            combined_data = json.dumps(reddit_data)

            #redis_client.hset(str(user_id), mapping={"r_api": combined_data})

            redis_client.hset(str(user_id), "r_api", combined_data)

    else:
        combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles,
                                    "top_authors": sorted_top_authors}, indent=4)
        redis_client.hset(str(user_id), mapping={"r_api": combined_data})'''




    #combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles,
    #                            "top_authors": sorted_top_authors}, indent=4)

    #redis_client.hset(str(user_id), mapping={"r_api": combined_data})




    


@celery_cleaner.task(name="celery_cleaner.clean_news_api", queue='news_cleaning_queue')
def clean_news_api(r, sorting, user_id):
    news_top_authors, news_top_articles, news_recent_articles = [], [], []

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
                #'created_at': dt.datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': article['publishedAt'],
                'title': article['title'] if article['title'] != "[Removed]" else None,
                'description': article['description'] if article['description'] != "[Removed]" else None,
                'media': [article['urlToImage']] if article['urlToImage'] != "[Removed]" else [],
                'url': article['url'] if article['url'] != "[Removed]" else [],
                'source': article['source']['name'] if article['source']['name'] != "[Removed]" else None,
                'sentiment': None,
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
                #'created_at': dt.datetime.fromisoformat(article['publishedAt'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
                'created_at': article['publishedAt'],
                'title': article['title'] if article['title'] != "[Removed]" else None,
                'description': article['description'] if article['description'] != "[Removed]" else None,
                #'media': article['urlToImage'],
                'media': [article['urlToImage']] if article['urlToImage'] != None else [],
                'url': article['url'] if article['url'] != "[Removed]" else [],
                'source': article['source']['name'] if article['source']['name'] != "[Removed]" else None,
                'sentiment': None
                }
        
            news_top_articles.append(article_data_top)

    fetch_data = redis_client.hget(str(user_id), 'news_api')

    if fetch_data:
        fetch_data = json.loads(fetch_data)  # Convert JSON string to a Python dictionary

        # Step 2: Modify the dictionary based on sorting type
        if sorting == "top":
            fetch_data["top_posts"][:] = news_top_articles
            fetch_data["top_authors"][:] = news_top_authors
        elif sorting == "new":
            fetch_data["recent_posts"][:] = news_recent_articles
        else:
            print(f"Invalid sorting type: {sorting}")
            return

        # Step 3: Serialize the updated dictionary back to a JSON string and store it in Redis
        redis_client.hset(str(user_id), "news_api", json.dumps(fetch_data, indent=4))
    else:
        print(f"No data found for user_id: {user_id}")

    # logger.info(f"Cleaning news data completed")
    # sentiment_top_data = sentiment_analysis_news(news_top_articles, "top")
    # sentiment_new_data = sentiment_analysis_news(news_recent_articles, "new")
    # logger.info(f"Sentiment analysis for news data completed")

    # combined_data = json.dumps({"top_posts": news_top_articles, "recent_posts": news_recent_articles, "top_authors": news_top_authors}, indent=4)
    #
    # redis_client.hset(str(user_id), mapping={"news_api": combined_data})
    
    return news_top_articles, news_recent_articles, news_top_authors
        
    

@celery_cleaner.task(name="celery_cleaner.clean_twitter_api", queue='twitter_cleaning_queue')
def clean_twitter_api(r, sorting, user_id):
    twitter_top_authors, twitter_top_articles, twitter_recent_articles = [], [], []

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

            #generate title

            title_data = gen_title(parsed_data, tweet_id)
        tweet_data = {
            #'created': dt.datetime.fromisoformat(parsed_data[tweet_id]['created_at'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
            'created': parsed_data[tweet_id]['created_at'],
            'id': parsed_data[tweet_id]['id'],
            'likes': parsed_data[tweet_id]['public_metrics']['like_count'],
            'note_tweet': parsed_data[tweet_id]['note_tweet']['text'] if 'note_tweet' in parsed_data[tweet_id] else None,
            'text': parsed_data[tweet_id]['text'] if parsed_data[tweet_id]['text'] != None else None,
            'media': parsed_data[tweet_id]['media'] if 'media' in parsed_data[tweet_id] else [],
            'url': f"https://x.com/{parsed_data[tweet_id]['author']['username']}/status/{tweet_id}",
            'author': parsed_data[tweet_id]['author'],
            'possibly_sensitive': parsed_data[tweet_id]['possibly_sensitive'],
            'author_id': author_id,
            'title': title_data
            
        }

        if sorting == "top":
            twitter_top_articles.append({
                'id': tweet_data['id'],
                'created_at': tweet_data['created'],
                'text': tweet_data['text'], 
                'note_tweet': tweet_data['note_tweet'],
                'likes': tweet_data['likes'], 
                'media': tweet_data['media'],
                'url': tweet_data['url'],
                'possibly_sensitive': tweet_data['possibly_sensitive'],
                'sentiment': None,
                'title': tweet_data['title']
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
                'created_at': tweet_data['created'],
                'text': tweet_data['text'], 
                'note_tweet': tweet_data['note_tweet'],
                'likes': tweet_data['likes'], 
                'media': tweet_data['media'],
                'url': tweet_data['url'],
                'possibly_sensitive': tweet_data['possibly_sensitive'],
                'sentiment': None,
                'title': tweet_data['title']
            })
    
    sorted_top_articles = sorted(twitter_top_articles, key=lambda x: x['likes'], reverse=True)
    sorted_top_authors = sorted(twitter_top_authors, key=lambda x: x['likes'], reverse=True)
    sorted_recent_articles = sorted(twitter_recent_articles, key=lambda x: x['created_at'], reverse=True)
    logger.info("Cleaning twitter data completed")

    fetch_data = redis_client.hget(str(user_id), 'x_api')

    if fetch_data:
        fetch_data = json.loads(fetch_data)  # Convert JSON string to a Python dictionary

        # Step 2: Modify the dictionary based on sorting type
        if sorting == "top":
            fetch_data["top_posts"][:] = sorted_top_articles
            fetch_data["top_authors"][:] = sorted_top_authors
        elif sorting == "new":
            fetch_data["recent_posts"][:] = sorted_recent_articles
        else:
            print(f"Invalid sorting type: {sorting}")
            return

        # Step 3: Serialize the updated dictionary back to a JSON string and store it in Redis
        redis_client.hset(str(user_id), "x_api", json.dumps(fetch_data, indent=4))
    else:
        print(f"No data found for user_id: {user_id}")

    # sentiment_top_data = sentiment_analysis_twitter(sorted_top_articles, "top")
    # sentiment_new_data = sentiment_analysis_twitter(sorted_recent_articles, "new")
    # logger.info("Sentiment analysis for twitter data completed")
    #
    # combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles, "top_authors": sorted_top_authors}, indent=4)
    #
    # redis_client.hset(str(user_id), mapping={"x_api": combined_data})
    # logger.info("Twitter data saved to redis")
    
    return sorted_top_articles, sorted_recent_articles
    


