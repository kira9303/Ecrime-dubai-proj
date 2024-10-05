from celery import Celery
from dotenv import load_dotenv
import os
from openai import OpenAI
from main.celery_service_4.logging_config import appLogger as logger
load_dotenv()


celery_sentiment_analysis = Celery('celery_sentiment', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
celery_sentiment_analysis.conf.task_routes = {
    'reddit_summary_generation_queue': {
        'queue': 'reddit_summary_generation_queue',
        'routing_key': 'reddit_summary_generation_queue'
    },
    'news_summary_generation_queue': {
        'queue': 'news_summary_generation_queue',
        'routing_key': 'news_summary_generation_queue'
    },
    'twitter_summary_generation_queue': {
        'queue': 'twitter_summary_generation_queue',
        'routing_key': 'twitter_summary_generation_queue'
    }
}
celery_sentiment_analysis.conf.update(
    worker_prefetch_multiplier=6
)


data_top, data_new, data_authors = [], [], []
@celery_sentiment_analysis.task(name="celery_sentiment_analysis.sentiment_analysis_reddit", queue="reddit_summary_generation_queue")
def sentiment_analysis_reddit(data, sorting):
    logger.info(f"Sentiment analysis for Reddit data started for {sorting}")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    try:
        for post in data:
            message = f"""
            Analyze the following data for sentiment (positive, negative, or neutral). Each post consists of a title, text, and sometimes a media link. 
            Please evaluate the sentiment based on both the content of the title and the text, considering relevant emotional tones like excitement, frustration, or neutrality. Return the only the sentiment of the posts
            such as positive, negative, or neutral.
            
            Title: {post['title']}
            Text: {post['text']}
            
            Output: "Neutral" or "Positive" or "Negative"
            """
        
            completion = client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                messages=[
                    {"role": "user", "content": message}
                ]
            )
            response = completion.choices[0].message.content
            post['sentiment'] = response
            logger.info(f"Sentiment analysis for Reddit data completed for {post['id']}")
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        
    return data


def sentiment_analysis_news(data, sorting):
    logger.info(f"Sentiment analysis for News data started ")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    try:
        for post in data:
            if post['description'] == None and post['title'] == None:
                post['sentiment'] = None
            # post['sentiment'] = "Neutral"
        message = f"""
        Analyze the following data for sentiment (positive, negative, or neutral). Each post consists of a title, text, and sometimes a media link. 
        Please evaluate the sentiment based on both the content of the title and the text, considering relevant emotional tones like excitement, frustration, or neutrality. Return the only the sentiment of the posts
        such as positive, negative, or neutral.
        
        Title:{post['title']}
        Text: {post['description']}
        
        Output: "Neutral" or "Positive" or "Negative"
        """
    
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "user", "content": message}
            ]
        )
        response = completion.choices[0].message.content
        post['sentiment'] = response
        logger.info(f"Sentiment analysis for News data completed for {post['id']}")
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        
    return data


def sentiment_analysis_twitter(data, sorting):
    logger.info(f"Sentiment analysis for X data started for {sorting} ")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    try:
        for post in data:
            if post['text'] == None and post['note_tweet'] == None:
                continue
            message = f"""
            Analyze the following data for sentiment (positive, negative, or neutral). Each post consists of a title, text, and sometimes a media link. 
            Please evaluate the sentiment based on both the content of the Note tweet and the text, considering relevant emotional tones like excitement, frustration, or neutrality. Return the only the sentiment of the posts
            such as positive, negative, or neutral. 
            
            Note about Note tweet data: Information about Tweets with more than 280 characters. 
            
            Note tweet:{post['note_tweet']}
            Text: {post['text']}
            
            Output: "Neutral" or "Positive" or "Negative"
            """
        
            completion = client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                messages=[
                    {"role": "user", "content": message}
                ]
            )
            response = completion.choices[0].message.content
            post['sentiment'] = response
            logger.info(f"Sentiment analysis for X data completed for {post['id']}")
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        
    return data




