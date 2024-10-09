from celery import Celery
from openai import AsyncOpenAI
from dotenv import load_dotenv
import redis
import time, os, re
import asyncio, nest_asyncio
from openai import OpenAI
from logging_config import appLogger as logger
load_dotenv()

celery_report = Celery('celery_report', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
celery_report.conf.task_routes = {
    'report_generation_queue': {
        'queue': 'report_generation_queue',
        'routing_key': 'report_generation_queue'
    }
}
celery_report.conf.update(
    worker_prefetch_multiplier=6
)

redis_client = redis.Redis(host='redis', port=6379, db=0)
client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def message_template(info, title=False):
    return f"""
    Summarize the following set of articles in bullet points. For each bullet point:

    1. Focus on the main topic or key message of each article.
    2. Provide essential context to understand the significance of the information.
    3. Include specific details that illustrate the importance or impact of the news.
    4. If there are related points across articles, group them together coherently.
    5. Ensure the summary reflects the relative importance of each piece of information.

    Your summary should:
    - Capture the core message of each article
    - Balance detail with conciseness
    - Present information in a logical, connected manner
    - Avoid overemphasizing minor details
    - Use clear, informative language

    After the summary, briefly explain how your points relate to each other or to broader themes in current events.

    Articles to summarize:
    {info}
    
    Output: summary of the articles in bullet points. don't give as an key value pair, just give the summary of the articles in bullet points.
    don't point out the Article 1 or Article 2
    """


   
    
async def response_completion(message, api):
    
    print("api: ", api)
    start = time.time()
    try:
        completion = await client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "user", "content": message}
            ]
        )
        response = completion.choices[0].message.content
        print(response)
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        raise
    print("finished response: ", time.time() - start)
    return response


def replace_special_chars(match):
    char = match.group(0)
    if char == '\n':
        return '<br>'
    elif char == '\t':
        return '&nbsp;&nbsp;&nbsp;&nbsp;'
    elif char == '\r':
        return '<br>'
    return char


@celery_report.task(name="celery_report.generate_reddit_report", queue="report_generation_queue")
def generate_reddit_report(data, user_id="12345"):
    info = []
    for post in data:
        info.append(post.get('text', ''))

    message = message_template(info)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)  # Apply nest_asyncio to the loop
    response = loop.run_until_complete(response_completion(message, "reddit"))
    response = re.sub("[\n\t\r]", replace_special_chars, response)

    return response


@celery_report.task(name="celery_report.generate_news_report", queue="report_generation_queue")
def generate_news_report(data, user_id="12345"):
    info = []
    media = None
    for post in data:
        info.append(post.get('description', ''))
        if media == None:
            media = post['media'] if 'media' in post else None 

    message = message_template(info, title="True")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)  # Apply nest_asyncio to the loop
    response = loop.run_until_complete(response_completion(message, "news"))
    response = re.sub("[\n\t\r]", replace_special_chars, response)
    
    return response, media


@celery_report.task(name="celery_report.generate_twitter_report", queue="report_generation_queue")
def generate_twitter_report(data, user_id="12345"):
    info = []
    for post in data:
        info.append(post.get('description', ''))

    message = message_template(info)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)  # Apply nest_asyncio to the loop
    response = loop.run_until_complete(response_completion(message, "twitter"))
    response = re.sub("[\n\t\r]", replace_special_chars, response)

    return response

@celery_report.task(name="celery_report.generate_title", queue="report_generation_queue")
def generate_title(report):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    prompt_template = f"""Craft a compelling title that captures the essence of a diverse set of articles discussing 
    keyword optimization in every field. return title

    Summary: {report}

    """
    
    completion = client.chat.completions.create(
        model="gpt-3.5-turbo-0125",
        messages=[
            {"role": "user", "content": prompt_template}
        ]
    )
    response = completion.choices[0].message.content
    
    return response