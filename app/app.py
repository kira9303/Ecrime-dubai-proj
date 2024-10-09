from importlib.metadata import metadata
from fastapi import FastAPI
from pydantic import BaseModel  
from typing import List, Dict, Any, Union
import json
import celery
import redis
from logging_config import appLogger as logger
import requests

redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
celery_app = celery.Celery('app', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
app = FastAPI()

class RequestData(BaseModel):
    searchTerm: str
    startDate: str
    endDate: str
    socialMediaSelected: List[str]
    newsSelected: List[str]
    reportId: int
    socketId: str
    email: str


  
class ReportData(BaseModel):
    data: List[Dict[str, Any]]
    metadata: Dict[str, Union[int, Any]]

@app.post("/receive_data")
async def receive_data(data: RequestData):
    redis_client.flushall()

    metadata = {
        "searchTerm": data.searchTerm,
        "startDate": data.startDate,
        "endDate": data.endDate,
        "socialMediaSelected": data.socialMediaSelected,
        "newsSelected": data.newsSelected,
        "reportId": data.reportId,
        "socketId": data.socketId,
        "email": data.email
    }

    #
    reddit_json = {
            "top_posts": [],
            "recent_posts": [],
            "top_authors": [],
    }

    news_json = {
        "top_posts": [],
        "recent_posts": [],
        "top_authors": [],
    }

    twitter_json = {
        "top_posts": [],
        "recent_posts": [],
        "top_authors": [],
    }

    #combined_data = json.dumps()

    #redis_client.hset(str(user_id), mapping={"news_api": combined_data})

    redis_client.hset(str(data.reportId), mapping={"r_api": json.dumps(reddit_json, indent=4)})
    redis_client.hset(str(data.reportId), mapping={"news_api": json.dumps(news_json, indent=4)})
    redis_client.hset(str(data.reportId), mapping={"x_api": json.dumps(twitter_json, indent=4)})

    logger.info("redis_entire_data ", redis_client.hgetall(str(data.reportId)))



    #reddit_recent_articles, reddit_top_articles, reddit_top_authors = [], [], []
    #combined_data = json.dumps({"top_posts": sorted_top_articles, "recent_posts": sorted_recent_articles,
     #                           "top_authors": sorted_top_authors}, indent=4)
    #redis_client.hset(str(user_id), mapping={"r_api": combined_data})

    rapi = celery_app.send_task('celery_fetcher.fetch_rapi', args=(data.reportId, data.searchTerm), queue='reddit_fetch_queue')
    news_api = celery_app.send_task('celery_fetcher.fetch_news_api',args=(data.reportId, data.searchTerm), queue='news_fetch_queue')
    twitter_api = celery_app.send_task('celery_fetcher.fetch_twitter', args=(data.reportId, data.searchTerm), queue='twitter_fetch_queue')
    #



    twitter = twitter_api.get()
    r_api = rapi.get()
    news = news_api.get()

    data = redis_client.hgetall(str(data.reportId))

    #data = json.loads(data)


    #logger.info(data)

    data["metadata"] = metadata



    url = 'https://6b2b-106-51-85-235.ngrok-free.app/report/fastapi/search-result'

    # If you want to send form data, you can use the data parameter
    response = requests.post(url, json=data)

    # Check the response
    if response.status_code == 200:

        return 'Success:', 200  # Parse response as JSON if expected
    else:
        return 'something went wrong', 404


    # return data


def sort_dict(data):
    r_api = []
    x_api = []
    news_api = []
    for i in range(0, len(data)):
        temp_data = data[i]
        if(temp_data["platform"]=='x'):
            x_api.append(temp_data)
        if(temp_data["platform"]=='reddit'):
            r_api.append(temp_data)
        if(temp_data["platform"]=='news'):
            news_api.append(temp_data)
    return r_api, x_api, news_api


@app.post("/report_generation")
async def recieve_report_data(data: ReportData):

    #print(data.data)

    r_api, x_api, news_api = sort_dict(data.data)

    print(f"First newsAPI data: {news_api}")


    
    r_api_summary = celery_app.send_task("celery_report.generate_reddit_report", (r_api,), queue="report_generation_queue")
    x_api_summary = celery_app.send_task("celery_report.generate_twitter_report", (x_api,), queue="report_generation_queue")
    news_api_summary = celery_app.send_task("celery_report.generate_news_report", (news_api,), queue="report_generation_queue")
    #metadata = celery_app.send_task("celery_report.generate_news_report", (data.data['news_api'],), queue="report_generation_queue")
    resp_metadata = data.metadata

    
    x_api = x_api_summary.get()
    news_api = news_api_summary.get()
    r_api = r_api_summary.get()
        
    
    report = f"<strong>Twitter summary:-</strong><br/>{x_api}<br /><strong>Reddit summary:-</strong><br/>{r_api}<br /><strong>News summary:-</strong><br/>{news_api[0]}"
        
    title_generation = celery_app.send_task("celery_report.generate_title", (report,), queue="report_generation_queue")
    title = title_generation.get()
    logger.info(f"Title: {title}")
    #for i in range(0, len(news_api[]))
    print(f"New link: {news_api[1][0]}")
    print(f"News API complete: {news_api}")
    response = {
            "title": title, 
            "description": report,
            "img_url": news_api[1][0],
            "metadata": resp_metadata
        }

    #print(response)
    url = 'https://6b2b-106-51-85-235.ngrok-free.app/report/fastapi/generated-report'

    # If you want to send form data, you can use the data parameter
    response = requests.post(url, json=response)

    # Check the response
    if response.status_code == 200:

        return 'success:', 200  # Parse response as JSON if expected
    else:
        return 'something went wrong', 404
    # return response
    


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
