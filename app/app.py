from fastapi import FastAPI
from pydantic import BaseModel  
from typing import List, Dict, Any
import json
import celery
import redis
from logging_config import appLogger as logger

redis_client = redis.Redis(host='redis', port=6379, db=0)
celery_app = celery.Celery('app', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
app = FastAPI()

class RequestData(BaseModel):
    user_id: int
    keyword: List[str]
  
class ReportData(BaseModel):
    data: Dict[str, Any]

@app.post("/receive_data")
async def receive_data(data: RequestData):
    redis_client.flushall()
    rapi = celery_app.send_task('celery_fetcher.fetch_rapi', args=(data.user_id, data.keyword), queue='reddit_fetch_queue')
    news_api = celery_app.send_task('celery_fetcher.fetch_news_api',args=(data.user_id, data.keyword), queue='news_fetch_queue')
    twitter_api = celery_app.send_task('celery_fetcher.fetch_twitter', args=(data.user_id, data.keyword), queue='twitter_fetch_queue')
    
    twitter = twitter_api.get()
    r_api = rapi.get()
    news = news_api.get()
    
    data = redis_client.hgetall(str(data.user_id))
    return data

@app.post("/report_generation")
async def recieve_report_data(data: ReportData):
    
    r_api_summary = celery_app.send_task("celery_report.generate_reddit_report", (data.data['r_api'],), queue="report_generation_queue")
    x_api_summary = celery_app.send_task("celery_report.generate_twitter_report", (data.data['x_api'],), queue="report_generation_queue")
    news_api_summary = celery_app.send_task("celery_report.generate_news_report", (data.data['news_api'],), queue="report_generation_queue")
    
    x_api = x_api_summary.get()
    news_api = news_api_summary.get()
    r_api = r_api_summary.get()
        
    
    report = f"* {x_api} <br/><br/> * {r_api} <br/><br/>* {news_api[0]}"
        
    title_generation = celery_app.send_task("celery_report.generate_title", (report,), queue="report_generation_queue")
    title = title_generation.get()
    logger.info(f"Title: {title}")
    response = json.dumps(
        {
            "title": title, 
            "description": report,
            "img_url": news_api[1], 
            # "sources": {data.data['metadata']['socialMediaSelected']},
            # "socket_id": data.data['socketId'],
            "sources": ["twitter", "reddit", "news"],
            "socket_id": 23423423,
        },
        indent=4
    )    
    
    return response
    


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
