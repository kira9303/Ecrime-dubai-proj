# celery_gpt_service/Dockerfile
FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies

COPY requirements.txt .

RUN pip install -r requirements.txt

# Copy the application code and .env file
COPY . .

# Start Celery worker
CMD ["celery", "-A", "celery_fetcher", "worker", "--autoscale=3,12", "--loglevel=info", "-Q", "reddit_fetch_queue,twitter_fetch_queue,news_fetch_queue"]