import requests
from dotenv import load_dotenv
import os
import redis 

load_dotenv()

class GoogleSearch:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        self.cse_id = os.getenv("GOOGLE_CSE_ID")
        self.url = "https://www.googleapis.com/customsearch/v1"
        self.posts = []
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)

    def search(self, user_id, query, num_results=20):
        params = {
            "key": self.api_key,
            "cx": self.cse_id,
            "q": query,
            "num": num_results  # Number of results to return
        }

        response = requests.get(self.url, params=params)
        results = response.json()

        for item in results.get("items", []):
            print(f"Title: {item['title']}")
            print(f"Link: {item['link']}")
            print(f"Snippet: {item['snippet']}")
            print()
            self.posts.append({
                "title": item["title"],
                "link": item["link"],
                "snippet": item["snippet"]
            })
        
        self.redis_client.hset(str(user_id), mapping={"google_api": self.posts})
            
        
            

# Example usage:

