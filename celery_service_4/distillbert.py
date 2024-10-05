import torch
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification
import json

tokenizer = DistilBertTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
model = DistilBertForSequenceClassification.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")

with open("files/reddit_combined_data.json", "r") as f:
    data = json.load(f)


for text in data['top_posts']:
    if text['text'] is not None:
        inputs = tokenizer(text['text'], return_tensors="pt", truncation=True)
        with torch.no_grad():
            logits = model(**inputs).logits

        predicted_class_id = logits.argmax().item()
        print(model.config.id2label[predicted_class_id])
