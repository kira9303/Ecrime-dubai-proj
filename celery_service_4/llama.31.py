from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
import torch
import time
import json
start = time.time()
model_name = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"

# Check if CUDA is available
device = 0 if torch.cuda.is_available() else -1

try:
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
except Exception as e:
    print(f"Error loading model or tokenizer: {e}")
    raise

try:
    nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer, device=device, max_length=512, truncation=True)
except Exception as e:
    print(f"Error initializing pipeline: {e}")
    raise

with open("files/reddit_combined_data.json", "r") as f:
    data = json.load(f)

label_mapping = {
    "LABEL_0": "Negative",
    "LABEL_1": "Neutral",
    "LABEL_2": "Positive"
}
for text in data['top_posts']:
    if text['text'] is not None:
        outputs = nlp(text['text'])
        for output in outputs:
            label = output['label']
            score = output['score']
            print(f"Label: {label}, Score: {score}")
    
with open("llama.31.json", "w") as f:
    f.write(json.dumps(data, indent=4))

# Define label mapping



print("Time taken: ", time.time() - start)