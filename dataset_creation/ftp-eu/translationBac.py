import json
import unicodedata
import torch
import os
from tqdm import tqdm
from langdetect import detect, DetectorFactory
from transformers import MarianMTModel, MarianTokenizer
from file_read_backwards import FileReadBackwards 

DetectorFactory.seed = 0

MODEL_NAME = "Helsinki-NLP/opus-mt-mul-en"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

INPUT = os.path.join(BASE_DIR, "dataset_final.jsonl")
OUTPUT = os.path.join(DATA_DIR, "dataset_english_reverse.jsonl")
LOG_PATH = os.path.join(DATA_DIR, "translation_log_reverse.txt")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {DEVICE}")

tokenizer = MarianTokenizer.from_pretrained(MODEL_NAME)
model = MarianMTModel.from_pretrained(MODEL_NAME).to(DEVICE)
model.eval()

def normalize_text(text):
    return unicodedata.normalize("NFKC", text)

def detect_language(text):
    try: return detect(text)
    except: return "en"

def translate_chunk(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512).to(DEVICE)
    with torch.no_grad():
        translated = model.generate(**inputs, max_length=512)
    return tokenizer.decode(translated[0], skip_special_tokens=True)

def translate(text):
    if not text or len(text.strip()) < 3:
        return text
    text = normalize_text(text)
    if detect_language(text) == "en":
        return text
    
    sentences = text.split(". ")
    return ". ".join([translate_chunk(sent) for sent in sentences if sent.strip()])

# Da cambiare
TOTAL_RECORDS = 7000000 
START_FROM_REVERSE = 7000000 

current_idx = TOTAL_RECORDS

with FileReadBackwards(INPUT, encoding="utf-8") as frb, \
     open(OUTPUT, "a", encoding="utf-8") as outfile, \
     open(LOG_PATH, "a", encoding="utf-8") as log_file:

    pbar = tqdm(total=TOTAL_RECORDS)
    
    for line in frb:
        if current_idx > START_FROM_REVERSE:
            current_idx -= 1
            pbar.update(1)
            continue
        
        if not line.strip():
            current_idx -= 1
            continue

        record = json.loads(line)
        title = record.get("title", "")
        description = record.get("description", "")

        if title:
            record["title_og"] = title
            record["title"] = translate(title)
        if description:
            record["description_og"] = description
            record["description"] = translate(description)

        outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
        outfile.flush()
        
        log_file.write(f"Processed index: {current_idx}\n")
        log_file.flush()
        
        current_idx -= 1
        pbar.update(1)

print("Translation completed")