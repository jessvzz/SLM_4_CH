import json
import unicodedata
import torch
from tqdm import tqdm
from langdetect import detect, DetectorFactory
from transformers import MarianMTModel, MarianTokenizer
import os

DetectorFactory.seed = 0

MODEL_NAME = "Helsinki-NLP/opus-mt-mul-en"
#INPUT = "test/test.jsonl"
#OUTPUT = "test/out.jsonl"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(DATA_DIR, exist_ok=True)

INPUT = os.path.join(BASE_DIR, "dataset_final.jsonl")
OUTPUT = os.path.join(DATA_DIR, "dataset_english.jsonl")
LOG_PATH = os.path.join(DATA_DIR, "translation_log.txt")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {DEVICE}")

tokenizer = MarianTokenizer.from_pretrained(MODEL_NAME)
model = MarianMTModel.from_pretrained(MODEL_NAME).to(DEVICE)
model.eval()


def normalize_text(text):
    return unicodedata.normalize("NFKC", text)


def detect_language(text):
    try:
        return detect(text)
    except:
        return "en"


def translate_chunk(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512).to(DEVICE)

    with torch.no_grad():
        translated = model.generate(**inputs, max_length=512)

    return tokenizer.decode(translated[0], skip_special_tokens=True)


def translate(text):
    if not text or len(text.strip()) < 3:
        return text

    text = normalize_text(text)
    lang = detect_language(text)

    if lang == "en":
        return text

    sentences = text.split(". ")
    translated_sentences = []

    for sent in sentences:
        if len(sent.strip()) > 0:
            translated_sentences.append(translate_chunk(sent))

    return ". ".join(translated_sentences)

i = 0
START_FROM = 223473
with open(INPUT, "r", encoding="utf-8") as infile, \
     open(OUTPUT, "a", encoding="utf-8") as outfile, \
     open(LOG_PATH, "a", encoding="utf-8") as log_file:

        for line in tqdm(infile):
            i = i + 1
            if i < START_FROM:
                continue
            record = json.loads(line)
            title = record.get("title", "")
            description = record.get("description", "")

            if title:
                record["title"] = translate(title)
                record["title_og"] = title
            if description:
                record["description"] = translate(description)
                record["description_og"] = description  

            outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
            outfile.flush()
            log_file.write(f"{i}\n")
            log_file.flush()    
    

print("Translation completed.")
