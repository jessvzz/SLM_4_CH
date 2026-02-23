import json
import unicodedata
import torch
from tqdm import tqdm
from langdetect import detect, DetectorFactory
from transformers import MarianMTModel, MarianTokenizer

DetectorFactory.seed = 0

MODEL_NAME = "Helsinki-NLP/opus-mt-mul-en"
INPUT = "test/test.jsonl"
OUTPUT = "test/out.jsonl"

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


with open(INPUT, "r", encoding="utf-8") as f:
    data = [json.loads(line) for line in f]

print(f"Loaded {len(data)} records. Beginning translation...")

for rec in tqdm(data):

    title = rec.get("title", "")
    if title:
        rec["title_en"] = translate(title)

    desc = rec.get("description", "")
    if desc:
        rec["description_en"] = translate(desc)


with open(OUTPUT, "w", encoding="utf-8") as f:
    for record in data:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

print("Translation completed.")
