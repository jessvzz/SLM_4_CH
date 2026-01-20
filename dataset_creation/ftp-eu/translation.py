import json
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

MODEL_NAME = "facebook/nllb-200-distilled-600M"
DEVICE = "mps" if torch.backends.mps.is_available() else "cpu"

TARGET_LANG = "eng_Latn"
BATCH_SIZE = 8

# MODEL LOADING

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME).to(DEVICE)
model.eval()

# TRANSLATION FUNCTION

def translate_texts(texts, src_lang):
    tokenizer.src_lang = src_lang

    inputs = tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512
    ).to(DEVICE)

    forced_bos_token_id = tokenizer.convert_tokens_to_ids(TARGET_LANG)

    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            forced_bos_token_id=forced_bos_token_id,
            max_length=512
        )

    return tokenizer.batch_decode(outputs, skip_special_tokens=True)

# Data loading and translation

with open("data6/json/europeana_dataset.json", "r", encoding="utf-8") as f:
    data = json.load(f)


for record in tqdm(data):

    title = record.get("title")
    if title and title.get("value"):
        lang = title.get("lang")

    if lang is None:
        src_lang = "lat_Latn"
    elif lang.startswith("de"):
        src_lang = "deu_Latn"
    elif lang.startswith("la"):
        src_lang = "lat_Latn"
    else:
        src_lang = "lat_Latn"

        if lang != "en":
            try:
                translated = translate_texts(
                    [title["value"]],
                    src_lang=src_lang
                )[0]

                record["title_en"] = translated
            except Exception as e:
                record["title_en"] = None
        else:
            record["title_en"] = title["value"]

    desc = record.get("description")
    if desc:
        try:
            translated = translate_texts(
                [desc],
                src_lang="deu_Latn"
            )[0]
            record["description_en"] = translated
        except Exception:
            record["description_en"] = None

# SAVE
with open("data6/json/europeana_dataset_en.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Translation completed.")
