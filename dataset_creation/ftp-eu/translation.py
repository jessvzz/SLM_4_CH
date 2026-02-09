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
    if not texts:
        return []
    
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

with open("data6/json/europeana_dataset_lang_detected.json", "r", encoding="utf-8") as f:
    data = json.load(f)


for record in tqdm(data):

    title = record.get("title", {})
    title_text = title.get("value", "")
    title_lang_nllb = record.get("title_lang_nllb")

    if title_text and title_lang_nllb != TARGET_LANG:
        try:
            record["title_en"] = translate_texts(
                [title_text],
                src_lang=title_lang_nllb
            )[0]
        except Exception:
            record["title_en"] = None
    else:
        record["title_en"] = title_text


    desc_text = record.get("description", "")
    desc_lang_nllb = record.get("description_lang_nllb")

    if desc_text and desc_lang_nllb != TARGET_LANG:
        try:
            record["description_en"] = translate_texts(
                [desc_text],
                src_lang=desc_lang_nllb
            )[0]
        except Exception:
            record["description_en"] = None
    else:
        record["description_en"] = desc_text

# SAVE
with open("data6/json/europeana_dataset_en.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Translation completed.")
