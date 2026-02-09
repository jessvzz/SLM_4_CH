import json
from pathlib import Path
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# ---------------------------
# PATH
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent
INPUT_JSON = BASE_DIR / "data6/json/europeana_dataset.json"
OUTPUT_JSON = BASE_DIR / "data6/json/europeana_dataset_lang_detected.json"

# ---------------------------
# MODELLO LINGUA
# ---------------------------
LANG_MODEL = "papluca/xlm-roberta-base-language-detection"

print("Caricamento modello lingua HF...")
lang_tokenizer = AutoTokenizer.from_pretrained(LANG_MODEL)
lang_model = AutoModelForSequenceClassification.from_pretrained(LANG_MODEL)
lang_model.eval()

ID2LANG = lang_model.config.id2label

# ---------------------------
# ISO → NLLB
# ---------------------------
ISO_TO_NLLB = {
    "en": "eng_Latn",
    "de": "deu_Latn",
    "fr": "fra_Latn",
    "it": "ita_Latn",
    "es": "spa_Latn",
    "lv": "lav_Latn",
    "lt": "lit_Latn",
    "pl": "pol_Latn",
    "la": "lat_Latn",
    "nl": "nld_Latn",
}

# ---------------------------
# FUNZIONI
# ---------------------------
def detect_language(text: str) -> str:
    if not text or not text.strip():
        return "unknown"

    inputs = lang_tokenizer(
        text[:512],
        return_tensors="pt",
        truncation=True
    )

    with torch.no_grad():
        logits = lang_model(**inputs).logits

    lang_id = torch.argmax(logits, dim=-1).item()
    return ID2LANG[lang_id]


def to_nllb(lang_code: str) -> str:
    return ISO_TO_NLLB.get(lang_code, "eng_Latn")


# ---------------------------
# MAIN
# ---------------------------
with open(INPUT_JSON, encoding="utf-8") as f:
    records = json.load(f)

print(f"{len(records)} record caricati")

for rec in records:
    # TITLE
    title_value = rec.get("title", {}).get("value", "")
    title_lang = rec.get("title", {}).get("lang")

    if not title_lang:
        title_lang = detect_language(title_value)

    rec["title_lang_detected"] = title_lang
    rec["title_lang_nllb"] = to_nllb(title_lang)

    # DESCRIPTION
    desc_value = rec.get("description", "")
    desc_lang = detect_language(desc_value)

    rec["description_lang_detected"] = desc_lang
    rec["description_lang_nllb"] = to_nllb(desc_lang)

# ---------------------------
# SAVE
# ---------------------------
OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"File salvato in {OUTPUT_JSON}")
print("Detection completata")
