import json
import re
import hashlib

def normalize_for_dedup(text):
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def deduplicate_file(input_path, output_path):
    seen_hashes = set()
    cleaned_data = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            desc = record.get("description", "")
            
            normalized = normalize_for_dedup(desc)
            text_hash = hashlib.md5(normalized.encode()).hexdigest()

            if text_hash not in seen_hashes:
                seen_hashes.add(text_hash)
                cleaned_data.append(record)

    with open(output_path, "w", encoding="utf-8") as f:
        for item in cleaned_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    deduplicate_file("dataset.jsonl", "dataset_final.jsonl")
    print("Deduplication completed successfully")