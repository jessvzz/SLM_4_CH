import re
import json



def normalize_whitespace(text):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


# ----------------------------
# EUROPEANA DATASET
# ----------------------------

def clean_europeana_record(record):
    title = record.get("title", {}).get("value", "").strip()
    country = record.get("places", ["Unknown"])[0]
    raw_desc = record.get("description", "")

    # Remove library classification codes like 725.822(469.322)"19"(084.11)
    raw_desc = re.sub(r'\d+\.\d+\([^)]+\)"?[\d/]*"?\([^)]+\)', '', raw_desc)

    description = f"{title}. {raw_desc}"
    description = normalize_whitespace(description)

    return {
        "id": record.get("id"),
        "title": title,
        "country": country,
        "description": description
    }


# ----------------------------
# BENI CULTURALI DATASET
# ----------------------------

def clean_italian_record(record):
    title = record.get("id", "").strip()
    country = record.get("states", "Italy")
    text = record.get("text", "")

    # Remove CSS blocks
    text = re.sub(r'\* \{[^}]+\}', '', text)
    text = re.sub(r'\.[a-zA-Z0-9_-]+\s*\{[^}]+\}', '', text)

    # Remove email
    text = re.sub(r'mailto:[^\s]+', '', text)

    # Remove website
    text = re.sub(r'http[s]?://\S+', '', text)

    # Remove opening hours section
    text = re.sub(r'Orari di apertura:.*?(Tipologia|$)', '', text, flags=re.DOTALL)

    # Remove contact section
    text = re.sub(r'Contatti:.*?(Sito web|$)', '', text, flags=re.DOTALL)

    description = normalize_whitespace(text)

    return {
        "id": title,
        "title": title,
        "country": country,
        "description": description
    }


# ----------------------------
# UNESCO DATASET
# ----------------------------

def clean_unesco_record(record):
    title = record.get("id", "").strip()
    country = record.get("states", "Unknown")
    text = record.get("text", "")

    # Only normalize whitespace (keep everything)
    description = normalize_whitespace(text)

    return {
        "id": title,
        "title": title,
        "country": country,
        "description": description
    }


# ----------------------------
# MAIN DISPATCHER
# ----------------------------

def process_record(record):
    """if "places" in record:
        return clean_europeana_record(record)

    elif "states" in record and "text" in record:
        if record.get("states") == "Russian Federation":
            return clean_unesco_record(record)
        else:
            return clean_italian_record(record)
    
    else:
        return None
    """
    return clean_europeana_record(record)


# ----------------------------
# PROCESS FILE
# ----------------------------

def process_file(input_path, output_path):
    output_data = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            cleaned = process_record(record)
            if cleaned:
                output_data.append(cleaned)

    with open(output_path, "w", encoding="utf-8") as f:
        for item in output_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    process_file("dataBeniCulturali.jsonl", "output2.jsonl")
