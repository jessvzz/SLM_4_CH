import requests
import json
import time 
import os 
from dotenv import load_dotenv

load_dotenv()

KEY = os.environ.get("EUROPEANA_API_KEY")

if not KEY:
    raise ValueError("Variable EUROPEANA_API_KEY is not set in the environment.")

URL = "https://api.europeana.eu/record/v2/search.json"
PARAMS = {
    "query": "*",
    "wskey": KEY,
    "rows": 10,  # max for request
    "reusability": "open", # could use also restricted, would have to look more into it
    "media": "true",
    "profile": "rich"
}

next_cursor = '*'
total_records_downloaded = 0
FILENAME = "europeana_dataset.json"


def extract_value(lang_aware_field, lang_code, item):
    """
    Function to extract value for a specific language code
    """
    lang_map = item.get(lang_aware_field, {})
    if lang_code in lang_map and lang_map[lang_code]:
        return lang_map[lang_code][0], lang_code 
    return "", "N/A"

def find_original_description(item):
    """
    Function to find the first NON-English description
    """
    lang_map = item.get("dcDescriptionLangAware", {})
    for lang_code, descriptions in lang_map.items():
        if lang_code != "en" and descriptions:
            return descriptions[0], lang_code 
    return "", "N/A"

def find_original_title(item):
    """
    Function to find the first NON-English title
    """
    lang_map = item.get("dcTitleLangAware", {})
    for lang_code, titles in lang_map.items():
        if lang_code != "en" and titles:
            return titles[0], lang_code
    return "", "N/A"


def load_existing_data(filename):
    """
    Load existing data from a JSON file if it exists and is not empty
    """
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Cannot decode JSON file")
            return []
    return []

dataset_sml = load_existing_data(FILENAME)
total_records_downloaded = len(dataset_sml) 

print(f"Starting download...")
print(f"Uploadea {total_records_downloaded} records")

try:
    while next_cursor:
        # Updating cursor and page number
        PARAMS['cursor'] = next_cursor
        current_page = total_records_downloaded // PARAMS['rows'] + 1
        
        # API request
        response = requests.get(URL, params=PARAMS)
        response.raise_for_status() 
        data = response.json()
        
        current_items = data.get("items", [])
        
        # if there are no more items, breaks the loop
        if not current_items:
            print(f"Page {current_page}: No other record found.")
            break

        next_cursor = data.get("nextCursor")

        page_records = []
        
        # extraction
        for item in current_items:
            # --- titles ---
            title_en, _ = extract_value("dcTitleLangAware", "en", item)
            title_original, original_title_lang_code = find_original_title(item)
            
            # --- descriptions ---
            description_en, _ = extract_value("dcDescriptionLangAware", "en", item)
            description_original, original_desc_lang_code = find_original_description(item)

            # --- country ---
            country = item.get("country", ["N/D"])[0]

            page_records.append({
                "europeana_id": item.get("id"),
                "title_en": title_en, 
                "title_original": title_original,
                "original_title_lang_code": original_title_lang_code, # for translation
                "category": "Cultural",
                "states": country,
                "description_en": description_en, 
                "description_original": description_original,
                "original_desc_lang_code": original_desc_lang_code # for translation
            })
            total_records_downloaded += 1

        dataset_sml.extend(page_records)
        
        with open(FILENAME, "w", encoding="utf-8") as f:
            json.dump(dataset_sml, f, ensure_ascii=False, indent=4)
        
        print(f"Page {current_page} completed. Total number of records: {total_records_downloaded}")
        
        """
        From Europeana API FAQ:
        We do ask that you are courteous with your API calls by 
        leaving some time between calls if you are making a bunch of them. 
        Leaving at least a few milliseconds between each request will 
        make it easier for our servers to handle the load.
        """
        time.sleep(0.08) 

    print("\n-------------------------------------------")
    print(f"FINISHED DOWNLOAD")
    print(f"Number of records: {total_records_downloaded}")

except requests.exceptions.RequestException as e:
    print(f"Error during API call: {e}")
except json.JSONDecodeError:
    print("Error: cannot decode JSON response")
except Exception as e:
    print(f"Unexpected Error: {e}")