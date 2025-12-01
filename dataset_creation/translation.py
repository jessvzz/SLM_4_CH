import json
import time
from deep_translator import GoogleTranslator


"""
This script processes the dataset JSON file,
translating titles and descriptions to English
and converting it into JSONL format. (for consistency with other datasets)
"""
INPUT_FILE = "europeana_dataset.json"
OUTPUT_FILE = "europeana.jsonl"  
SLEEP_TIME = 0.5  # break between translations to avoid rate limiting

translator = GoogleTranslator(source='auto', target='en')

def translate_and_process(input_path, output_path, sleep_time):
   
    print(f"Uploading data..")
    try:
        with open(input_path, "r", encoding="utf-8") as f:
            dataset = json.load(f)
    except FileNotFoundError:
        print(f"Error: could not find flie")
        return
    except json.JSONDecodeError:
        print(f"Error: cannot decode JSON file")
        return

    total_entries = len(dataset)
    translated_count = 0
    
    print(f"Starting processing...")

    with open(output_path, "w", encoding="utf-8") as outfile:
        
        for i, entry in enumerate(dataset):
            # -- title --            
            final_title = entry.get("title_en", "") 
            
            if not final_title and entry.get("title_original"):
                try:
                    final_title = translator.translate(entry["title_original"])
                    translated_count += 1
                    time.sleep(sleep_time) 
                except Exception as e:
                    print(f"Translation error for record {i+1}: {e}")
                    final_title = entry["title_original"] # Fallback to original

            # --- text ---
            
            final_text = entry.get("description_en", "") 
                 
            if not final_text and entry.get("description_original"):
                try:
                    final_text = translator.translate(entry["description_original"])
                    translated_count += 1
                    time.sleep(sleep_time)
                except Exception as e:
                    print(f"Translation error for record {i+1}: {e}")
                    final_text = entry["description_original"] # Fallback to original

            # decided to leave original text and language code in case it is useful for the future
            final_entry = {
                "id_europeana": entry.get("europeana_id", ""),
                "title": final_title, 
                "original_title": entry.get("title_original", ""),
                "category": entry.get("category", ""), # because i have it in another dataset
                "states": entry.get("states", ""),
                "text": final_text,
                "original_text": entry.get("description_original", ""),
                "language_code": entry.get("original_desc_lang_code", "") 
            }
            
            outfile.write(json.dumps(final_entry, ensure_ascii=False) + '\n')
            
            if (i + 1) % 100 == 0:
                print(f"Elaborated {i+1}/{total_entries} record. Translations: {translated_count}")

    print("\n-------------------------------------------")
    print(f"FINISHED PROCESSING")
   
translate_and_process(INPUT_FILE, OUTPUT_FILE, SLEEP_TIME)