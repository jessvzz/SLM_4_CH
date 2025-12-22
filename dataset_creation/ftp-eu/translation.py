import json
import sys
from deep_translator import GoogleTranslator

translator = GoogleTranslator(source='el', target='en')

def safe_translate(text):
    if not text or not isinstance(text, str):
        return text
    try:
        return translator.translate(text)
    except Exception as e:
        print(f"Translation error: {e}")
        return text


def json_to_jsonl(input_json, output_jsonl):
    print("Starting translation pipeline...")

    try:
        with open(input_json, "r", encoding="utf-8") as f:
            dataset = json.load(f)
        print(f"Input file loaded. {len(dataset)} records found")
    except Exception as e:
        print(f"Failed to load input file: {e}")
        sys.exit(1)

    success = 0
    failed = 0

    with open(output_jsonl, "w", encoding="utf-8") as out:
        for idx, data in enumerate(dataset, 1):
            print(f" - Processing record {idx}/{len(dataset)}")

            try:
                title = data.get("title", {}).get("value", "")
                description = data.get("description", {}).get("value", "")

                if data.get("needs_translation", False):
                    title = safe_translate(title)
                    description = safe_translate(description)
                    print("Translation completed")
                else:
                    print("Translation not required")

                places = data.get("places", [])
                location_text = ""
                if places:
                    location_text = f" Located in {', '.join(places)}."

                jsonl_obj = {
                    "id": title,
                    "category": "Cultural",
                    "states": places[-1] if places else "",
                    "text": f"{description}{location_text}".strip()
                }

                out.write(json.dumps(jsonl_obj, ensure_ascii=False) + "\n")
                success += 1
                print("Record written successfully\n")

            except Exception as e:
                failed += 1
                print(f"Error processing record {idx}: {e}\n")

    print("Translation pipeline finished!")
    print(f"Successful records: {success}")
    print(f"Failed records: {failed}")
    print(f"Output written to: {output_jsonl}")


if __name__ == "__main__":
    json_to_jsonl("data3/json/europeana_dataset.json", "dataset.jsonl")
