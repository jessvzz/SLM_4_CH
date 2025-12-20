from lxml import etree
import json
import os
from pathlib import Path

#namespaces
NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "edm": "http://www.europeana.eu/schemas/edm/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "ore": "http://www.openarchives.org/ore/terms/"
}

# ---------------------------
# HELPERS
# ---------------------------
def extract_text(elements, preferred_lang="en"):
    for el in elements:
        lang = el.attrib.get("{http://www.w3.org/XML/1998/namespace}lang")
        if lang == preferred_lang:
            return {"value": el.text, "lang": lang}

    if elements:
        el = elements[0]
        return {
            "value": el.text,
            "lang": el.attrib.get("{http://www.w3.org/XML/1998/namespace}lang")
        }

    return None


def parse_xml_file(xml_path):
    tree = etree.parse(xml_path)

    proxies = tree.xpath("//ore:Proxy", namespaces=NS)

    provider_proxy = None
    europeana_proxy = None

    for p in proxies:
        flag = p.xpath("edm:europeanaProxy/text()", namespaces=NS)
        if flag:
            if flag[0] == "false":
                provider_proxy = p
            elif flag[0] == "true":
                europeana_proxy = p

    if provider_proxy is None:
        raise ValueError("Provider Proxy non trovato")

    # -------- TEXT --------
    title = extract_text(
        provider_proxy.xpath("dc:title", namespaces=NS)
    )

    description = extract_text(
        provider_proxy.xpath("dc:description", namespaces=NS)
    )

    # -------- PLACES (EN) --------
    uri_to_label_en = {}

    for place in tree.xpath("//edm:Place", namespaces=NS):
        uri = place.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
        label_en = place.xpath(
            "skos:prefLabel[@xml:lang='en']/text()",
            namespaces=NS
        )
        if label_en:
            uri_to_label_en[uri] = label_en[0]

    places = []

    if europeana_proxy is not None:
        for p in europeana_proxy.xpath("dcterms:spatial", namespaces=NS):
            uri = p.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
            if uri in uri_to_label_en:
                places.append(uri_to_label_en[uri])

    needs_translation = (
        (title and title["lang"] != "en") or
        (description and description["lang"] != "en")
    )

    return {
        "title": title,
        "description": description,
        "places": places,
        "needs_translation": needs_translation
    }

# ---------------------------
# BATCH PIPELINE
# ---------------------------
INPUT_DIR = Path("data3/xml")
OUTPUT_DIR = Path("data3/json")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

all_records = []
errors = []

for xml_file in INPUT_DIR.glob("*.xml"):
    try:
        record = parse_xml_file(xml_file)

        record["id"] = xml_file.stem
        all_records.append(record)

        print(f"Processed: {xml_file.name}")

    except Exception as e:
        errors.append({
            "file": xml_file.name,
            "error": str(e)
        })
        print(f"Error: {xml_file.name}")


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_DIR / "europeana_dataset.json", "w", encoding="utf-8") as f:
    json.dump(all_records, f, ensure_ascii=False, indent=2)

print(f"{len(all_records)} record saved")

if errors:
    with open(OUTPUT_DIR / "errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)
    print(f"{len(errors)} errors logged")

else:
    print("All files processed without errors")

print("Processing complete.")