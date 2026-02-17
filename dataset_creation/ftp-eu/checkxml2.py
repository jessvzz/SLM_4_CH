from lxml import etree
import json
import os
from pathlib import Path
import shutil
import zipfile


def extract_zip(zip_path, dest_dir):
    folder_name = zip_path.stem
    extract_path = dest_dir / folder_name

    if extract_path.exists():
        shutil.rmtree(extract_path)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_path)

    return extract_path

#namespaces
NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "edm": "http://www.europeana.eu/schemas/edm/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "ore": "http://www.openarchives.org/ore/terms/"
}

XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"

# HELPERS
def extract_text(elements, preferred_lang="en"):
    for el in elements:
        if el.attrib.get(XML_LANG) == preferred_lang and el.text:
            return {"value": el.text.strip(), "lang": preferred_lang}

    for el in elements:
        if el.text:
            return {
                "value": el.text.strip(),
                "lang": el.attrib.get(XML_LANG)
            }

    return None


def extract_all_text(elements, preferred_lang="en"):
    """Return list of texts, English first, then others."""
    texts = []

    for el in elements:
        if el.attrib.get(XML_LANG) == preferred_lang and el.text:
            texts.append(el.text.strip())

    for el in elements:
        if el.text and el.text.strip() not in texts:
            texts.append(el.text.strip())

    return texts

def parse_xml_file(xml_path):
    tree = etree.parse(xml_path)

    provider_proxy = None
    europeana_proxy = None

    for p in tree.xpath("//ore:Proxy", namespaces=NS):

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

    description_parts = []

    subjects = extract_all_text(
        provider_proxy.xpath("dc:subject", namespaces=NS)
    )
    if subjects:
        description_parts.append("Subjects: " + "; ".join(subjects))

    toc = extract_all_text(
        provider_proxy.xpath("dcterms:tableOfContents", namespaces=NS)
    )
    if toc:
        description_parts.append("Contents: " + " ".join(toc))
    creator = extract_all_text(
        provider_proxy.xpath("dc:creator", namespaces=NS)
    )
    if creator:
        description_parts.append("Creator: " + ", ".join(creator))

    publisher = extract_all_text(
        provider_proxy.xpath("dc:publisher", namespaces=NS)
    )
    if publisher:
        description_parts.append("Publisher: " + ", ".join(publisher))
    
    issued = extract_all_text(
        provider_proxy.xpath("dcterms:issued", namespaces=NS)
    )
    if issued:
        description_parts.append("Year: " + ", ".join(issued))

    description = " ".join(description_parts) if description_parts else None

    countries = tree.xpath("//edm:country/text()", namespaces=NS)
    countries = [c.strip() for c in countries if c.strip()]

    places = []
    for place in tree.xpath("//edm:Place", namespaces=NS):
        uri = place.attrib.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
        label_en = place.xpath(
            "skos:prefLabel[@xml:lang='en']/text()",
            namespaces=NS
        )
        if label_en:
            places.append(label_en[0])


    geo = []
    geo.extend(countries)
    geo.extend(places)

    geo = list(dict.fromkeys(geo))

    needs_translation = (
        (title and title["lang"] != "en") or
        (description and description["lang"] != "en")
    )


    return {
        "title": title,
        "description": description,
        "places": geo,
        "needs_translation": needs_translation
    }

#PIPLINE FOR BATCH PROCESSING

# "C:\Users\Utente\OneDrive - Università degli Studi dell'Aquila\TesiMagistral
ZIP_DIR = Path(r"C:\Users\Utente\OneDrive - Università degli Studi dell'Aquila\TesiMagistrale")
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "dataInput" / "json"
OUTPUT_DIR = BASE_DIR / "dataOutput" / "json"


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATASET_FILE = OUTPUT_DIR / "europeana_dataset.json"
ERRORS_FILE = OUTPUT_DIR / "errors.json"
PROCESSED_DIRS_FILE = OUTPUT_DIR / "processed_directories.txt"

if DATASET_FILE.exists():
    with open(DATASET_FILE, "r", encoding="utf-8") as f:
        all_records_global = json.load(f)
else:
    all_records_global = []

if ERRORS_FILE.exists():
    with open(ERRORS_FILE, "r", encoding="utf-8") as f:
        errors_global = json.load(f)
else:
    errors_global = []

processed_dirs = set()
if PROCESSED_DIRS_FILE.exists():
    processed_dirs = set(
        PROCESSED_DIRS_FILE.read_text(encoding="utf-8").splitlines()
    )

print("ZIP_DIR exists:", ZIP_DIR.exists())
print("ZIP files found:", list(ZIP_DIR.glob("*.zip")))

"""for zip_file in ZIP_DIR.glob("*.zip"):
    try:
        extract_zip(zip_file, INPUT_DIR)
        #print(f"Extracted: {zip_file.name}")
    except Exception as e:
        #print(f"Error extracting {zip_file.name}: {e}")"""

for subdirectory in INPUT_DIR.iterdir():
    if not subdirectory.is_dir():
        continue

    if subdirectory.name in processed_dirs:
        print(f"Skipping already processed directory: {subdirectory.name}")
        continue

    print(f"Processing directory: {subdirectory.name}")

    local_records = []
    local_errors = []

    for xml_file in subdirectory.glob("*.xml"):
        try:
            record = parse_xml_file(xml_file)
            record["id"] = xml_file.stem
            local_records.append(record)
            print(f"  Processed: {xml_file.name}")

        except Exception as e:
            local_errors.append({
                "directory": subdirectory.name,
                "file": xml_file.name,
                "error": str(e)
            })
            print(f"  Error: {xml_file.name}")

    all_records_global.extend(local_records)
    errors_global.extend(local_errors)


    with open(PROCESSED_DIRS_FILE, "a", encoding="utf-8") as f:
        f.write(subdirectory.name + "\n")
    processed_dirs.add(subdirectory.name)

    print(f"  {len(local_records)} records added")

with open(DATASET_FILE, "w", encoding="utf-8") as f:
    json.dump(all_records_global, f, ensure_ascii=False, indent=2)

with open(ERRORS_FILE, "w", encoding="utf-8") as f:
    json.dump(errors_global, f, ensure_ascii=False, indent=2)

print("Processing complete.")
print(f"Total records: {len(all_records_global)}")
print(f"Total errors: {len(errors_global)}")