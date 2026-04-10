import requests
import time
import json
import re
from collections import deque

API_URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "SLM4CH_Thesis/1.0 (gea.viozzi@student.univaq.it)"}

ROOT_CATEGORY = "Category:Cultural heritage of Europe"
EXISTING_FILE = "wikipedia_heritage.jsonl"
OUTPUT_FILE = "wikipedia_heritage_v2.jsonl"

MAX_DEPTH_DEFAULT = 3
MAX_DEPTH_BY_COUNTRY = {"Italy": 5}
SLEEP = 0.2


def get_max_depth(country):
    return MAX_DEPTH_BY_COUNTRY.get(country, MAX_DEPTH_DEFAULT)


COUNTRY_PATTERNS = [
    r"Cultural [Hh]eritage of (.+)",
    r"Cultural [Rr]outes? of (.+)",
]

def extract_country(title):
    title = title.removeprefix("Category:")
    for pattern in COUNTRY_PATTERNS:
        m = re.match(pattern, title)
        if m:
            country = m.group(1).strip()
            country = re.sub(r"\s*\(.*?\)", "", country).strip()
            return country
    return None


def get_category_members(category):
    subcats, pages = [], []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category,
        "cmlimit": 500,
        "format": "json"
    }
    while True:
        r = requests.get(API_URL, params=params, headers=HEADERS)
        data = r.json()
        for item in data["query"]["categorymembers"]:
            if item["ns"] == 14:
                subcats.append(item["title"])
            elif item["ns"] == 0:
                pages.append(item["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return subcats, pages


def get_page_links(title):
    links = []
    params = {
        "action": "query",
        "prop": "links",
        "titles": title,
        "pllimit": 500,
        "plnamespace": 0,
        "format": "json"
    }
    while True:
        r = requests.get(API_URL, params=params, headers=HEADERS)
        data = r.json()
        for p in data["query"]["pages"].values():
            for link in p.get("links", []):
                links.append(link["title"])
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return links


def get_page_text(title):
    params = {
        "action": "query",
        "prop": "extracts",
        "explaintext": True,
        "titles": title,
        "format": "json"
    }
    try:
        r = requests.get(API_URL, params=params, headers=HEADERS)
        for p in r.json()["query"]["pages"].values():
            return p.get("extract", "")
    except:
        return ""
    return ""


def is_good(text):
    return text and len(text.split()) >= 50


def load_existing(filepath):
    """Carica le pagine già salvate per non riprocessarle."""
    visited = set()
    try:
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                visited.add(record["title"])
        print(f"Caricate {len(visited)} pagine esistenti")
    except FileNotFoundError:
        print("Nessun file esistente, parto da zero")
    return visited


def save_record(f, title, country, text):
    record = {
        "id": title.replace(" ", "_"),
        "title": title,
        "country": country,
        "description": text
    }
    f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    visited_pages = load_existing(EXISTING_FILE)
    visited_categories = set()
    queue = deque([(ROOT_CATEGORY, 0, None)])
    saved = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        while queue:
            category, depth, inherited_country = queue.popleft()

            if category in visited_categories:
                continue

            country = extract_country(category) or inherited_country or "unknown"

            if depth > get_max_depth(country):
                continue

            print(f"[depth {depth}] {category} (country: {country})")
            visited_categories.add(category)

            try:
                subcats, pages = get_category_members(category)
            except Exception as e:
                print(f"  Errore: {e}")
                continue

            for sub in subcats:
                if sub not in visited_categories:
                    queue.append((sub, depth + 1, country))

            for title in pages:
                # Pagine "List of": segui i link invece di salvare il testo
                if title.lower().startswith("list of"):
                    if title in visited_pages:
                        continue
                    visited_pages.add(title)
                    print(f"  → Lista: '{title}'")
                    linked_titles = get_page_links(title)
                    print(f"    {len(linked_titles)} link trovati")
                    for linked_title in linked_titles:
                        if linked_title in visited_pages:
                            continue
                        visited_pages.add(linked_title)
                        text = get_page_text(linked_title)
                        if not is_good(text):
                            continue
                        save_record(f, linked_title, country, text)
                        saved += 1
                        if saved % 50 == 0:
                            print(f"  Saved: {saved}")
                        
                        time.sleep(SLEEP)
                    continue

                # Pagina normale
                if title in visited_pages:
                    continue
                visited_pages.add(title)
                text = get_page_text(title)
                if not is_good(text):
                    continue
                save_record(f, title, country, text)
                saved += 1
                if saved % 50 == 0:
                    print(f"  Saved: {saved}")
                    
                time.sleep(SLEEP)

    print(f"\nTot saved: {saved}")
    print(f"Output in: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()