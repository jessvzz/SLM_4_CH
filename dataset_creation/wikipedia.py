import requests
import time
import json
import re
from collections import deque

API_URL = "https://en.wikipedia.org/w/api.php"

HEADERS = {
    "User-Agent": "SLM4CH_Thesis/1.0 (gea.viozzi@student.univaq.it)"
}
ROOT_CATEGORY = "Category:Cultural heritage of Europe"

MAX_DEPTH = 3
SLEEP = 0.2
OUTPUT_FILE = "wikipedia.jsonl"



COUNTRY_PATTERNS = [
    r"Cultural [Hh]eritage of (.+)",
    r"Cultural [Rr]outes? of (.+)",
]

def extract_country(category_title: str) -> str | None:
    
    title = category_title.removeprefix("Category:")

    for pattern in COUNTRY_PATTERNS:
        m = re.match(pattern, title)
        if m:
            country = m.group(1).strip()
            country = re.sub(r"\s*\(.*?\)", "", country).strip()
            return country

    return None


def get_category_members(category):
    subcats = []
    pages = []

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
            if item["ns"] == 14:  # category
                subcats.append(item["title"])
            elif item["ns"] == 0:  # page
                pages.append(item["title"])

        if "continue" in data:
            params.update(data["continue"])
        else:
            break

    return subcats, pages

def get_page_links(title):
    params = {
        "action": "query",
        "prop": "links",
        "titles": title,
        "pllimit": 500,
        "plnamespace": 0,
        "format": "json"
    }

    links = []
    while True:
        r = requests.get(API_URL, params=params, headers=HEADERS)
        data = r.json()
        pages = data["query"]["pages"]
        for p in pages.values():
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
        pages = r.json()["query"]["pages"]
        for p in pages.values():
            return p.get("extract", "")
    except:
        return ""

    return ""


def is_good(text):
    if not text:
        return False
    if len(text.split()) < 50:
        return False
    return True



# MAIN CRAWLER
def main():
    visited_categories = set()
    visited_pages = set()

    
    queue = deque([(ROOT_CATEGORY, 0, None)])

    saved = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:

        while queue:
            category, depth, inherited_country = queue.popleft()

            if category in visited_categories:
                continue
            if depth > MAX_DEPTH:
                continue

            print(f"[depth {depth}] {category}  (country: {inherited_country or '?'})")
            visited_categories.add(category)

            
            country = extract_country(category) or inherited_country or "unknown"

            try:
                subcats, pages = get_category_members(category)
            except Exception as e:
                print(f"  Error fetching category: {e}")
                continue

            for sub in subcats:
                if sub not in visited_categories:
                    queue.append((sub, depth + 1, country))

            for title in pages:
                if title in visited_pages:
                    continue
                visited_pages.add(title)

                # I it's "List of...", follow links for italy
                if title.lower().startswith("list of") and country == "Italy":
                    linked_titles = get_page_links(title)
                    for linked_title in linked_titles:
                        if linked_title not in visited_pages:
                            visited_pages.add(linked_title)
                            text = get_page_text(linked_title)
                            if not is_good(text):
                                continue
                            record = {
                                "id": linked_title.replace(" ", "_"),
                                "title": linked_title,
                                "country": country,
                                "description": text
                            }
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                            saved += 1
                            time.sleep(SLEEP)
                    continue


                text = get_page_text(title)
                if not is_good(text):
                    continue
                
                country = extract_country("Category:" + title) or extract_country(title) or country

                record = {
                    "id": title.replace(" ", "_"),
                    "title": title,
                    "country": country,
                    "description": text
                }

                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                saved += 1

                if saved % 50 == 0:
                    print(f"  Saved: {saved}")


                time.sleep(SLEEP)

    print(f"\nTot saved: {saved}")


if __name__ == "__main__":
    main()