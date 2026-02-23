import wikipediaapi
import json
from tqdm import tqdm
import time

# Configurazione Wikipedia (inglese per copertura europea)
wiki = wikipediaapi.Wikipedia(
    language='en',
    extract_format=wikipediaapi.ExtractFormat.WIKI,
    user_agent="SLM4CH/1.0 (https://github.com/jessvzz/SLM_4_CH.git)"

)

# Categorie seed
SEED_CATEGORIES = [
    "Cultural heritage of Europe",
    "Museums in Europe",
    "Art in Europe",
    "European painters",
    "European sculptors",
    "Architecture in Europe",
    "World Heritage Sites in Europe"
]

MAX_DEPTH = 1  # puoi mettere 2 ma cresce molto il dataset

visited_pages = set()
collected_articles = []

def collect_from_category(category_name, depth):
    if depth < 0:
        return

    category_page = wiki.page("Category:" + category_name)

    if not category_page.exists():
        print(f"Category not found: {category_name}")
        return

    for member in category_page.categorymembers.values():

        # Articoli normali
        if member.ns == 0:
            if member.title not in visited_pages:
                visited_pages.add(member.title)

                page = wiki.page(member.title)
                if page.exists() and len(page.text) > 500:  # filtro minimo lunghezza
                    collected_articles.append({
                        "title": page.title,
                        "text": page.text,
                        "url": page.fullurl
                    })

        # Sottocategorie
        elif member.ns == 14:
            collect_from_category(member.title.replace("Category:", ""), depth - 1)


print("Starting extraction...\n")

for category in SEED_CATEGORIES:
    print(f"Processing category: {category}")
    collect_from_category(category, MAX_DEPTH)
    time.sleep(1)  # rispetto per Wikipedia

print(f"\nTotal collected articles: {len(collected_articles)}")

# Salvataggio
with open("europe_cultural_heritage_dataset.json", "w", encoding="utf-8") as f:
    json.dump(collected_articles, f, ensure_ascii=False, indent=2)

print("Dataset saved as europe_cultural_heritage_dataset.json")
