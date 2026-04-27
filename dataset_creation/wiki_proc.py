import json
import re

INPUT_FILE = "wikipedia_heritage_v2.jsonl"
OUTPUT_CPT = "wiki_data.jsonl"
OUTPUT_STATS = "stats.json"

def clean_text(text):
    text = re.sub(r"==\s*(References|External links|See also|Notes|Bibliography|Further reading)\s*==.*",
                  "", text, flags=re.DOTALL)
    text = re.sub(r"^\^.*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*[\d\.\,\;\:]+\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()

def chunk_by_sections(text, max_words=400, overlap_words=50):
    sections = re.split(r"\n(==+[^=]+=+)\n", text)
    
    blocks = []
    current_title = None
    current_text = ""

    for part in sections:
        if re.match(r"==+[^=]+=+", part):
            if current_text.strip():
                blocks.append((current_title, current_text.strip()))
            current_title = part.strip("= ").strip()
            current_text = ""
        else:
            current_text += part

    if current_text.strip():
        blocks.append((current_title, current_text.strip()))

    return blocks


def process_record(record, max_words=400, overlap_words=50):
    text = clean_text(record["description"])
    blocks = chunk_by_sections(text)
    
    chunks = []
    buffer_section = None
    buffer_text = ""

    for section_title, section_text in blocks:
        words = section_text.split()

        def make_title(base_title, sec_title):
            if sec_title is None:
                return base_title
            return f"{base_title} - {sec_title}"

        if len(words) > max_words:
            if buffer_text:
                if len(buffer_text.split()) >= 50:
                    chunks.append({
                        "title": make_title(record["title"], buffer_section),
                        "text": buffer_text.strip()
                    })
                buffer_text = ""
                buffer_section = None

            start = 0
            while start < len(words):
                end = start + max_words
                chunk_text = " ".join(words[start:end])
                if len(chunk_text.split()) >= 50:
                    chunks.append({
                        "title": make_title(record["title"], section_title),
                        "text": chunk_text
                    })
                start += max_words - overlap_words

        elif len(words) < 50:
            buffer_section = buffer_section or section_title
            buffer_text += f"\n{section_text}\n"

        else:
            full_text = (buffer_text + "\n" + section_text).strip()
            chunks.append({
                "title": make_title(record["title"], buffer_section or section_title),
                "text": full_text
            })
            buffer_text = ""
            buffer_section = None

    if buffer_text.strip() and len(buffer_text.split()) >= 50:
        chunks.append({
            "title": make_title(record["title"], buffer_section),
            "text": buffer_text.strip()
        })

    return chunks


def main():
    stats = {
        "records_total": 0,
        "records_skipped_too_short": 0,
        "records_processed": 0,
        "chunks_total": 0,
        "chunks_discarded_too_short": 0,
    }

    with open(INPUT_FILE, encoding="utf-8") as fin, \
         open(OUTPUT_CPT, "w", encoding="utf-8") as fout:

        for line in fin:
            record = json.loads(line)
            stats["records_total"] += 1

            if len(record["description"].split()) < 50:
                stats["records_skipped_too_short"] += 1
                continue

            stats["records_processed"] += 1
            chunks = process_record(record)
            stats["chunks_total"] += len(chunks)

            for i, chunk in enumerate(chunks):
                out = {
                    "id": chunk["title"].replace(" ", "_"),
                    "title": chunk["title"],
                    "country": record.get("country", "unknown"),
                    "description": chunk["text"],
                    "chunk_index": i,
                    "source_id": record["id"]
                }
                fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    print(f"Records totali letti:       {stats['records_total']}")
    print(f"Records scartati (<50 w):   {stats['records_skipped_too_short']}")
    print(f"Records processati:         {stats['records_processed']}")
    print(f"Chunk totali prodotti:      {stats['chunks_total']}")
    avg = stats["chunks_total"] / stats["records_processed"] if stats["records_processed"] else 0
    print(f"Chunk medi per record:      {avg:.2f}")

    with open(OUTPUT_STATS, "w", encoding="utf-8") as sf:
        json.dump(stats, sf, indent=2, ensure_ascii=False)
    print(f"\nStats salvate in {OUTPUT_STATS}")


if __name__ == "__main__":
    main()