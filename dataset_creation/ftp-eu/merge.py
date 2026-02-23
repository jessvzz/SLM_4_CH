# merge two jsonl files into one
import json

def merge_jsonl(file1, file2, output_file):
    merged_data = []

    with open(file1, "r", encoding="utf-8") as f1:
        for line in f1:
            merged_data.append(json.loads(line))

    with open(file2, "r", encoding="utf-8") as f2:
        for line in f2:
            merged_data.append(json.loads(line))

    with open(output_file, "w", encoding="utf-8") as out_f:
        for item in merged_data:
            out_f.write(json.dumps(item, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    merge_jsonl("output.jsonl", "output1.jsonl", "dataset.jsonl")
    print("Files merged successfully")