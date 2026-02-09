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

if __name__ == "__main__":
    from pathlib import Path

    BASE_DIR = Path(__file__).resolve().parent
    ZIP_DIR = BASE_DIR / "data6"
    EXTRACT_DIR = BASE_DIR / "data6" / "prova"

    print("ZIP_DIR exists:", ZIP_DIR.exists())
    print("ZIP files found:", list(ZIP_DIR.glob("*.zip")))

    for zip_file in ZIP_DIR.glob("*.zip"):
        try:
            extract_zip(zip_file, EXTRACT_DIR)
            print(f"Extracted: {zip_file.name}")
        except Exception as e:
            print(f"Error extracting {zip_file.name}: {e}")