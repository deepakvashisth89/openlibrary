import argparse
import csv
import json
import os
import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# CONFIG & LOGGING SETUP
# --------------------------------------------------------------------------- #

BASE_DIR = "data"
FOLDERS = {
    "metadata": os.path.join(BASE_DIR, "book_metadata"),
    "coverpage": os.path.join(BASE_DIR, "book_coverpage"),
    "blurb": os.path.join(BASE_DIR, "book_blurb"),
    "reviews": os.path.join(BASE_DIR, "book_reviews"),
}

LOG_FILE = "scraper_execution.log"
OPENLIBRARY_METADATA_FILE = os.path.join(
    FOLDERS["metadata"], "openlibrary_metadata.json"
)


def log(message: str):
  """Prints message to console and appends it to the execution log file."""
  print(message)
  try:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
      f.write(message + "\n")
  except Exception:
    pass


# Open Library guidelines request a descriptive User-Agent with contact info
HEADERS = {
    "User-Agent": (
        "BookDataPipeline/1.0 (student-project; contact@example.com)"
    )
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Automatic retry with exponential backoff for transient network failures
# (e.g. "Remote end closed connection without response", timeouts, 5xx
# errors). Without this, a single dropped connection on Open Library's side
# kills that book's request instead of quietly retrying.
_retry_strategy = Retry(
    total=4,
    backoff_factor=1.5,  # waits ~1.5s, 3s, 4.5s, 6s between retries
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_adapter = HTTPAdapter(max_retries=_retry_strategy)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# --------------------------------------------------------------------------- #
# SETUP FOLDERS
# --------------------------------------------------------------------------- #


def setup_folders():
  for path in FOLDERS.values():
    os.makedirs(path, exist_ok=True)


def load_existing_metadata(path: str) -> list[dict]:
  if not os.path.exists(path):
    return []
  try:
    with open(path, "r", encoding="utf-8") as f:
      data = json.load(f)
      if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
  except Exception as e:
    log(f"[error] Failed to load existing metadata JSON: {e}")
  return []


def append_metadata_record(metadata: dict, path: str):
  try:
    records = load_existing_metadata(path)
    records = [r for r in records if r.get("isbn13") != metadata.get("isbn13")]
    records.append(metadata)
    with open(path, "w", encoding="utf-8") as f:
      json.dump(records, f, ensure_ascii=False, indent=2)
  except Exception as e:
    log(f"[error] Failed to write metadata record to JSON: {e}")


# --------------------------------------------------------------------------- #
# PIPELINE FOR OPEN LIBRARY API
# --------------------------------------------------------------------------- #


LANGUAGE_CODE_MAP = {
    "eng": "English", "fre": "French", "fra": "French", "ger": "German",
    "deu": "German", "spa": "Spanish", "ita": "Italian", "por": "Portuguese",
    "rus": "Russian", "chi": "Chinese", "zho": "Chinese", "jpn": "Japanese",
    "ara": "Arabic", "hin": "Hindi", "kor": "Korean", "dut": "Dutch",
    "nld": "Dutch", "swe": "Swedish", "dan": "Danish", "nor": "Norwegian",
    "pol": "Polish", "tur": "Turkish", "gre": "Greek", "ell": "Greek",
    "heb": "Hebrew", "urd": "Urdu", "ben": "Bengali", "tam": "Tamil",
    "tel": "Telugu", "mar": "Marathi", "guj": "Gujarati", "pan": "Punjabi",
    "lat": "Latin", "cze": "Czech", "ces": "Czech", "fin": "Finnish",
    "hun": "Hungarian", "ukr": "Ukrainian", "vie": "Vietnamese",
    "tha": "Thai", "ind": "Indonesian", "may": "Malay", "msa": "Malay",
}
_LANGUAGE_NAME_CACHE = {}


def resolve_language_name(lang_code: str) -> str:
  """Translate a MARC/OL language code (e.g. 'eng') to a readable name."""
  if lang_code in LANGUAGE_CODE_MAP:
    return LANGUAGE_CODE_MAP[lang_code]
  if lang_code in _LANGUAGE_NAME_CACHE:
    return _LANGUAGE_NAME_CACHE[lang_code]
  try:
    resp = SESSION.get(
        f"https://openlibrary.org/languages/{lang_code}.json", timeout=10
    )
    resp.raise_for_status()
    name = resp.json().get("name", lang_code)
  except Exception:
    name = lang_code
  _LANGUAGE_NAME_CACHE[lang_code] = name
  return name


def fetch_edition_supplementary_data(book_info: dict, isbn13: str) -> dict:
  """Fetches the edition's raw record once and pulls out everything jscmd=data
  is unreliable for: work-level description fallback, language, and
  origin/country of publication.

  jscmd=data does not reliably expose 'works', and its language/place fields
  are inconsistent across editions, so we go to the canonical raw record at
  /books/{OLID}.json instead.
  """
  result = {"description": None, "language": "Unknown", "origin": "Unknown"}

  identifiers = book_info.get("identifiers", {})
  edition_olids = identifiers.get("openlibrary", [])
  if not edition_olids:
    log(f"[warn] No edition OLID found for {isbn13}, cannot fetch edition record")
    return result

  edition_olid = edition_olids[0]
  edition_url = f"https://openlibrary.org/books/{edition_olid}.json"
  try:
    edition_resp = SESSION.get(edition_url, timeout=15)
    edition_resp.raise_for_status()
    edition_data = edition_resp.json()
  except Exception as e:
    log(f"[warn] Could not fetch edition record for {isbn13}: {e}")
    return result

  # --- Language ---
  languages = edition_data.get("languages", [])
  if languages:
    lang_codes = []
    for lang in languages:
      key = lang.get("key", "")
      code = key.rsplit("/", 1)[-1] if key else None
      if code:
        lang_codes.append(resolve_language_name(code))
    if lang_codes:
      result["language"] = ", ".join(lang_codes)

  # --- Origin / Country of publication ---
  publish_places = edition_data.get("publish_places", [])
  if publish_places:
    result["origin"] = ", ".join(publish_places)
  elif edition_data.get("publish_country"):
    result["origin"] = edition_data["publish_country"]

  # --- Work-level description fallback ---
  work_refs = edition_data.get("works", [])
  if work_refs and "key" in work_refs[0]:
    work_key = work_refs[0]["key"]
    work_url = f"https://openlibrary.org{work_key}.json"
    try:
      work_resp = SESSION.get(work_url, timeout=15)
      work_resp.raise_for_status()
      work_data = work_resp.json()
      result["description"] = work_data.get("description")
    except Exception as e:
      log(f"[warn] Could not fetch work-level description for {isbn13}: {e}")
  else:
    log(f"[warn] Edition record for {isbn13} has no linked work")

  return result


def run_for_isbn(isbn13: str):
  isbn13 = isbn13.strip().replace("-", "").replace(" ", "")
  log(f"\n=== Processing ISBN-13 via Open Library API: {isbn13} ===")

  api_url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn13}&format=json&jscmd=data"

  try:
    response = SESSION.get(api_url, timeout=15)
    response.raise_for_status()
    data = response.json()
  except Exception as e:
    log(f"[error] Failed to connect or fetch API data for ISBN {isbn13}: {e}")
    return

  book_key = f"ISBN:{isbn13}"
  if book_key not in data:
    log(f"[warn] No record found in Open Library for ISBN {isbn13}")
    return

  book_info = data[book_key]

  try:
    # --- 1. Metadata & Genres Extraction ---
    title = book_info.get("title", "Unknown Title")
    authors = [
        author.get("name")
        for author in book_info.get("authors", [])
        if "name" in author
    ]
    publishers = [
        pub.get("name")
        for pub in book_info.get("publishers", [])
        if "name" in pub
    ]
    publisher = publishers[0] if publishers else "Unknown"
    pub_date = book_info.get("publish_date", "Unknown")
    subjects = [
        sub.get("name") for sub in book_info.get("subjects", []) if "name" in sub
    ]

    # Single extra call to the edition's raw record — reused for language,
    # origin/country, and the work-level description fallback below.
    supplementary = fetch_edition_supplementary_data(book_info, isbn13)

    metadata = {
        "isbn13": isbn13,
        "title": title,
        "authors": authors,
        "publisher": publisher,
        "origin_country": supplementary["origin"],
        "date_of_publication": pub_date,
        "language": supplementary["language"],
        "genres": subjects,
        "source_url": book_info.get(
            "url", f"https://openlibrary.org/isbn/{isbn13}"
        ),
        "sources": ["OpenLibrary"],
    }

    append_metadata_record(metadata, OPENLIBRARY_METADATA_FILE)
    log(
        f"[metadata] Saved record for {isbn13} "
        f"(language={supplementary['language']}, origin={supplementary['origin']})."
    )

    # --- 2. Blurb Text File (edition-level, with work-level fallback) ---
    desc_raw = book_info.get("description")

    if not desc_raw:
      desc_raw = supplementary["description"]

    if isinstance(desc_raw, dict):
      blurb_text = desc_raw.get("value", "No description available.")
    elif isinstance(desc_raw, str) and desc_raw.strip():
      blurb_text = desc_raw
    else:
      blurb_text = "No description available."
      log(f"[warn] No description found (edition or work level) for {isbn13}")

    blurb_path = os.path.join(FOLDERS["blurb"], f"{isbn13}_b_openlibrary_1.txt")
    with open(blurb_path, "w", encoding="utf-8") as f:
      f.write(blurb_text)
    log(f"[blurb] Saved text file -> {blurb_path}")

    # --- 3. Cover Page (.jpg) ---
    cover_url = f"https://covers.openlibrary.org/b/isbn/{isbn13}-L.jpg"
    try:
      cover_resp = SESSION.get(cover_url, timeout=15)
      if cover_resp.status_code == 200 and len(cover_resp.content) > 1000:
        cover_path = os.path.join(
            FOLDERS["coverpage"], f"{isbn13}_cp_openlibrary_1.jpg"
        )
        with open(cover_path, "wb") as f:
          f.write(cover_resp.content)
        log(f"[coverpage] Downloaded cover image -> {cover_path}")
      else:
        log(f"[warn] Cover image not available for ISBN {isbn13}")
    except Exception as e:
      log(f"[coverpage] Failed to download cover image for {isbn13}: {e}")

    # --- 4. Reviews ---
    # IMPORTANT: Open Library's API does not expose individual user-written
    # reviews. We do NOT fabricate review text to hit a file-count target —
    # doing so would violate the assignment's "do not hardcode scraped
    # values" rule and would not reflect real data. Instead we log this
    # honestly per book so the gap is visible in the execution log and can
    # be addressed with a proper review source (e.g. scraping Goodreads/
    # Amazon, which do expose real user reviews).
    review_note_path = os.path.join(
        FOLDERS["reviews"], f"{isbn13}_r_openlibrary_1.txt"
    )
    with open(review_note_path, "w", encoding="utf-8") as f:
      f.write(
          "No individual user reviews are available via the Open Library "
          "API for this ISBN. Open Library does not expose per-user review "
          "text through any public endpoint."
      )
    log(
        f"[reviews] Open Library has no review text endpoint — wrote "
        f"honest placeholder for {isbn13}. Task 4 (25 reviews/book) is "
        f"NOT satisfied by this source."
    )

  except Exception as e:
    log(f"[error] An unexpected error occurred while parsing ISBN {isbn13}: {e}")


# --------------------------------------------------------------------------- #
# CSV LOADER & MAIN
# --------------------------------------------------------------------------- #

DEFAULT_CSV = "2602193001.csv"


def load_isbns_from_csv(csv_path: str) -> list[str]:
  isbns = []
  if not os.path.exists(csv_path):
    log(f"[error] CSV file not found at: {csv_path}")
    return isbns

  try:
    with open(csv_path, "r", encoding="utf-8-sig") as f:
      reader = csv.reader(f)
      for row in reader:
        if not row:
          continue
        candidate = row[0].strip().strip('"').strip("'")
        cleaned = candidate.replace("-", "").replace(" ", "")
        if cleaned.isdigit() and len(cleaned) in (10, 13):
          isbns.append(cleaned)
  except Exception as e:
    log(f"[error] Failed to read CSV file {csv_path}: {e}")
  return isbns


def main():
  parser = argparse.ArgumentParser(
      description="Open Library ISBN-13 API Pipeline"
  )
  parser.add_argument(
      "--isbn-file", default=DEFAULT_CSV, help="Path to CSV file with ISBNs"
  )
  parser.add_argument(
      "--limit", type=int, default=None, help="Process first N ISBNs"
  )
  args = parser.parse_args()

  setup_folders()
  log(f"[info] Starting Open Library pipeline using CSV: {args.isbn_file}")
  log(f"[info] All system actions and errors are logged to: {LOG_FILE}")

  isbn_list = load_isbns_from_csv(args.isbn_file)
  log(f"[info] Found {len(isbn_list)} valid ISBNs.")

  if not isbn_list:
    log("[error] No valid ISBNs found or file is empty. Exiting.")
    sys.exit(1)

  if args.limit is not None:
    isbn_list = isbn_list[:args.limit]

  for idx, isbn in enumerate(isbn_list, start=1):
    log(
        f"\n{'='*50}\n[progress] {idx}/{len(isbn_list)} — ISBN:"
        f" {isbn}\n{'='*50}"
    )
    try:
      run_for_isbn(isbn)
    except Exception as e:
      log(f"[!] Critical error processing ISBN {isbn}: {e}")
    time.sleep(1)  # Polite delay

  log("\n[done] Finished processing Open Library dataset batch.")


if __name__ == "__main__":
  main()
