import argparse
import csv
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

# -------------------------------------------
# základné vstupné a výstupné súbory/cesty
CSV_EXTRACT = Path("data/csv/extracted.csv")   # vstupné CSV s extrahovanými údajmi
OUT_DIR     = Path("data/index")                # výstupný priečinok s indexom

# Výstupné súbory
INVERTED_INDEX_PATH = OUT_DIR / "inverted_index.jsonl"  # INVERTOVANÝ INDEX (term -> {doc_id: tf})
IDF_PATH            = OUT_DIR / "idf.jsonl"             # DF/IDF pre termy (TF-IDF aj BM25)
META_PATH           = OUT_DIR / "meta.json"             # meta a štatistiky (N, avgdl, doclen, mapy doc->meta)

# ---------------------------------
# atribúty na indexovanie (vynechávam title lebo z toho som si extrahovala drug_name)
ATTRS_TO_INDEX = (
    "drug_name",
    "generic_name",
    "brand_names",
    "dosage_forms",
    "drug_class",
    "availability",
    "indications",
    "dosage",
    "side_effects",
    "warnings",
)

# ----------------------------------------------------------------
# tokenizácia, zachovávam medicínske zápisy (mg/mL, 2.5%, q12h, IV, SC, 10-20…).

MIN_TOKEN_LEN_DEFAULT = 2  # minimálna dĺžka tokenu

def tokenize(text: str, min_token_len: int = MIN_TOKEN_LEN_DEFAULT) -> List[str]:
    text = (text or "").lower()      # zmena na malé písmená
    # ponecháme len písmená, čísla a znaky dôležité pre dávkovanie/jednotky
    text = re.sub(r"[^a-z0-9%\./\-+ ]+", " ", text)
    toks = re.split(r"\s+", text)       # split podľa whitespace
    return [t for t in toks if t and len(t) >= min_token_len]   # zahodím prázdne a príliš krátke tokeny

# -------------------------------------
# práca s dokumentom = stránka

def _doc_id_from_url(url: str) -> str:
    # ID dokumentu = MD5 hash z URL.
    raw = url or ""
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def _concat_row_fields(row: Dict[str, str], fields: Iterable[str]) -> str:
    # spojím obsah vybraných polí (sekcií) do jedného TEXTU dokumentu
    parts: List[str] = []
    for f in fields:
        val = (row.get(f) or "").strip()
        if val:
            parts.append(val)
    return "\n".join(parts).strip()

def iter_docs_pages(csv_path: Path) -> Iterable[Dict]:
    """ Vytvorenie dokumentu
    Každý riadok CSV = 1 dokument:
      {
        "doc_id": <md5(url)>,
        "url": <url>,
        "drug_name": <drug_name>,
        "text": <obsah všetkých indexovaných sekcií>
      }
    """
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            url  = (row.get("url") or "").strip()
            drug = (row.get("drug_name") or "").strip()
            text = _concat_row_fields(row, ATTRS_TO_INDEX)  # spojíme všetky sekcie do jedného textu

            if not text:
                # prázdna stránka = nič na indexovanie
                continue

            yield {
                "doc_id": _doc_id_from_url(url),
                "url": url,
                "drug_name": drug,
                "text": text,
            }

# -----------------------------------------------------
#  IDF VZORCE (2 metódy)

def idf_logN(df: int, N: int) -> float:
    # TF-IDF štýl:   idf = log(N / df)
    # kde N = počet dokumentov, df = v koľkých dokumentoch sa term vyskytol.
    df = max(1, df)
    return math.log(N / df)

def idf_bm25(df: int, N: int) -> float:
    # BM25 štýl:  idf = log( (N - df + 0.5) / (df + 0.5) + 1 )

    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)

# ---------------------------------------------------------------
""" 
vytvorenie indexov, poznámky: 
 TF – Term Frequency (frekvencia výskytu termu v dokumente)
 DF – Document Frequency (počet dokumentov, ktoré tento term obsahujú)
 IDF = Inverzná DF:    IDF = log(N / DF)
           Ak DF je vysoké (slovo sa vyskytuje všade), IDF je malé → term nemá vysokú váhu.
           Ak DF je nízke (slovo je zriedkavé), IDF je veľké → term je dôležitý na rozlíšenie dokumentov.
"""

def build_index(csv_path: Path, out_dir: Path, min_doc_tokens: int, min_token_len: int) -> None:
    """
    invertovaný index a výsledné dokumenty:
      - inverted_index.jsonl : term -> {doc_id: tf}
      - idf.jsonl            : term -> {df, idf_logN, idf_bm25}
      - meta.json            : {N, avgdl, doclen, meta, nastavenia}

    meta.json obsahuje:
    - N: počet dokumentov
    - avgdl: priemerná dĺžka dokumentu (pre BM25)
    - doclen: dĺžky jednotlivých dokumentov (doc_id -> token count)
    - meta: mapovanie doc_id na URL a názov lieku (pre výstup výsledkov)
    - granularity: typ dokumentu ("page" = celá stránka)
    - attrs_joined: zoznam sekcií, ktoré boli spojené do jedného dokumentu
    => Tento súbor slúži na interpretáciu výsledkov a výpočet skóre, nie je to samotný index.
    """

    global INVERTED_INDEX_PATH, IDF_PATH, META_PATH
    INVERTED_INDEX_PATH = out_dir / "inverted_index.jsonl"
    IDF_PATH            = out_dir / "idf.jsonl"
    META_PATH           = out_dir / "meta.json"

    # Invertovaný index: term -> {doc_id: tf}
    postings: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # Dĺžky dokumentov (počet tokenov) – pre normalizácie (BM25) a diagnostiku
    doclen: Dict[str, int] = {}
    # Meta údaje pre spätný výstup výsledkov
    meta: Dict[str, Dict[str, str]] = {}

    # deduplikácia stránok, ak by sa v CSV náhodou zopakovala tá istá stránka s identickým textom
    seen_keys = set()  # (url, hash(textu))

    for d in iter_docs_pages(csv_path):
        text_hash = hashlib.md5(d["text"].encode("utf-8")).hexdigest()
        dedup_key = (d["url"], text_hash)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        # tokenizácia textu stránky (spojené sekcie)
        toks = tokenize(d["text"], min_token_len=min_token_len)

        # hranica na min. počet tokenov v dokumente (default 1 – ponechá aj veľmi krátke stránky)
        if len(toks) < min_doc_tokens:
            continue

        did = d["doc_id"]
        doclen[did] = len(toks)

        # uložím meta (url, názov lieku)
        meta[did] = {
            "url": d["url"],
            "drug_name": d["drug_name"],
        }

        # naplím invertovaný index: TF pre každý term v dokumente
        for t in toks:
            postings[t][did] += 1

    # počet dokumentov a priemerná dĺžka dokumentu (pre BM25)
    N = len(doclen)
    avgdl = (sum(doclen.values()) / N) if N else 0.0

    # skontrolujem, že výstupný priečinok existuje
    out_dir.mkdir(parents=True, exist_ok=True)

    # uložím INVERTOVANÝ INDEX
    with INVERTED_INDEX_PATH.open("w", encoding="utf-8") as f:
        for term, plist in postings.items():
            f.write(json.dumps({"term": term, "postings": plist}, ensure_ascii=False) + "\n")

    # uložím DF/IDF pre obe metódy
    with IDF_PATH.open("w", encoding="utf-8") as f:
        for term, plist in postings.items():
            df = len(plist)
            obj = {
                "term": term,
                "df": df,
                "idf_logN": round(idf_logN(df, N), 8),
                "idf_bm25": round(idf_bm25(df, N), 8),
            }
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # uložím meta a štatistiky
    META_PATH.write_text(json.dumps({
        "N": N,
        "avgdl": avgdl,
        "doclen": doclen,          # dĺžky dokumentov – pre normalizáciu v BM25
        "meta": meta,              # doc_id -> {url, drug_name}
        "granularity": "page",     #  dokument = stránka (1 riadok CSV)
        "attrs_joined": list(ATTRS_TO_INDEX),  # ktoré polia sa spojili do jedného textu
        "min_doc_tokens": min_doc_tokens,
        "min_token_len": min_token_len,
    }), encoding="utf-8")

    # Kontrolný výpis do konzoly
    print(f"[OK] Počet dokumentov (stránok): {N}")
    print(f"[OK] Unikátnych termov                  : {len(postings)}")
    print(f"[OK] Priemerný počet tokenov na dok.    : {avgdl:.2f}")
    print(f"[OK] Výstupný priečinok                 : {out_dir.resolve()}")
    print(f"     - {INVERTED_INDEX_PATH.name}, {IDF_PATH.name}, {META_PATH.name}")

# --------- CLI ---------------------------------------------------------------

def main():
    # vstup: načíta parametre, skontroluje CSV a spustí build_index()
    parser = argparse.ArgumentParser(
        description="Postaví invertovaný index (1 dokument = 1 stránka) a predpočíta IDF (TF-IDF aj BM25)."
    )
    parser.add_argument("--csv", type=Path, default=CSV_EXTRACT,
                        help="Cesta k extrahovanému CSV (semicolon + UTF-8 BOM).")
    parser.add_argument("--out", type=Path, default=OUT_DIR,
                        help="Výstupný priečinok (default: data/index).")
    parser.add_argument("--min-doc-tokens", type=int, default=1,
                        help="Minimálny počet tokenov, aby sa stránka indexovala (1 ponechá aj krátke).")
    parser.add_argument("--min-token-len", type=int, default=MIN_TOKEN_LEN_DEFAULT,
                        help="Minimálna dĺžka tokenu (default 2).")

    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"[ERR] Nenájdené CSV: {args.csv}")

    build_index(args.csv, args.out, args.min_doc_tokens, args.min_token_len)

if __name__ == "__main__":
    main()
