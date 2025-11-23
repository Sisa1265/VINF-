import argparse
import json
import math
import re
from pathlib import Path
from typing import Dict, List, Set

# ============
# tokenizacia dopytu (rovnako ako v indexeri)

MIN_TOKEN_LEN_DEFAULT = 2  # minimálna dĺžka tokenu (slova)

def tokenize(text: str, min_token_len: int = MIN_TOKEN_LEN_DEFAULT) -> List[str]:
    # prevedie text na malé písmená, odstráni nežiaduce znaky, rozdelí na tokeny (slova)
    text = (text or "").lower()  # prevedie text na malé písmená
    text = re.sub(r"[^a-z0-9%\./\-+ ]+", " ", text)      # ponechá len znaky a-z, čísla, medicínske znaky
    toks = re.split(r"\s+", text)  # rozdelí podľa medzier
    return [t for t in toks if t and len(t) >= min_token_len]  # odfiltruje prázdne a krátke tokeny


# ============
# načítam si všetky súbory, ktore som vytvorila pri indexovaní

def load_inverted_index(path: Path) -> Dict[str, Dict[str, int]]:
    # načítam inverted_index.jsonl → pre každý term zoznam dokumentov a ich frekvencie
    inv = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)  # načíta jeden JSON objekt (term + postings)
            inv[obj["term"]] = {k: int(v) for k, v in obj["postings"].items()}  # prevod na dict
    return inv

def load_idf_table(path: Path) -> Dict[str, Dict[str, float]]:
    # načítam idf.jsonl → pre každý term uloží DF, IDF pre TF-IDF aj BM25
    idf = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            idf[obj["term"]] = {
                "df": float(obj["df"]),              # počet dokumentov obsahujúcich term
                "idf_logN": float(obj["idf_logN"]),  # klasický logaritmický IDF
                "idf_bm25": float(obj["idf_bm25"]),  # upravený IDF pre BM25
            }
    return idf

def load_meta(path: Path):
    # načítam meta.json → obsahuje celkové štatistiky a metadáta dokumentov
    meta_json = json.loads(path.read_text(encoding="utf-8"))
    N = int(meta_json["N"])  # počet dokumentov
    avgdl = float(meta_json["avgdl"])  # priemerná dĺžka dokumentov
    doclen = {k: int(v) for k, v in meta_json["doclen"].items()}  # dĺžky dokumentov
    meta = meta_json["meta"]  # info o lieku (názov, url)
    return N, avgdl, doclen, meta


# ============
# výber dokumentov, v ktorých sa nachádzajú moje hladané termy v dopyte

def candidate_docs_SOFT(terms: List[str], inv_index: Dict[str, Dict[str, int]]) -> Set[str]:
    # vrátim všetky dokumenty, ktoré obsahujú všetky z dotazových termov (ak nie su žiadne take dokuemnty tak vráti tie, kde ich je čo najvoac)
    cand = set()
    for t in dict.fromkeys(terms):
        cand |= set(inv_index.get(t, {}).keys())  # union všetkých dokumentov
    return cand


# ============
# výpočet skóre – TF-IDF alevo BM25
def tfidf_score(query_terms, doc_id, inv_index, idf_table):
    # vypočítam TF-IDF skóre pre dokument podľa dotazu
    score = 0.0
    seen = set()
    for t in query_terms:
        if t in seen:  # ak už term bol spracovaný, preskočím ho
            continue
        seen.add(t)
        plist = inv_index.get(t)  # postings list pre term
        if not plist:
            continue
        tf = plist.get(doc_id, 0)  # term frequency pre daný dokument
        if tf <= 0:
            continue
        idf = idf_table.get(t, {}).get("idf_logN", 0.0)  # IDF pre TF-IDF
        tf_w = 1.0 + math.log(tf)  # váženie logaritmom TF
        score += tf_w * idf        # súčet príspevkov jednotlivých termov
    return score

def bm25_score(query_terms, doc_id, inv_index, idf_table, doclen, avgdl, k1=1.2, b=0.75):
    # vypočítam BM25 skóre pre dokument podľa dotazu
    dl = doclen.get(doc_id, 0)  # dĺžka dokumentu
    if dl <= 0:
        return 0.0

    score = 0.0
    seen = set()
    for t in query_terms:
        if t in seen:
            continue
        seen.add(t)
        plist = inv_index.get(t)
        if not plist:
            continue
        tf = plist.get(doc_id, 0)  # term frequency
        if tf <= 0:
            continue
        idf = idf_table.get(t, {}).get("idf_bm25", 0.0)  # IDF pre BM25
        denom = tf + k1 * (1 - b + b * (dl / (avgdl if avgdl > 0 else 1.0)))  # normalizácia podľa dĺžky
        score += idf * (tf * (k1 + 1)) / (denom if denom != 0 else 1.0)  # hlavný BM25 vzorec
    return score


# ============
# hlavná funkcia na vyhľadávanie

def search(query, index_dir, method="bm25", topk=5):
    # vyhľadám relevantné dokumenty podľa dotazu pomocou Boolean logiky
    inv_path = index_dir / "inverted_index.jsonl"
    idf_path = index_dir / "idf.jsonl"
    meta_path = index_dir / "meta.json"

    # skontrolujem, či všetky súbory existujú
    if not (inv_path.exists() and idf_path.exists() and meta_path.exists()):
        raise SystemExit("[ERR] Chýbajú indexové súbory")

    # načítanie dát
    inv_index = load_inverted_index(inv_path)
    idf_table = load_idf_table(idf_path)
    N, avgdl, doclen, meta = load_meta(meta_path)

    # tokenizácia dotazu
    q_terms = tokenize(query)

    # výber kandidátov
    candidates = candidate_docs_SOFT(q_terms, inv_index)
    if not candidates:
        return [], meta

    # výpočet skóre podľa zvolenej metódy
    scored = []
    if method == "bm25":
        for did in candidates:
            s = bm25_score(q_terms, did, inv_index, idf_table, doclen, avgdl)
            if s > 0:
                scored.append((did, s))
    else:  # TF-IDF
        for did in candidates:
            s = tfidf_score(q_terms, did, inv_index, idf_table)
            if s > 0:
                scored.append((did, s))

    # zoradenie podľa skóre (najvyššie prvé)
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topk], meta



def main():
    """Spúšťa vyhľadávanie cez príkazový riadok."""
    parser = argparse.ArgumentParser(
        description="Soft Boolean vyhľadávanie (union kandidátov) s BM25 alebo TF-IDF."
    )
    parser.add_argument("query", type=str, help='Prirodzený dotaz, napr. "treatment for crohn disease"')
    parser.add_argument("--index-dir", type=Path, default=Path("data/index"),
                        help="Priečinok s indexovými súbormi.")
    parser.add_argument("--method", choices=["bm25", "tfidf"], default="bm25",
                        help="Skórovacia metóda (default bm25).")
    parser.add_argument("--topk", type=int, default=5, help="Počet výsledkov (default 5).")

    args = parser.parse_args()  # načítanie argumentov z CLI

    # spustenie vyhľadávania
    results, meta = search(args.query, args.index_dir, args.method, args.topk)

    # ak sa nič nenašlo
    if not results:
        print("Žiadne výsledky (žiadny dokument neobsahuje ani jeden term).")
        return

    # výpis výsledkov
    print(f"\nTOP {len(results)} výsledkov  (metóda: {args.method})  [Soft Boolean]:")
    for i, (did, score) in enumerate(results, start=1):
        m = meta.get(did, {})
        print(f"{i:>2}. doc_id={did} | {m.get('drug_name','(unknown)')} | {m.get('url','-')} | score={score:.4f}")

# Spustenie hlavnej funkcie pri priamom spustení skriptu
if __name__ == "__main__":
    main()


# python searcher.py "subcutaneous dosing schedule for ustekinumab in the treatment of Crohn disease after induction phase" --method bm25 --topk 5
# python searcher.py "subcutaneous dosing schedule for ustekinumab in the treatment of Crohn disease after induction phase" --method tfidf --topk 5

# python searcher.py "common and serious side effects of infliximab including risk of infections tuberculosis and malignancies" --method bm25 --topk 5
# python searcher.py "common and serious side effects of infliximab including risk of infections tuberculosis and malignancies" --method tfidf --topk 5

# python searcher.py "treatment Crohn biologic" --method bm25 --topk 5
# python searcher.py "treatment Crohn biologic" --method tfidf --topk 5

# python searcher.py "boxed warning tofacitinib" --method bm25 --topk 5
# python searcher.py "boxed warning tofacitinib" --method tfidf --topk 5

# python searcher.py "treatment options for severe plaque psoriasis with subcutaneous biologic administration" --method bm25 --topk 5
# python searcher.py "treatment options for severe plaque psoriasis with subcutaneous biologic administration" --method tfidf --topk 5
