import re
import csv
from html import unescape
from pathlib import Path
from typing import List, Optional, Set, Dict
from urllib.parse import urlparse


INPUT_DIR      = Path("data/html_stranky")        # priečinok so stiahnutými HTML súbormi
OUTPUT_CSV     = Path("data/csv/extracted2.csv")  # výstupné CSV s extrahovanými poliami
PROCESSED_LIST = Path("processed_files2.txt")     # zoznam už spracovaných HTML súborov (aby sa nespracovali 2×)
CRAWL_LOG      = Path("crawl_log.csv")            # log z crawlera (filterujeme len status=ok)
EXTRACT_LIMIT  = 20000                            # koľko HTML súborov spracovať v jednom behu (horný limit)

FALLBACK_MAX_CHARS = 4000                         # maximálna dĺžka fallback textu pred skracovaním
FALLBACK_SENTENCES = 6                            # koľko viet ponechať pri skrátení fallbacku

# ========================================================
# funkcie na prácu s csv súborom s extrahovanými dátami a s txt súborom so zoznamom spracovaných stránok

def init_csv(csv_path: Path = OUTPUT_CSV) -> None:
    # vytvorím výstupné CSV s hlavičkou, ak ešte neexistuje
    if not csv_path.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:         # otvorí súbor na zápis (UTF-8 BOM pre Excel)
            w = csv.writer(
                f, delimiter=";", quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar="\\"
            )
            w.writerow([    # zapíše hlavičky stĺpcov v dohodnutom poradí
                "url","drug_name","title","generic_name","brand_names",
                "dosage_forms","drug_class","availability",
                "indications","dosage","side_effects","warnings"
            ])

def append_csv_row(row: List[str], csv_path: Path = OUTPUT_CSV) -> None:
    # pridmá jeden nový riadok do výstupného CSV
    with csv_path.open("a", newline="", encoding="utf-8-sig") as f:             # otvorí CSV v režime append
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar="\\")
        w.writerow(row)                        # zapíše dáta v poradí podľa hlavičky

def load_processed(path: Path = PROCESSED_LIST) -> Set[str]:
    # načítam mená HTML súborov, ktoré už boli spracované (na deduplikáciu spracovania)
    if not path.exists():                       # ak súbor neexistuje, vrátim prázdnu množinu
        return set()
    with path.open("r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())              # vráti set neprázdnych názvov

def save_processed(processed: Set[str], path: Path = PROCESSED_LIST) -> None:
    # uložím aktuálny set spracovaných súborov, aby som nespracovávala súbory viackrát
    with path.open("w", encoding="utf-8") as f:
        for name in sorted(processed):             # v abecednom poradí
            f.write(name + "\n")

def load_ok_file_basenames_from_log(log_path: Path = CRAWL_LOG) -> Set[str]:
    # z crawl_log.csv vyberiem názvy súborov s úspešným statusom 'ok', aby som extrahovala dáta iba z úspešne stiahnutých stránok
    ok_names: Set[str] = set()
    if not log_path.exists():
        print(f"[WARN] Nenašiel som crawl log: {log_path}.")
        return ok_names                                             # vrátim prázdny set
    with log_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)                              # čítam CSV podľa hlavičiek
        for row in reader:
            if (row.get("status") or "").strip().lower() == "ok" and (row.get("filepath") or "").strip():
                ok_names.add(Path(row["filepath"]).name)                    # pridá len názov súboru (bez cesty)
    return ok_names                                                    # vráti množinu úspešne stiahnutých súborov

# ============================================================
# filtrovanie URL stránok podľa toho, či ide o lieky

def get_allowed_section(url: str) -> Optional[str]:
    # podľa URL rozhodne, či ide o sekciu 'pro' | 'mtm' | 'root' alebo None (ignorujem)
    if not url:
        return None
    path = urlparse(url).path or ""          # vyberiem path časť URL (napr. /pro/...)
    if path.startswith("/pro/") and re.match(r"^/pro/[a-z0-9-]+\.html$", path):  # presný tvar /pro/<nazov>.html
        return "pro"                                                             # vráti label „pro“
    if path.startswith("/mtm/") and re.match(r"^/mtm/[a-z0-9-]+\.html$", path):  # presný tvar /mtm/<nazov>.html
        return "mtm"                                                   # vráti label „mtm“
    if re.match(r"^/[a-z0-9-]+\.html$", path):                  # root tvar /<nazov>.html
        return "root"             # vráti label „root“
    return None        # inú štruktúru ignoruje

DENY_PATTERNS = [         # vzory URL (len pre „root“), ktoré ignorujeme pretože ide o zoznamy a nie stránky s liekmi ale nevieme ich inak odfiltlrovať
    r"^/alpha/.*",
    r"^/imprints[a-z0-9-]*\.html$",
    r"^/cg[a-z0-9]+\.html$",
    r"^/sfx-[a-z0-9-]+\.html$",
    r"^/prof[a-z0-9-]*\.html$",
    r"^/pro[0-9a-z-]*\.html$",        # indexy typu pro7.html (nie /pro/<drug>)
    r"^/mult[a-z0-9-]*\.html$",
    r"^/otc-[a-z0-9-]+\.html$",
    r"^/mdx[0-9a-z-]*\.html$",
    r"^/generic-availability-[a-z0-9-]+\.html$",
    r"^/international-[a-z0-9-]+\.html$",
]

def is_denied_url(url: str) -> bool:
    # vrátim True, ak root URL zjavne patrí medzi indexy/zoznamy (deny list)
    if not url:
        return False
    path = urlparse(url).path or ""              # vyber path
    if path.startswith("/pro/") or path.startswith("/mtm/"):        # na 'pro' a 'mtm' deny neaplikujeme
        return False
    return any(re.match(p, path) for p in DENY_PATTERNS)        # test proti deny regexom

def extract_canonical_url(html: str) -> Optional[str]:
    # z HTML vytiahne kanonickú URL (<link rel="canonical"> alebo <meta property="og:url">)
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', html, flags=re.I)
    if m:                      # ak našlo canonical link vráti jeho URL
        return m.group(1).strip()
    m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']', html, flags=re.I)
    if m:
        return m.group(1).strip()
    return None

# ==========================================================

_TAG_RE = re.compile(r"<[^>]+>")         # regex na odstránenie HTML tagov
_WS_RE  = re.compile(r"\s+")           # regex na zjednotenie whitespace

def html_to_text(fragment: str) -> str:
    # odtagujem a znormalizujem medzeru v HTML fragmente
    if not fragment:
        return ""
    txt = unescape(_TAG_RE.sub(" ", fragment))          # odstráni tagy, dekóduje HTML entity
    txt = _WS_RE.sub(" ", txt).strip()         # viac medzier -> jedna, oreže okraje
    return txt           # vrátim čistý text

def extract_first(html: str, patterns: List[str]) -> str:
    # vrátim prvý match zo zoznamu regexov
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
        if m:                             # ak našlo match
            gd = m.groupdict()                     # získam menované skupiny
            if "body" in gd:                          # ak existuje skupina 'body'
                return html_to_text(gd["body"])           # vráť očistený text z nej
            return html_to_text(m.group(1))               # inak prvú zachytenú skupinu
    return ""                                             # ak nič nenašlo, prázdny reťazec

# ========================================================
# regexy na moje atributy

# Title
TITLE_PATTERNS = [r"<title>\s*(?P<body>.*?)\s*</title>"]      # univerzálne vybratie obsahu <title>

# Drug name z <title> – viac tvarov
DRUG_NAME_PATTERNS = [
    r"<title>\s*(?P<body>[^:]+?)\s*:\s*.*?</title>",                     # „Name: ...“
    r"<title>\s*(?P<body>.*?)\s*-\s*Drugs\.com\s*</title>",              # „Name - Drugs.com“
    r"<title>\s*(?P<body>.*?)\s+Uses\b.*?</title>",                      # „Name Uses ...“
    r"<title>\s*(?P<body>.*?)\s+Information from Drugs\.com\s*</title>", # „Name Information from Drugs.com“
]

# Generic / Brand
GENERIC_NAME_PATTERNS = [
    r'(?s)<b>\s*Generic name\s*:\s*</b>\s*(?P<body>.*?)<br',       # HTML vzor 'Generic name:'
    r'(?s)"nonProprietaryName"\s*:\s*"(?P<body>[^"]+)"',
]
BRAND_NAMES_PATTERNS = [
    r'(?s)<b>\s*Brand name(?:s)?\s*:\s*</b>\s*(?P<body>.*?)<br',       # HTML vzor 'Brand name(s):'
    r'(?s)(?:Also|Other)\s+known\s+as:\s*(?P<body>.*?)(?:\.|</p>|</li>|<br)',    # „Also/Other known as:“
    r'(?s)Other\s+brand\s+names\s+of\s+.*?\s+include:\s*(?P<body>.*?)(?:\.|</p>|</li>|<br)',  # ďalší variant
]

# Dosage forms – JSON/Sidebar/inline
DOSAGE_FORMS_PATTERNS = [
    r'(?s)"dosageForm"\s*:\s*"(?P<body>[^"]+)"',
    r'(?s)<dt[^>]*>\s*Dosage forms?\s*</dt>\s*<dd[^>]*>\s*(?P<body>.*?)\s*</dd>',# sidebar
    r'(?s)<p[^>]*class=["\']drug-subtitle["\'][^>]*>.*?(?:<b>\s*Dosage forms?\s*:\s*</b>|Dosage forms?\s*:)\s*(?P<body>.*?)(?:<br\s*/?>|</p>)',
    r'(?s)<li[^>]*>\s*<strong>\s*Dosage forms?\s*:\s*</strong>\s*(?P<body>.*?)</li>',
    r'(?s)Dosage forms?\s*:\s*</?[^>]*>\s*(?P<body>.*?)(?:<|$)',
]

# Drug class
DRUG_CLASS_PATTERNS = [
    r'(?s)<b>\s*Drug class(?:es)?:\s*</b>\s*(?P<body>.*?)\s*(?:<br|</p>)',    # „Drug class:“ v tele
]

# Availability – hlavné cesty a sekcia
AVAILABILITY_MAIN_PATTERNS = [
    r'"prescriptionStatus"\s*:\s*"(?P<body>[^"]+)"',
    r'(?s)<dt[^>]*>\s*Availability\s*</dt>\s*<dd[^>]*>\s*(?P<body>.*?)\s*</dd>', # sidebar
]
AVAILABILITY_SECTION_PATTERN = (                             # blok „Drug Status“
    r'(?s)<h2[^>]*>\s*(?:Drug Status|DRUG STATUS)\s*</h2>(?P<body>.*?)(?:<h2|</aside>|</section>|</main>|</body>)'
)
AVAILABILITY_INLINE_PATTERNS = [
    r'(?s)Availability\s*:\s*</?[^>]*>\s*(?P<body>.*?)(?:<|$)',       # inline „Availability: …“
    r'(?s)<[^>]*>\s*Availability\s*</[^>]*>\s*[:\-]?\s*(?P<body>[^<]+)',    # alternatívny inline zápis
]

# Root/MTM sekcie (spotrebiteľské)
INDICATIONS_PATTERNS_ROOT = [
    r'(?s)<h2[^>]*>\s*(?:What is .*?|Uses)\s*</h2>(?P<body>.*?)(?:<h2[^>]*>|</main>|</body>)',
]
DOSAGE_PATTERNS_ROOT = [
    r'(?s)<h2[^>]*>\s*(?:Dosage|Dosing Information|Recommended dosage)\s*</h2>(?P<body>.*?)(?:<h2[^>]*>|</main>|</body>)'
]
WARNINGS_PATTERNS_ROOT = [
    r'(?s)<h2[^>]*>\s*(?:Warnings|Before taking .*?)\s*</h2>(?P<body>.*?)(?:<h2[^>]*>|</main>|</body>)'
]
SIDE_EFFECTS_PATTERNS_COMMON = [
    r'(?s)<h2[^>]*id=["\']side-effects["\'][^>]*>.*?</h2>(?P<body>.*?)(?:<h2|</main>|</body>)',
    r'(?s)<h2[^>]*id=["\']adverse-reactions["\'][^>]*>.*?</h2>(?P<body>.*?)(?:<h2|</main>|</body>)',
    r'(?s)<h2[^>]*>\s*(?:Side effects|Side Effects|Side effects of .*?)\s*</h2>(?P<body>.*?)(?:<h2|</main>|</body>)',
]

# PRO: Highlights a plné sekcie
def _strip_html(s: str) -> str:
    # odtagujem  script/style a všetky tagy a znormalizuje medzery
    s = s or ""
    s = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", "", s)   # zahodí skripty a štýly
    s = re.sub(r"(?is)<[^>]+>", " ", s)         # všetky tagy nahradí medzerou
    return re.sub(r"\s+", " ", s).strip()       # whitespace normalizácia

def _pro_highlights_block(html: str) -> Optional[str]:
    # vyberiem celé telo „Highlights of Prescribing Information“ (H2 → ďalší H2)
    m = re.search(
        r'(?is)<h2[^>]*>\s*Highlights of Prescribing Information\s*</h2>(?P<body>.*?)(?=<h2[^>]*>)',
        html
    )
    return m.group("body") if m else None        # vráti raw HTML blok alebo None

def _hl_sec(body: str, title_core_regex: str) -> str:
    # z 'Highlights' vymedzí konkrétnu podsekciu (podľa H3/H4 titulku) a odtaguje ju
    m = re.search(
        rf'(?is)<h[34][^>]*>\s*(?:[^<]*?)?(?:{title_core_regex})(?:[^<]*?)?\s*</h[34]>\s*(?P<b>.*?)(?=<h[34][^>]*>|\Z)',
        body
    )
    return _strip_html(m.group("b") if m else "")       # vráti plain text podsekcie

def extract_pro_from_highlights(html: str) -> dict:
    # kúsi vytiahnuť Indications/Dosage/Warnings/Side effects z Highlights; inak vráti prázdne hodnoty
    body = _pro_highlights_block(html)      # najskôr lokalizuje celý blok
    if body is None:          # ak Highlights blok nie je, vráti prázdny dict
        return {}
    return {             # inak extrahuje podsekcie podľa názvov
        "indications":  _hl_sec(body, r"Indications(?:\s*&\s*|\s+and\s+)Usage"),
        "dosage":       _hl_sec(body, r"Dosage(?:\s*&\s*|\s+and\s+)Administration"),
        "warnings":     _hl_sec(body, r"(?:Boxed\s+Warning|WARNING:|Contraindications|Warnings(?:\s*&\s*|\s+and\s+)Precautions|Warnings\b)"),
        "side_effects": _hl_sec(body, r"(?:Adverse\s+Reactions(?:/Side Effects)?|Adverse\s+Events|Side\s+Effects\b)"),
    }

def _pro_block_samelevel(tag: str, title_core_regex: str) -> str:
    # regex na vybratie sekcie z plného PI podľa nadpisu <h2>/<h3> po najbližší rovnaký heading
    end_tag = tag               # sekcia končí ďalším rovnakým headingom
    return (
        rf'(?is)<{tag}[^>]*>\s*(?:\d+\.\s*)?(?:[^<]*?)?{title_core_regex}(?:[^<]*?)?\s*</{tag}>\s*'
        rf'(?P<body>.*?)'
        rf'(?=<{end_tag}[^>]*>|</section>|</main>|</body>|$)'
    )

# PI vzory pre PRO stránky (plné sekcie)
INDICATIONS_PATTERNS_PRO = [
    _pro_block_samelevel("h2", r"Indications(?:\s*&\s*|\s+and\s+)Usage"),
    _pro_block_samelevel("h3", r"Indications(?:\s*&\s*|\s+and\s+)Usage"),
]
DOSAGE_PATTERNS_PRO = [
    _pro_block_samelevel("h2", r"Dosage(?:\s*&\s*|\s+and\s+)Administration"),
    _pro_block_samelevel("h3", r"Dosage(?:\s*&\s*|\s+and\s+)Administration"),
]
WARNINGS_TOGETHER_PATTERNS_PRO = [
    _pro_block_samelevel("h2", r"Warnings(?:\s*&\s*|\s+and\s+)Precautions"),
    _pro_block_samelevel("h3", r"Warnings(?:\s*&\s*|\s+and\s+)Precautions"),
]
WARNINGS_SPLIT_PATTERNS_PRO = [
    _pro_block_samelevel("h2", r"Warnings"),
    _pro_block_samelevel("h3", r"Warnings"),
    _pro_block_samelevel("h2", r"Precautions"),
    _pro_block_samelevel("h3", r"Precautions"),
]
SIDE_EFFECTS_PATTERNS_PRO = [
    _pro_block_samelevel("h2", r"Adverse\s+Reactions(?:/Side Effects)?"),
    _pro_block_samelevel("h3", r"Adverse\s+Reactions(?:/Side Effects)?"),
    _pro_block_samelevel("h2", r"Adverse\s+Events"),
    _pro_block_samelevel("h3", r"Adverse\s+Events"),
]

# ============================================================
# funkcie na skracovanie príliš dlhých sekcií

def _first_sentences(text: str, n: int = FALLBACK_SENTENCES) -> str:
    # ráti prvých N viet z textu (delenie na . ! ? + medzera)
    parts = re.split(r'(?<=[\.\!\?])\s+', (text or "").strip())      # rozdelí text na vety
    return " ".join(parts[:max(1, n)]).strip()          # spojí prvých N viet

def _cap_fallback(text: str) -> str:
    # ak je sekcia príliš dlhá, skráti ju na prvé vety (len pre fallbacky)
    if not text:       # prázdne -> nechaj prázdne
        return text
    if len(text) <= FALLBACK_MAX_CHARS:         # ak je krátky, nekráť
        return text
    return _first_sentences(text, FALLBACK_SENTENCES)       # inak skráť na N viet

# =========================================================
# extrahovanie atribútov

def extract_availability(html: str) -> str:
    # získam 'Availability' z JSON/sidebaru; ak nie je, skúsi sekciu 'Drug Status' a z nej inline
    val = extract_first(html, AVAILABILITY_MAIN_PATTERNS)          # JSON alebo sidebar hodnota
    if val:                                    # ak niečo našiel
        return val
    block = extract_first(html, [AVAILABILITY_SECTION_PATTERN])         # inak skús celý sekčný blok
    if block:                                               # ak existuje blok, skús inline hodnotu vnútri bloku
        inner = extract_first(block, AVAILABILITY_INLINE_PATTERNS)
        return inner or block                   # preferuj inline, inak vráť celý blok textu
    return ""       # nič nenašiel

def extract_dosage_forms(html: str) -> str:
    # vrátim 'Dosage forms' (JSON/side/inline)
    return extract_first(html, DOSAGE_FORMS_PATTERNS)     # použijem postupne definované vzory

def extract_generic_name(html: str) -> str:
    # vytiahnem 'Generic name' (HTML/JSON-LD), oreže prípadnú výslovnosť v [] a očistí medzery.
    raw = extract_first(html, GENERIC_NAME_PATTERNS)
    if not raw:         # ak nič nebolo nájdené
        return ""
    raw = raw.split("[", 1)[0]         # odstráni výslovnosť v hranatých zátvorkách
    return raw.strip(" :;\u00a0").strip()           # odstráni okraje a nadbytočné znaky

def extract_brand_names(html: str) -> str:
    # vytiahnem 'Brand name(s)' alebo príbuzné frázy
    return extract_first(html, BRAND_NAMES_PATTERNS)       # postupná séria vzorov

# ===========================================================
# hlavné funkcie na extrakciu atributov

def extract_one_html(html: str, section: str) -> Dict[str, str]:
    # extrahujem všetky požadované polia z jedného HTML podľa rozpoznanej sekcie ('root'|'mtm'|'pro')
    title_full   = extract_first(html, TITLE_PATTERNS)        # <title>...</title>
    drug_name    = extract_first(html, DRUG_NAME_PATTERNS)    # názov lieku z titulku
    generic_name = extract_generic_name(html)
    brand_names  = extract_brand_names(html)
    dosage_forms = extract_dosage_forms(html)
    drug_class   = extract_first(html, DRUG_CLASS_PATTERNS)
    availability = extract_availability(html)

    if section == "pro":                         # pre PRO stránky: najprv Highlights
        hl = extract_pro_from_highlights(html)         # vytiahni skrátené sekcie z Highlights
        indications = hl.get("indications", "")
        dosage      = hl.get("dosage", "")
        warnings    = hl.get("warnings", "")
        side_effects= hl.get("side_effects", "")

        if not indications:                # ak v Highlights chýbajú, skús plné PI
            indications = extract_first(html, INDICATIONS_PATTERNS_PRO)
            indications = _cap_fallback(indications)           # prípadne skráť ak je príliš dlhá text
        if not dosage:
            dosage = extract_first(html, DOSAGE_PATTERNS_PRO)
            dosage = _cap_fallback(dosage)
        if not warnings:
            warnings = extract_first(html, WARNINGS_TOGETHER_PATTERNS_PRO)         # spolu „Warnings & Precautions“
            if not warnings:                                                       # ak nie je spolu, skús oddelene
                w_part = extract_first(html, WARNINGS_SPLIT_PATTERNS_PRO[:2])      # časť „Warnings“
                p_part = extract_first(html, WARNINGS_SPLIT_PATTERNS_PRO[2:])      # časť „Precautions“
                warnings = " ".join(x for x in (w_part, p_part) if x)              # spojiť, ak existujú
            warnings = _cap_fallback(warnings)
        if not side_effects:
            side_effects = extract_first(html, SIDE_EFFECTS_PATTERNS_PRO) or \
                           extract_first(html, SIDE_EFFECTS_PATTERNS_COMMON)    # fallback na „common“ vzory
            side_effects = _cap_fallback(side_effects)
    else:                            # root/mtm: spotrebiteľské nadpisy
        indications  = extract_first(html, INDICATIONS_PATTERNS_ROOT)
        dosage       = extract_first(html, DOSAGE_PATTERNS_ROOT)
        warnings     = extract_first(html, WARNINGS_PATTERNS_ROOT)
        side_effects = extract_first(html, SIDE_EFFECTS_PATTERNS_COMMON)

    return {
        "title":        title_full,
        "drug_name":    drug_name,
        "generic_name": generic_name,
        "brand_names":  brand_names,
        "dosage_forms": dosage_forms,
        "drug_class":   drug_class,
        "availability": availability,
        "indications":  indications,
        "dosage":       dosage,
        "side_effects": side_effects,
        "warnings":     warnings,
    }

# ============================== Main =========================================

def main():
    # načítam HTML, prefiltrované URL, extrahuje polia a uloží do CSV
    init_csv()              # pripraví CSV (ak chýba, vytvorí s hlavičkou)
    processed = load_processed()         # načíta set už spracovaných HTML mien

    if not INPUT_DIR.exists():          # kontrola, že priečinok s HTML existuje
        print(f"[ERROR] Neexistuje vstupný priečinok: {INPUT_DIR}")
        return

    all_html_files = sorted([p for p in INPUT_DIR.iterdir() if p.suffix.lower() == ".html"])  # zoznam všetkých .html

    ok_names = load_ok_file_basenames_from_log(CRAWL_LOG)       # z logu zober len súbory so status=ok
    if not ok_names:
        print(f"[WARN] V logu {CRAWL_LOG} som nenašiel žiadne status=ok; nemám čo spracovať.")
        candidates = []                # prázdny zoznam kandidátov
    else:
        candidates = [p for p in all_html_files if p.name not in processed and p.name in ok_names]

    if not candidates:          # ak nie sú kandidáti
        print("[INFO] Nie sú žiadne nové HTML (status=ok) na extrakciu.")
        return

    to_process = candidates[:EXTRACT_LIMIT]                  # uplatní horný limit spracovania
    print(f"[RUN] Na extrakciu teraz: {len(to_process)} z {len(candidates)} nových (status=ok).")

    done_now = 0    # počítadlo práve spracovaných
    for path in to_process:                                 # iterácia cez kandidátov
        try:
            html = path.read_text(encoding="utf-8", errors="ignore")        # načítaj HTML ako text
        except Exception as e:
            print(f"[READ-ERROR] {path.name}: {e}")
            processed.add(path.name)         # označ ako spracované (aby sa nezacyklilo)
            continue    # pokračuj ďalším súborom

        url = extract_canonical_url(html) or ""         # z HTML vyťahni kanonickú URL
        section = get_allowed_section(url)              # rozhodni, či je to 'root'/'mtm'/'pro'
        if not section or is_denied_url(url):       # ak sekcia nevyhovuje alebo je deny
            print(f"[SKIP] {path.name}  url={url or '(unknown)'}  section={section}  (denied/unsupported)")
            processed.add(path.name)       # označ ako spracované a preskoč
            done_now += 1
            continue

        print(f"[EXTRACT] file={path.name}  url={url or '(unknown)'}  section={section}")

        data = extract_one_html(html, section)   # extrahuj všetky polia z HTML

        append_csv_row([           # zapíš výsledný riadok do CSV
            url, data["drug_name"], data["title"], data["generic_name"], data["brand_names"],
            data["dosage_forms"], data["drug_class"], data["availability"],
            data["indications"], data["dosage"], data["side_effects"], data["warnings"],
        ])

        processed.add(path.name)       # označ tento súbor za spracovaný
        done_now += 1

    save_processed(processed)     # ulož stav spracovaných súborov

    print(f"[DONE] Spracovaných teraz: {done_now}  (limit={EXTRACT_LIMIT})")
    print(f"[STATE] Spolu spracovaných súborov: {len(processed)}")
    print(f"[STATE] CSV: {OUTPUT_CSV.resolve()}")

if __name__ == "__main__":
    main()
