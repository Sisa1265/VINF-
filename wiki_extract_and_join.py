import os
import sys
import re
import html
from typing import Optional
from urllib.parse import quote



# Nastavenie prostredia pre Spark na Windows
# Spark executory aj driver nech používajú ten istý Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hadoop závislosti
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

# Ignorovanie timezone warningov pri Arrow konverzii
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

from pyspark.sql import SparkSession, Row, functions as F
from pyspark import StorageLevel
from pyspark.sql import Window

# Cesty k dátam
EXTRACTED_CSV      = "data/csv/extracted.csv"        # CSV z 1. zadania (Drugs.com extrakcia)
WIKI_DUMPS_PATTERN = "data/wiki/*.bz2"               # Wiki XML dumpy
OUT_TSV_DIR        = "data/join/drugs_wiki_join"     # Výstupný adresár pre joinnuté TSV

# Regexy na parsovanie Wiki <page> blokov
# <title>...</title> – názov článku
TITLE_RE   = re.compile(r"<title>(.*?)</title>", re.S)
# <text ...>...</text> – hlavný wikitext článku
TEXT_RE    = re.compile(r"<text[^>]*>(.*?)</text>", re.S)
# <redirect title="..."> – redirect stránky
REDIR_RE   = re.compile(r'<redirect[^>]*title="([^"]+)"')

#--------------------------------------------
# Pomocné funkcie na čistenie a spracovanie textu

def clean_val(s: str, *, drop_markers: bool = True) -> str:
    # vyčistím hodnotu z infoboxu alebo wikitextu
    if not s:
        return ""

    # prevedenie HTML entít na obyčajné znaky
    s = html.unescape(s)
    # HTML komentáre <!-- ... -->
    s = re.sub(r"<!--.*?-->", " ", s, flags=re.S)
    # <ref>...</ref> citácie – často obsahujú zdroje, nie samotnú hodnotu
    s = re.sub(r"<ref[^>]*>.*?</ref>", " ", s, flags=re.S)
    # Šablóny {{...}} – jednoduché odstránenie bez rekurzívneho parsovania
    s = re.sub(r"\{\{[^{}]*\}\}", " ", s)
    # Wikilinky [[ ... ]] – odstránim hranaté zátvorky ale vnútorný text nechám
    s = s.replace("[[", "").replace("]]", "")
    # formátovanie '''bold''' a ''italic''
    s = s.replace("'''", "").replace("''", "")
    # HTML tagy (napr. <b>, <i>, <span>, <sup>, ...)
    s = re.sub(r"<[^>]+>", " ", s)
    # normalizujem whitespace (všetky medzery, tab, newline -> jedna medzera)
    s = re.sub(r"\s+", " ", s).strip()

    if drop_markers:
        # pre typické marker hodnoty vrátime prázdny string
        low = s.lower()
        if low in {"none", "n/a", "na", "not applicable", "unknown", "unk", "any"}:
            return ""

    return s


def remove_infobox_from_text(wt: str) -> str:
    # z wikitextu odstránim celý infobox (Infobox drug / Drugbox / Chembox / ...),ktorý je na začiatku článku
    if not wt:
        return ""

    # nájdem začiatok konkrétnych typov infoboxov
    m = re.search(
        r"\{\{\s*(infobox\s*drug|drugbox|chembox|infobox\s*chemical|infobox\s*enzyme|infobox\s*anatomy)\b",
        wt,
        flags=re.I,
    )
    if not m:
        # ak žiadny infobox nenajdem – vrátim pôvodný text
        return wt

    start = m.start()
    text = wt[start:]

    # počítadlo hĺbky vnorených {{ }}  - v niektorých infoboxoch sa nachádzalo viacero vnorení, ktoré mi potom robili problém
    depth = 0
    end_idx = None
    i = 0
    while i < len(text):
        if text.startswith("{{", i):
            depth += 1
            i += 2
            continue
        if text.startswith("}}", i):
            depth -= 1
            i += 2
            if depth <= 0:
                # Našli sme koniec infoboxu
                end_idx = i
                break
            continue
        i += 1

    if end_idx is None:
        # ak sa nepodarilo nájsť koniec, odseknem text od začiatku infoboxu
        return wt[:start]

    # vrátim text bez úvodného infoboxu
    return wt[:start] + text[end_idx:]


def first_paragraph(wt: str) -> str:
    # vrátim prvý odstavec článku (krátky summary alebo opis lieku)

    if not wt:
        return ""
    wt = remove_infobox_from_text(wt)
    wt = clean_val(wt)

    # odrežem text za určitými sekciami, ktoré nie sú súčasťou popisu lieku
    wt = re.split(r"\n==\s*(References|See also|External links|Further reading)\s*==", wt)[0]

    # rozdelím na odstavce podľa prázdneho riadku
    parts = re.split(r"\n\s*\n", wt)
    return (parts[0].strip() if parts else "")[:4000]


# ----------------------------------------
# extrakcia tela infoboxu z wikitextu
def extract_infobox_body(wikitext: str) -> str:
    # nájdem Infobox (Infobox drug, Drugbox, Chembox, Infobox chemical, enzyme, anatomy)
    if not wikitext:
        return ""

    # index začiatku infoboxu
    m = re.search(
        r"\{\{\s*(infobox\s*drug|drugbox|chembox|infobox\s*chemical|infobox\s*enzyme|infobox\s*anatomy)\b",
        wikitext,
        flags=re.I,
    )
    if not m:
        return ""

    start = m.start()
    text = wikitext[start:]

    depth = 0
    end_idx = None
    i = 0
    while i < len(text):
        if text.startswith("{{", i):
            depth += 1
            i += 2
            continue
        if text.startswith("}}", i):
            depth -= 1
            i += 2
            if depth <= 0:
                end_idx = i
                break
            continue
        i += 1

    if end_idx is None:
        # ak sa mi nepodarí nájsť koniec, beriem zvyšok textu
        box = text
    else:
        box = text[:end_idx]

    # rozdelím celý box na riadky
    lines = box.splitlines()
    if not lines:
        return ""

    # preskočíme prvý riadok - je tam hlavička šablóny {{Infobox drug}}
    core_lines = []
    for line in lines[1:]:
        # ignorujem čisto uzatvárací riadok '}}'
        if line.strip() == "}}":
            continue
        core_lines.append(line)

    # vrátim telo infoboxu (riadky s kľúč=hodnota)
    return "\n".join(core_lines)


#---------------------------------------------
# regex vzory na extrakciu konkrétnych polí z infoboxu.
# každý pattern berie hodnotu za '=' až po prvý '|' na riadku.

FIELD_PATTERNS = {
    "tradename": r"^\s*\|\s*(?:tradename|trade_name|trade_names)\s*=\s*(?P<val>[^|]+)",
    "synonyms":  r"^\s*\|\s*(?:synonyms|other_names|aka|also_known_as)\s*=\s*(?P<val>[^|]+)",
    "routes":    r"^\s*\|\s*(?:route|routes|routes_of_administration|routes_of_administration2?)\s*=\s*(?P<val>[^|]+)",
    "atc":       r"^\s*\|\s*(?:atc|atc_code|atc_codes?|atc_prefix|atc_suffix)\s*=\s*(?P<val>[^|]+)",
    "half_life": r"^\s*\|\s*(?:elimination[_\s-]*half[-\s]*life|half[-\s]*life)\s*=\s*(?P<val>[^|]+)",
    "cas":       r"^\s*\|\s*(?:cas_number|cas_no|casno|CAS_number)\s*=\s*(?P<val>[^|]+)",
    "legal_status": r"^\s*\|\s*legal_status\s*=\s*(?P<val>[^|]+)",

    # pregnancy polia – rôzne spôsoby zápisu, ktore neskôr spojím do jedného fieldu
    "pregnancy_AU": r"^\s*\|\s*pregnancy[_\s]*AU\s*=\s*(?P<val>[^|]+)",
    "pregnancy_US": r"^\s*\|\s*pregnancy[_\s]*US\s*=\s*(?P<val>[^|]+)",
    "pregnancy_UK": r"^\s*\|\s*pregnancy[_\s]*UK\s*=\s*(?P<val>[^|]+)",
    "pregnancy_generic": r"^\s*\|\s*pregnancy\s*=\s*(?P<val>[^|]+)",
    "pregnancy_category": r"^\s*\|\s*pregnancy_category\s*=\s*(?P<val>[^|]+)",
}

BASE_WIKI_URL = "https://en.wikipedia.org/wiki/"


def make_wiki_url(title: str) -> str:
    # vygenerujem URL na Wikipedia článok z <title>.
    if not title:
        return ""
    slug = title.replace(" ", "_")
    # quote() zaistí escapovanie špeciálnych znakov v URL
    return BASE_WIKI_URL + quote(slug)


def normalize_atc(val: str) -> str:
    # z hodnoty ATC odstránim text 'ATC code', 'ATC codes', 'ATC' a nechám len samotné kódy
    if not val:
        return ""
    v = clean_val(val)
    v = re.sub(r"(?i)\bATC\s*codes?\b", " ", v)
    v = re.sub(r"(?i)\bATC\s*code\b", " ", v)
    v = re.sub(r"(?i)\bATC\b", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


def parse_infobox_fields(body: str) -> dict:
    # z infoboxu vytiahnem hodnoty jednotlivých polí podľa FIELD_PATTERNS
    if not body:
        return {}
    out = {}
    for key, pat in FIELD_PATTERNS.items():
        m = re.search(pat, body, flags=re.I | re.M)
        if not m:
            continue
        raw = m.group("val")
        val = clean_val(raw)

        # ATC kódy normalizujem trochu špeciálne (odstránime text 'ATC code')
        if key == "atc":
            val = normalize_atc(val)

        if not val:
            continue

        out[key] = val
    return out


#--------------------------------------------------------------
# vytvorenie SparkSession
def make_spark() -> SparkSession:
    # vytvorím SparkSession v lokálnom režime

    return (
        SparkSession.builder
        .master("local[4]")        # 4 jadrá
        .appName("WikiJoinDrugs")
        .config("spark.driver.bindAddress", "127.0.0.1")    # fixnutie na 127.0.0.1
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.network.timeout", "600s")                  # zabránenie time-outom pri dlhších operáciách
        .config("spark.executor.heartbeatInterval", "60s")        # zabránenie time-outom pri dlhších operáciách
        .config("spark.python.worker.reuse", "true")      # opätovné použitie worker procesov
        .getOrCreate()
    )


def split_pages(part_iter):
    # funkcia na rozdelenie raw XML dumpu na jednotlivé <page> bloky
    buf = []
    in_page = False
    for row in part_iter:
        s = row[0]  # spark.read.text -> Row(value=...); row[0] = obsah riadku
        if "<page>" in s:
            # začiatok novej stránky – resetujem buffer
            in_page = True
            buf = [s]
        elif "</page>" in s and in_page:
            # koniec stránky – pridaj riadok
            buf.append(s)
            yield "\n".join(buf)
            in_page = False
            buf = []
        elif in_page:
            # som uprostred stránky – len pridávam riadky
            buf.append(s)


#--------------------------
def main():
    # vytvorím SparkSession
    spark = make_spark()

    # print("[SELFTEST] Python:", sys.executable)
    # print("[SELFTEST] Version:", sys.version)

    # načítam si extrahované dáta z 1. zadania (Drugs.com)
    if not os.path.exists(EXTRACTED_CSV):
        raise SystemExit(f"[ERR] Nenašiel som vstupné CSV: {EXTRACTED_CSV}")

    df_drugs = (
        spark.read
        .option("header", True)        # prvý riadok obsahuje header
        .option("sep", ";")            # CSV je oddelené bodkočiarkou
        .option("encoding", "utf-8")
        .csv(EXTRACTED_CSV)
    )

    # Oprava prípadného BOM v názve prvého stĺpca
    first_col = df_drugs.columns[0]
    if first_col.startswith("\ufeff"):
        df_drugs = df_drugs.withColumnRenamed(first_col, first_col.lstrip("\ufeff"))

    # vytvorím si normalizovaný generic_name (lower + trim), ktorý slúži ako join kľúč k wiki titulkom
    df_drugs = df_drugs.withColumn(
        "generic_norm",
        F.lower(F.trim(F.col("generic_name")))
    )

    # dropnem riadky bez generického názvu - nebudem ich mať ako spojiť
    df_drugs = df_drugs.filter(
        F.col("generic_norm").isNotNull() & (F.col("generic_norm") != "")
    )

    print("[INFO] Počet riadkov v CSV:", df_drugs.count())
    print("[INFO] Počet unikátnych generic_name:", df_drugs.select("generic_norm").distinct().count())

    # načítam si wiki dump a extrahujem relevantné stránky
    # raw: DataFrame s jedným stĺpcom 'value' (riadky XML)
    raw = spark.read.text(WIKI_DUMPS_PATTERN)

    # uložím si všetky jedinečné generic_norm z df_drugs ako Python list
    generic_names = [r["generic_norm"] for r in df_drugs.select("generic_norm").distinct().collect()]
    generic_set = set(generic_names)
    # Broadcast-set – posielam do executors, aby vedeli kontrolovať, či title patrí medzi lieky
    generic_bc = spark.sparkContext.broadcast(generic_set)

    # regex na filtrovanie meta stránok podľa title, ktoré by mi mohli prejsť cez regexy ale sú to iba zoznamy nie stránky o liekoch
    INVALID_TITLE_RE = re.compile(
        r"^(ATC code\b|Category:|Template:|List of\b|Index of\b|Outline of\b)",
        re.I
    )

    def parse_page(xml: str) -> Optional[Row]:
        # parsujem jeden <page> XML blok a v prípade, že title po normalizácii z wiki patrí do množiny generic_norm (lieky z Drugs.com),
        # vytvorím Row s extrahovanými wiki dátami a infobox informáciami

        # Title a text článku
        title_m = TITLE_RE.search(xml)
        text_m  = TEXT_RE.search(xml)
        # redirect (ak by bol)
        redir_m = REDIR_RE.search(xml)

        if not title_m:
            return None

        title = title_m.group(1).strip()
        title_norm = title.lower().strip()

        # vyfiltrujem neobsahové stránky napríklad zoznamy,..
        if INVALID_TITLE_RE.match(title):
            return None

        # zaujímajú ma len tie tituly, ktoré sa zhodujú s generic_norm z Drugs.com
        if title_norm not in generic_bc.value:
            return None

        wikitext = text_m.group(1) if text_m else ""
        redirect_to = redir_m.group(1) if redir_m else ""

        # Alternatívne by sa dali vyhodiť stránky, ktoré sú len čistý redirect
        # if wikitext.lstrip().lower().startswith("#redirect"):
        #     return None

        # telo infoboxu
        body = extract_infobox_body(wikitext)
        has_infobox = bool(body)

        # flag, či sa v infoboxe nachádza ATC kód
        has_atc = bool(re.search(
            r"^\s*\|\s*(atc|atc_code|atc_codes?|atc_prefix|atc_suffix)\s*=",
            body, flags=re.I | re.M
        ))

        # parsovanie konkrétnych polí z infoboxu
        fields = parse_infobox_fields(body)

        # spojenie pregnancy kategórií z rôznych polí do jedného stringu aby som v tom mala prehľad
        preg_values = []
        if "pregnancy_AU" in fields:
            preg_values.append(f"AU:{fields['pregnancy_AU']}")
        if "pregnancy_US" in fields:
            preg_values.append(f"US:{fields['pregnancy_US']}")
        if "pregnancy_UK" in fields:
            preg_values.append(f"UK:{fields['pregnancy_UK']}")
        if "pregnancy_generic" in fields:
            preg_values.append(f"GEN:{fields['pregnancy_generic']}")
        if "pregnancy_category" in fields:
            preg_values.append(f"CAT:{fields['pregnancy_category']}")

        preg_string = ", ".join(preg_values) if preg_values else ""

        # spravím si URL na wiki článok
        wiki_url = make_wiki_url(title)

        # vrátim jednu "wiki" ako Row – bude to riadok v df_wiki
        return Row(
            wiki_title=title,
            title_norm=title_norm,
            wiki_url=wiki_url,
            wiki_has_infobox=has_infobox,
            wiki_has_atc=has_atc,
            wiki_summary=first_paragraph(wikitext),
            wiki_tradename=fields.get("tradename", ""),
            wiki_synonyms=fields.get("synonyms", ""),
            wiki_routes=fields.get("routes", ""),
            wiki_atc=fields.get("atc", ""),
            wiki_half_life=fields.get("half_life", ""),
            wiki_cas=fields.get("cas", ""),
            wiki_legal_status=fields.get("legal_status", ""),
            wiki_pregnancy=preg_string
        )

    # prevedieme raw DataFrame na RDD, zoskupíme riadky do <page> blokov
    pages_rdd = raw.rdd.mapPartitions(split_pages)
    rows_rdd = pages_rdd.map(parse_page).filter(lambda r: r is not None)

    # vytvorím DataFrame z extrahovaných Row a persistneme (pre opakované použitie)
    df_wiki = spark.createDataFrame(rows_rdd).persist(StorageLevel.MEMORY_AND_DISK)

    wiki_count = df_wiki.count()
    uniq_wiki_titles = df_wiki.select("title_norm").distinct().count()
    print(f"[INFO] Počet wiki stránok, kde title zodpovedá generic_name: {wiki_count}")
    print(f"[INFO] Počet unikátnych wiki titulkov (po normalizácii): {uniq_wiki_titles}")

    # JOIN -> Drugs.com dáta a Wiki dáta
    # najprv vyberiem najlepší záznam z df_wiki pre každý title_norm -> "Najlepší" = s infoboxom + dlhším summary.
    w = Window.partitionBy("title_norm").orderBy(
        F.desc("wiki_has_infobox"),         # preferuj riadky, ktoré majú infobox
        F.desc(F.length("wiki_summary"))    # a dlhší summary
    )

    df_wiki_best = (
        df_wiki
        # spravila som si voliteľné skóre, lebo sa mi stavalo, že som mala viacero zaznamov pre jeden liek pričom v jednom bol
        # len redirect na druhu stranku tak tieto som nechcela brat do úvahy
        .withColumn("row_score",
                    F.when(F.col("wiki_has_infobox"), 1).otherwise(0) +
                    F.when(F.length("wiki_summary") > 0, 1).otherwise(0))
        # priradím poradie v rámci skupiny (title_norm), najlepší má rn = 1
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)  # necháme si len najlepší riadok
        .drop("rn", "row_score")   # pomocné stĺpce už nepotrebujeme
    )

    # JOIN -> normalizovaný generický názov = normalizovaný wiki.title
    df_join = (
        df_drugs
        .join(df_wiki_best, on=(df_drugs.generic_norm == df_wiki_best.title_norm), how="left")
        .drop("generic_norm", "title_norm")  # joinovacie stĺpce vo výsledku už netreba tak ich dropnem
    )

    joined_count = df_join.count()
    uniq_join_generic = df_join.select("generic_name").distinct().count()
    print(f"[INFO] Počet riadkov po JOIN-e: {joined_count}")
    print(f"[INFO] Počet unikátnych generic_name po JOIN-e: {uniq_join_generic}")

    # zápis výsledného datasetu do TSV (Spark CSV s tab delimiterom)
    if os.path.exists(OUT_TSV_DIR) and not os.path.isdir(OUT_TSV_DIR):
        # Ak OUT_TSV_DIR existuje ako súbor, nechceme ho prepísať adresárom
        raise SystemExit(f"[ERR] {OUT_TSV_DIR} existuje ako súbor, zmaž ho alebo premenuj.")

    # Vytvor adresár, ak neexistuje
    os.makedirs(OUT_TSV_DIR, exist_ok=True)

    (
        df_join
        .coalesce(1)                # zlúčim všetko do jedného výstupného súboru (part-00000)
        .write
        .mode("overwrite")          # prepíš prípadný starý obsah
        .option("header", True)     # prvý riadok bude hlavička
        .option("delimiter", "\t")  # použijem tabulátor ako oddeľovač
        .csv(OUT_TSV_DIR)
    )

    print(f"[OK] Join dataset uložený do: {OUT_TSV_DIR}")
    print("    (vnútri nájdeš súbor part-*.csv, obsahuje TSV dáta)")

    # Ukončenie SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
