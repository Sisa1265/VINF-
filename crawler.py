import os
import re
import csv
import time
import random
import requests
from datetime import datetime
from collections import deque
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

# ----------------- Konfigurácia -----------------
BASE_URL = "https://www.drugs.com"
HEADERS = {
    "User-Agent": "STU-FIIT-StudentCrawler/1.0 (course Information Retrieval; contact: xkuchtovas@stuba.sk)"
}
SAVE_DIR = "data/html_stranky"

TO_VISIT_QUEUE_FILE = "to_visit_queue.txt"   # FIFO fronta čakajúcich URL
VISITED_FILE = "visited.txt"                 # set už navštívených URL
CSV_LOG = "crawl_log.csv"                    # logovanie behu crawlu

CRAWL_DELAY_BASE = 8                        # pauza po úspešnom stiahnutí
FAILED_URLS_FILE = "failed.txt"              # finálne zlyhané URL po vyčerpaní pokusov

# Jednoduchý backoff requeue - pri chybe vraciame URL na koniec fronty až MAX_RETRIES krát
MAX_RETRIES = 3
RETRY_COUNTS_FILE = "retry_counts.txt"       # evidujeme počty pokusov: TSV (url \t count)


# ----------------------------------
#  funkcie na prácu s robot.txt

def get_robots_parser(base_url=BASE_URL, cache_file="robots.txt"):
    parser = RobotFileParser()

    # zistím, či som už robot.txt raz stiahla a mám ho uložený - ak áno, sparsujem ho
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            parser.parse(f.read().splitlines())
        return parser

    # ak som ho ešte nestiahla tak ho stiahnem a sparsujem
    try:
        resp = requests.get(base_url + "/robots.txt", headers=HEADERS, timeout=(10, 30))
        if resp.status_code == 200:
            with open(cache_file, "w", encoding="utf-8") as f:
                f.write(resp.text)
            parser.parse(resp.text.splitlines())
    except requests.exceptions.RequestException:
        # Ak sieť zlyhá, ponechám parser prázdny (teda default bude allow all)
        pass

    return parser


def is_allowed(url, parser):
    # overím či môžem danú url sťahovať podľa robot.txt
    return parser.can_fetch("*", url)


# ----------------------------------
#  funkcie na prácu so zoznamom navštívených stránok
def load_visited(filename=VISITED_FILE):
    # Načítam visited.txt pomocu set aby sme nemali duplicity
    if not os.path.exists(filename):
        return set()
    with open(filename, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_visited(visited, filename=VISITED_FILE):
    # uloží zoznam navštívených url do súboru
    with open(filename, "w", encoding="utf-8") as f:
        for u in sorted(visited):
            f.write(u + "\n")


# ----------------------------------
# funkcie na prácu s frontou (FIFO)
def load_queue(filename=TO_VISIT_QUEUE_FILE):
    # Načítam FIFO frontu z disku do deque aby som zachovala poradie url
    if not os.path.exists(filename):
        return deque()
    with open(filename, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    return deque(lines)


def save_queue(q, filename=TO_VISIT_QUEUE_FILE):
    # uložím aktuálne poradie aby som ho zachovala (FIFO)
    with open(filename, "w", encoding="utf-8") as f:
        for u in q:
            f.write(u + "\n")


# ----------------------------------
# práca s retry_counts - je to počaet pokusov sťahovania aby som stránku nesťahovala donekonečna

def load_retry_counts():
    # načítam si url, ktorú chcem stiahnúť a pokusy, koľkokrát už som sa ju pokúsila stiahnuť
    d = {}
    if os.path.exists(RETRY_COUNTS_FILE):
        with open(RETRY_COUNTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                url, cnt = line.strip().split("\t")
                d[url] = int(cnt)
    return d


def save_retry_counts(d):
    # uložím si url aj s počtom pokusov, koľkokrát som ju chcela stiahnúť
    with open(RETRY_COUNTS_FILE, "w", encoding="utf-8") as f:
        for url, cnt in d.items():
            f.write(f"{url}\t{cnt}\n")


# ---------------------------------
# inicializujem si csv súbor, v ktorom si udržiavam základé údaje o sťahovaných súboroch

def init_csv(csv_path=CSV_LOG):
    # vytvorím si hlavičku dokumentu aby som potom už iba pridávala informácie
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "url", "filepath", "status", "http_status", "bytes", "elapsed_ms"])


def log_csv(url, filepath, status, http_status=None, nbytes=None, elapsed_ms=None):
    # pridávam nové riadky do csv súboru aby som mala prehľad
    with open(CSV_LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            datetime.now().isoformat(timespec="seconds"),           # čas kedy som stiahla súbor
            url,                                                    # url, ktorú som sťahovala
            filepath or "",                                         # kde som to uložila (ak sa mi to podarilo inak prázdneň
            status,                                                 # status sťahovania (či sa mi to podarilo alebo je tam výnimkaň
            (http_status if http_status is not None else ""),       # aky status som dostala pri odpovedi (napr. 200)
            (nbytes if nbytes is not None else ""),                 # veľkosť stiahnuého súboru (ak sa stiahol)
            (elapsed_ms if elapsed_ms is not None else "")          # ako dlho trvala odpoveď od servera
        ])


# ----------------------------------
def normalize_url(u: str) -> str:
    # normalizujem url aby bola malými písmenami, bola bez query a koncovej lomky aby som predišla duplicitám
    s = urlsplit(u)
    scheme = "https"
    netloc = s.netloc.lower()
    path = s.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def extract_links(html, base_url):
    # prechádzam html subor a hľadám referencie na stránky
    links = set()
    for match in re.findall(r'href=[\'"]([^\'"]+)[\'"]', html, flags=re.IGNORECASE):
        # preskočím pseudo-odkazy a e-mailové odkazy
        if not match or match.startswith(("javascript:", "#", "mailto:")):
            continue
        # zmením relatívne adresy na absolútne podľa base_url
        full = urljoin(base_url, match)
        # normalizujem url aby mala malé písmená, bola bez query a poslednje lomky
        full = normalize_url(full)
        # skontrolujem, či daná adresa zostáva na mojej doméne drugs.com - tie ktore idu mimo zahodím
        if urlparse(full).netloc == urlparse(base_url).netloc:
            links.add(full)
    return links


def safe_filename_from_url(url):
    # vytvorím názov súboru podľa url ktoru spracovávam - odstranim protokol, nahradím / _ aby to nebralo
    # # ako cestu a pridam .html ak nie je
    name = re.sub(r"^https?://", "", url).replace("/", "_")
    if not name.endswith(".html"):
        name += ".html"
    return os.path.join(SAVE_DIR, name)


def polite_sleep(base=CRAWL_DELAY_BASE):
    # náhodná pauza po úspešnom stiahnutí url (base + náhodný jitter 0–3 s).
    time.sleep(base + random.uniform(0.0, 3.0))


# ----------------------------------
# hlavná funkcia pre sťahovanie url

def download_page(url, parser):
    # skontrolujem či url nie je zakazana podľa robot.txt, ak áno tak skončím
    if not is_allowed(url, parser):
        print(f"[BLOCKED] {url}")
        log_csv(url, None, status="blocked")
        return None, set(), "blocked", None, None, None

    # cieľový priečinok a názov súboru
    os.makedirs(SAVE_DIR, exist_ok=True)
    filepath = safe_filename_from_url(url)

    try:
        # samotný request
        t0 = time.time()                # čas odoslania požiadavky
        resp = requests.get(url, headers=HEADERS, timeout=(10, 40)) # 10 sekund čakam na spojenie so serverom a 40 na odpoveď
        elapsed_ms = int((time.time() - t0) * 1000)   # čas ako dlho trvalo serveru odpovedat na požiadavku v milisekundách
        http_status = resp.status_code
        ctype = (resp.headers.get("Content-Type") or "").lower()  # zistim, aky obsah mi server poslal - či je to html napr.

        # úslešne stiahnute html
        if http_status == 200 and "text/html" in ctype:
            # tvytorim si subor, kde zapíšem odpoveď servera (moje html)
            html = resp.text
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html)
            # pokúsim sa získať linky z hrml
            found = extract_links(html, BASE_URL)
            nbytes = len(resp.content)   # zistím si veľkosť súboru
            print(f"[OK] {url}  ({nbytes} B)")
            # vložím si údaje o súbore a url do csv súboru
            log_csv(url, filepath, status="ok", http_status=http_status, nbytes=nbytes, elapsed_ms=elapsed_ms)
            # počkám určitý čas kým začnej sťahovať novú url
            polite_sleep()
            return filepath, found, "ok", http_status, nbytes, elapsed_ms

        # úspešne stiahnuté, ale nie HTML (obrázky, PDF, JSON…) - preskočíme
        elif http_status == 200:
            print(f"[SKIP-NONHTML] {url}  Content-Type={ctype}")
            log_csv(url, None, status="skip-nonhtml", http_status=http_status, nbytes=len(resp.content), elapsed_ms=elapsed_ms)
            polite_sleep()
            return None, set(), "skip-nonhtml", http_status, len(resp.content), elapsed_ms

        # neúslešné stiahnutie - chyba
        else:
            print(f"[ERROR] {url} ({http_status})")
            log_csv(url, None, status="http-error", http_status=http_status, elapsed_ms=elapsed_ms)
            time.sleep(2)  # krátka pauza po chybe
            return None, set(), "http-error", http_status, None, elapsed_ms

    except requests.exceptions.RequestException as e:
        # výnimky pri sťahovaní
        print(f"[EXCEPTION] {url}: {e}")
        log_csv(url, None, status="exception")
        time.sleep(5)  # dlhšia pauza po výnimke
        return None, set(), "exception", None, None, None


# ----------------- Hlavný crawl (FIFO + requeue backoff) -----------------
def crawl_step(limit):
    # vytvorím csv súbor pre zapisovanie informácií o url, datume, stave
    init_csv()
    # získam parser robot.txt
    parser = get_robots_parser()

    # načítam aktuálne zoznamy
    to_visit_q = load_queue()         # FIFO fronta
    visited = load_visited()          # zoznam už spracovaných URL
    to_visit_set = set(to_visit_q)    # pomocná množina

    retry_counts = load_retry_counts()  # mapovanie url -> pokusy

    # ak je prvý beh (prázdna fronta aj visited), vložím BASE_URL do zoznamu
    if not to_visit_q and BASE_URL not in visited:
        seed = normalize_url(BASE_URL)
        to_visit_q.append(seed)
        to_visit_set.add(seed)

    count = 0  # koľko url spracujeme v tomto behu

    while to_visit_q and count < limit:
        print('count: ', count)
        # zoberiem ďalšiu URL z čela fronty
        url = to_visit_q.popleft()
        to_visit_set.discard(url)

        # ak sme ju už spracovali v inom behu, preskoč danu url
        if url in visited:
            continue

        # pokus o stiahnutie
        filepath, found_links, status, *_ = download_page(url, parser)

        count += 1

        if status == "ok":
            # stiahnutu url adresu pridám do zoznamu stiahnutých
            visited.add(url)
            # url som úspešne stiahla, preto vynulujem counter pre počítanie pokusov stiahnutia
            if url in retry_counts:
                del retry_counts[url]
            # do zoznamov pridám nové linky, ktore som ešte nenavštívila (nie su v to_visit_set)
            for u in found_links:
                if u not in visited and u not in to_visit_set:
                    to_visit_q.append(u)
                    to_visit_set.add(u)
        # ak sa mi url nepodarilo stiahnúť
        elif status in ("http-error", "exception"):
            # zistím, koľký pokus o sťahovanie tejto url to už bol
            attempts = retry_counts.get(url, 0) + 1   # ak je tam 0 tak vráti 1 = prvý pokus
            retry_counts[url] = attempts        # uložím späť počet pokusov o stiahnutie
            # ak som ešte nedosiahla limit pokusov sťahovania, vrátim ju do zoznamu
            if attempts <= MAX_RETRIES:
                to_visit_q.append(url)   # vráť na koniec fronty (časom sa k nej dostaneme znova)
                to_visit_set.add(url)
                log_csv(url, None, status="deferred")
            else:
                # po vyčerpaní pokusov už túto URL nechceme skúšať znova
                visited.add(url)
                # dosiahla som limit pokusov sťahovania, vložím do súboru neúspešne stiahnutých
                with open(FAILED_URLS_FILE, "a", encoding="utf-8") as f:
                    f.write(url + "\n")
                log_csv(url, None, status="gave-up")
                if url in retry_counts:
                    del retry_counts[url]

        else:
            visited.add(url)
            # ak je to blocked / skip-nonhtml, neskúšame znova stiahnúť a odstránime počet pokusov
            if url in retry_counts:
                del retry_counts[url]

    # uložím stavy zoznamov (fronta, visited, retry počty)
    save_queue(to_visit_q)
    save_visited(visited)
    save_retry_counts(retry_counts)

    print("\n--- STAV SŤAHOVANIA PO SPUSTENÍ PROGRAMU ---")
    print(f" Navštívené spolu: {len(visited)}")
    print(f" Zostáva vo fronte: {len(to_visit_q)}")
    print(f" Spracované v tomto behu: {count}")

    return count


# ----------------- Spustenie -----------------
if __name__ == "__main__":
    # limit - koľko url chcem spracovať v jednom toku
    limit = 2500

    zaciatok = time.strftime("%H:%M")
    print('zaciatok: ', zaciatok)
    crawl_step(limit)

    koniec = time.strftime("%H:%M")
    print('koniec: ', koniec )