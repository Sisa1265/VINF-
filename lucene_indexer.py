import os
import csv
import lucene

# Importy Java tried z Lucene cez PyLucene wrapper
from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, TextField, StringField


# cesty k dátam
# Vstupný TSV súbor – výsledok joinu Drugs.com + Wikipedia zo Sparku
INPUT_TSV = "data/join/drugs_wiki_join/part-00000-7cc105f0-9d80-4cfd-ace5-aa037ddbd2f9-c000.csv"

# Adresár, kam sa uloží Lucene index -> Lucene vytvorí množinu súborov
INDEX_DIR = "lucene_index_drugs"


def normalize(s: str) -> str:
    # pomocná funkcia na normalizáciu stringov: odstráni whitespace z okrajov, prevedie na malé písmená
    if s is None:
        return ""
    return s.strip().lower()


def create_index():
    # hlavná funkcia, ktorá:
    # - inicializuje PyLucene (JVM),
    # - pripraví IndexWriter,
    # - načíta vstupný TSV súbor,
    # - pre každý riadok vytvorí Document s príslušnými poľami,
    # - pridá dokumenty do indexu,
    # - commitne a uzavrie index.

    # inicializujem JVM (java virtual machine) pre PyLucene
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])     # zakáže GUI (headless režim)
    print("Lucene version:", lucene.VERSION)

    # príprava FSDirectory a IndexWriter-a
    # Paths.get vytvorí java.nio.file.Path na adresár s indexom
    index_path = Paths.get(INDEX_DIR)

    # FSDirectory reprezentuje adresár na disku, kde sa budú ukladať súbory indexu
    directory = FSDirectory.open(index_path)

    # StandardAnalyzer – základný analyzer Lucene, ktorý tokenizuje text, prevádza na lowercase a filtruje stop words
    analyzer = StandardAnalyzer()

    # Konfigurácia IndexWriter-a – nastavím mu, aby používal StandardAnalyzer
    config = IndexWriterConfig(analyzer)

    # OpenMode.CREATE vždy vytvorí nový index teda ak existuje starý, prepisuje ho.
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)

    # IndexWriter – zápis dokumentov do indexu
    writer = IndexWriter(directory, config)

    # skontrolujem, či existuje vstupný súbor
    if not os.path.exists(INPUT_TSV):
        # Ak vstupný súbor neexistuje, ukonči program s chybovým hlásením
        raise SystemExit(f"Input TSV neexistuje: {INPUT_TSV}")

    # otvorím TSV súbor v textovom režime
    with open(INPUT_TSV, "r", encoding="utf-8", newline="") as f:
        # DictReader vráti každý riadok ako dict {názov_stĺpca: hodnota}
        reader = csv.DictReader(f, delimiter="\t")

        count = 0  # počítadlo zaindexovaných dokumentov

        # postupne prechádzam všetky riadky súboru
        for row in reader:
            # vytvorím nový Lucene Document – každý záznam/liek = jeden dokument v indexe
            doc = Document()

            # URL z Drugs.com – presný string na zobrazenie

            # get("url", "") – vezme hodnotu zo stĺpca "url", ak chýba, vráti ""
            url = row.get("url", "") or ""
            if url:
                # StringField: NEanalyzované pole (berie sa ako jeden token), Store.YES – hodnotu chcem vedieť z dokumentu načítať (zobraziť ju).
                doc.add(StringField("url", url, Field.Store.YES))

            # Hlavné názvy – fulltext + zobrazovanie

            drug_name = row.get("drug_name", "") or ""
            generic_name = row.get("generic_name", "") or ""
            title = row.get("title", "") or ""
            brand_names = row.get("brand_names", "") or ""

            # tieto polia chcem fulltextovo vyhľadávať aj zobrazovať, preto použijem TextField + Store.YES.
            if drug_name:
                doc.add(TextField("drug_name", drug_name, Field.Store.YES))
            if generic_name:
                doc.add(TextField("generic_name", generic_name, Field.Store.YES))
            if brand_names:
                doc.add(TextField("brand_names", brand_names, Field.Store.YES))

            # 'title' – názov stránky, ktorý môže pomôcť pri fulltexte, ale nechcem ho zobrazovať -> Store.NO.
            if title:
                doc.add(TextField("title", title, Field.Store.NO))

            #  exact match polia – normalizované názvy - vytvorím si pomocne polia, kde budu normalizované hodnoty na exact match
            # generic_name_exact / drug_name_exact:StringField – NEanalyzované, normalize() – lower + trim, Store.NO – nezobrazujú sa
            if generic_name:
                doc.add(StringField("generic_name_exact", normalize(generic_name), Field.Store.NO))
            if drug_name:
                doc.add(StringField("drug_name_exact", normalize(drug_name), Field.Store.NO))

            # polia z Drugs.com – obsahové texty
            dosage_forms = row.get("dosage_forms", "") or ""
            drug_class = row.get("drug_class", "") or ""
            availability = row.get("availability", "") or ""
            indications = row.get("indications", "") or ""
            dosage = row.get("dosage", "") or ""
            side_effects = row.get("side_effects", "") or ""
            warnings = row.get("warnings", "") or ""

            # tieto polia sú často dlhé texty – chcem v nich vyhľadávať,
            # ale nezobrazovať ich v základnom výsledku -> TextField + Store.NO.
            if dosage_forms:
                doc.add(TextField("dosage_forms", dosage_forms, Field.Store.NO))
            if drug_class:
                doc.add(TextField("drug_class", drug_class, Field.Store.NO))
            if indications:
                doc.add(TextField("indications", indications, Field.Store.NO))
            if dosage:
                doc.add(TextField("dosage", dosage, Field.Store.NO))
            if side_effects:
                doc.add(TextField("side_effects", side_effects, Field.Store.NO))
            if warnings:
                doc.add(TextField("warnings", warnings, Field.Store.NO))

            # 'availability' – kategória typu Rx-only, OTC atď
            # chceš ju používať ako filter (presný string) -> StringField + normalize() a zobrazovať vo výsledkoch -> Store.YES
            if availability:
                doc.add(StringField("availability", normalize(availability), Field.Store.YES))

            # Polia z Wikipédie
            wiki_title = row.get("wiki_title", "") or ""
            wiki_url = row.get("wiki_url", "") or ""
            wiki_summary = row.get("wiki_summary", "") or ""
            wiki_tradename = row.get("wiki_tradename", "") or ""
            wiki_synonyms = row.get("wiki_synonyms", "") or ""
            wiki_routes = row.get("wiki_routes", "") or ""
            wiki_atc = row.get("wiki_atc", "") or ""
            wiki_half_life = row.get("wiki_half_life", "") or ""
            wiki_cas = row.get("wiki_cas", "") or ""
            wiki_legal_status = row.get("wiki_legal_status", "") or ""
            wiki_pregnancy = row.get("wiki_pregnancy", "") or ""
            wiki_has_infobox = row.get("wiki_has_infobox", "") or ""
            wiki_has_atc = row.get("wiki_has_atc", "") or ""

            # wiki_title : text, ktorý môže pomôcť pri fulltexte, nepotrebujem ho zobrazovať => TextField + Store.NO
            if wiki_title:
                doc.add(TextField("wiki_title", wiki_title, Field.Store.NO))

            # wiki_tradename, wiki_synonyms: texty, ktoré chcem vyhľadávať aj zobrazovať -> TextField + Store.YES
            if wiki_tradename:
                doc.add(TextField("wiki_tradename", wiki_tradename, Field.Store.YES))
            if wiki_synonyms:
                doc.add(TextField("wiki_synonyms", wiki_synonyms, Field.Store.YES))

            # wiki_summary_ veľmi užitočný pre fulltext ale príliš dlhý na zobrazovanie v základnom výsledku -> TextField + Store.NO.
            if wiki_summary:
                doc.add(TextField("wiki_summary", wiki_summary, Field.Store.NO))

            # wiki_routes, wiki_legal_status, wiki_pregnancy:
            #  textové polia vhodné na fulltextové vyhľadávanie, nezobrazujú sa priamo => Store.NO.
            if wiki_routes:
                doc.add(TextField("wiki_routes", wiki_routes, Field.Store.NO))
            if wiki_legal_status:
                doc.add(TextField("wiki_legal_status", wiki_legal_status, Field.Store.NO))
            if wiki_pregnancy:
                doc.add(TextField("wiki_pregnancy", wiki_pregnancy, Field.Store.NO))

            # wiki_url – presná URL článku: StringField (bez analýzy), chcem ju zobraziť ako odkaz vo výsledkoch -> Store.YES
            if wiki_url:
                doc.add(StringField("wiki_url", wiki_url, Field.Store.YES))

            # wiki_atc, wiki_cas – kódy: exact match polia, nepotrebuješ ich zobrazovať -> StringField + Store.NO.
            if wiki_atc:
                doc.add(StringField("wiki_atc", wiki_atc, Field.Store.NO))
            if wiki_cas:
                doc.add(StringField("wiki_cas", wiki_cas, Field.Store.NO))

            # wiki_half_life – text využiteľný vo fulltexte, nezobrazujem ho => TextField + Store.NO.
            if wiki_half_life:
                doc.add(TextField("wiki_half_life", wiki_half_life, Field.Store.NO))

            # wiki_has_infobox, wiki_has_atc – boolean flagy:
            #   - využiteľné na filtrovanie (napr. články, ktoré majú infobox/ATC),
            #   - StringField ("true"/"false"), Store.NO.
            if wiki_has_infobox:
                doc.add(StringField("wiki_has_infobox", wiki_has_infobox.lower(), Field.Store.NO))
            if wiki_has_atc:
                doc.add(StringField("wiki_has_atc", wiki_has_atc.lower(), Field.Store.NO))

            # full_text pole – agregovaný text nad celým dokumentom, len na fulltext, nezobrazovať
            full_text_parts = []

            for val in [
                drug_name,
                generic_name,
                title,
                brand_names,
                dosage_forms,
                drug_class,
                availability,
                indications,
                dosage,
                side_effects,
                warnings,
                wiki_title,
                wiki_summary,
                wiki_tradename,
                wiki_synonyms,
                wiki_routes,
                wiki_atc,
                wiki_half_life,
                wiki_cas,
                wiki_legal_status,
                wiki_pregnancy,
            ]:
                if val:
                    full_text_parts.append(val)

            full_text = " ".join(full_text_parts)

            if full_text:
                doc.add(TextField("full_text", full_text, Field.Store.NO))

            # pridám kompletne nadefinovaný dokument do indexu
            writer.addDocument(doc)
            count += 1

            # pomocný výpis každých 1000 dokumentov
            if count % 1000 == 0:
                print(f"Zaindexovaných dokumentov: {count}")

    # commit() – zapíše všetky zmeny do indexu na disk
    writer.commit()
    # close() – uvoľní zdroje spojené s IndexWriter
    writer.close()
    # zatvorím aj Directory objekt
    directory.close()

    print(f"Indexovanie hotovo, celkom dokumentov: {count}")
    print(f"Index adresár: {INDEX_DIR}")


if __name__ == "__main__":
    # Spustenie indexovania, ak sa skript spúšťa priamo
    create_index()

"""
Poznámky k Dockeru / spúšťaniu:

  Ukončenie:
    exit                 # ukonči Python REPL v kontajneri
    docker stop pylucene # zastav kontajner

  Spustenie:
    docker start pylucene          # naštartuj kontajner 
    docker exec -it pylucene bash  # pripoj sa do shellu v kontajneri
    python3 lucene_indexer.py      # spusti indexovanie vo vnútri kontajnera
"""
