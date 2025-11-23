import lucene
from java.io import File
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.queryparser.classic import QueryParser

# sdresár, kde je uložený Lucene index
# !!! musí byť rovnaký ako INDEX_DIR v indexeri, inak searcher uvidí iný / prázdny index.
INDEX_DIR = "lucene_index_drugs"

def main():
    # štart JVM (Java Virtual Machine) pre PyLucene
    # moja poznamka: PyLucene je wrapper okolo Java Lucene, takže pred použitím
    # musím spustiť JVM. Bez initVM() by volania Lucene tried zlyhali.
    lucene.initVM()
    print("Lucene version:", lucene.VERSION)

    # Otvorenie existujúceho indexu na disku
    directory = FSDirectory.open(File(INDEX_DIR).toPath())

    # DirectoryReader.open(directory): otvorí index v read-only režime
    reader = DirectoryReader.open(directory)

    # IndexSearcher:nad "reader" vie vykonávať dopyty (Query) a vracať výsledky
    searcher = IndexSearcher(reader)

    # Analyzer musí byť rovnakého typu ako ten, ktorý som použila pri indexovaní
    # (StandardAnalyzer) – aby QueryParser rovnako tokenizovat dopyty ako mám tokenizované dáta
    analyzer = StandardAnalyzer()

    # QueryParser prekladá textový dopyt na Lucene Query objekt.
    # - podporuje syntax: field:value, "frazove dopyty", AND, OR, atď.

    # kde sa ma defaultne vyhľadávať ak v dopyte nezadám konkretny field
    # default_field = "drug_name"
    # default_field = "indications"
    # default field nastavím na full_text, aby voľný text hľadal nad celým dokumentom
    default_field = "full_text"
    parser = QueryParser(default_field, analyzer)

    # pomocný výpis na začiatok
    print(f"Načítaný index z: {INDEX_DIR}")
    print(f"Počet dokumentov v indexe: {reader.numDocs()}")
    print("Môžeš skúsiť napríklad:")
    print("  aspirin")
    print("  generic_name:ibuprofen")
    print("  availability:rx-only")
    print('  wiki_tradename:"Panadol"')
    print()

    # slučka – čakanie na dopyty od používateľa ak nezada prazny dopyt
    # ============================================
    while True:
        try:
            # input() a strip() – odstráň medzery na okrajoch
            query_str = input("Zadaj dopyt (alebo prázdny pre koniec): ").strip()
        except (EOFError, KeyboardInterrupt):
            # Ctrl+D (EOF) alebo Ctrl+C (KeyboardInterrupt) – ukončí program
            break

        # ak používateľ zadá prázdny riadok tak tiež ukonči slučku
        if not query_str:
            break

        try:
            # Parsovanie dopytu -> QueryParser:
            #   - rozbije dopyt podľa syntaxe (field:hodnota, frázy v uvodzovkách, AND/OR)
            #   - použije analyzer na tokenizáciu (lowercase, stop words)
            #   - vytvorí Lucene Query objekt, ktorý vie IndexSearcher spustiť
            query = parser.parse(query_str)
        except Exception as e:
            # ak dopyt nie je správny (napr. zlé úvodzovky),vypíšem chybu a spýtam sa používateľa znova
            print("Chyba pri parsovaní dopytu:", e)
            continue

        # searcher.search(query, k) - vráti TopDocs s k najlepšími výsledkami podľa skóre (vráti top 5)
        top_docs = searcher.search(query, 5)

        # totalHits je objekt, preto z neho treva vytiahnúť ešte value
        total_hits = top_docs.totalHits.value()
        print(f"Nájdených dokumentov: {total_hits}")

        # ak mi nič nenašlo, spýtajm sa používateľa na ďalší dopyt
        if total_hits == 0:
            continue

        # stored_fields slúži na načítanie uložených polí pre daný doc ID
        stored_fields = searcher.storedFields()

        # Prejdi všetky nájdené dokumenty (max 5, podľa search(query, 5))
        for rank, score_doc in enumerate(top_docs.scoreDocs, start=1):
            # score_doc.doc = interné ID dokumentu v indexe
            # stored_fields.document(id) vráti Document s uloženými poľami
            doc = stored_fields.document(score_doc.doc)

            # Načítanie polí, ktoré sú Store.YES v indexeri
            # doc.get("field_name") vráti hodnotu uloženého poľa alebo None,
            # preto používam "or ''", aby som nemala 'None' v printoch
            url = doc.get("url") or ""
            drug_name = doc.get("drug_name") or ""
            generic_name = doc.get("generic_name") or ""
            brand_names = doc.get("brand_names") or ""
            availability = doc.get("availability") or ""
            wiki_url = doc.get("wiki_url") or ""
            wiki_tradename = doc.get("wiki_tradename") or ""
            wiki_synonyms = doc.get("wiki_synonyms") or ""

            # score_doc.score = skóre, ktoré Lucene pridelil dokumentu pre daný dopyt
            print("=" * 80)
            print(f"Výsledok #{rank}  (score={score_doc.score})")
            print(f"  drug_name      : {drug_name}")
            print(f"  generic_name   : {generic_name}")
            print(f"  brand_names    : {brand_names}")
            print(f"  availability   : {availability}")
            print(f"  url (drugs.com): {url}")
            print(f"  wiki_tradename : {wiki_tradename}")
            print(f"  wiki_synonyms  : {wiki_synonyms}")
            print(f"  wiki_url       : {wiki_url}")

        print("-" * 80)

    # zatvorenie readera a directory po skončení - dôležité kvôli uvoľneniu file handlerov a zdrojov v JVM
    reader.close()
    directory.close()
    print("Koniec vyhľadávania.")


if __name__ == "__main__":
    main()


    # ============================================
    # Príklady podporovaných typov vyhľadávania
    #
    # 1) Voľný text (default field = "indications")
    #    psoriasis
    #    rheumatoid arthritis
    #    "severe plaque psoriasis"
    #
    # 2) Vyhľadávanie v konkrétnom poli (field:hodnota)
    #      drug_name:aspirin
    #      availability:otc
    #      wiki_tradename:"Panadol Osteo"
    #      wiki_synonyms:"acetylsalicylic acid"
    #      wiki_routes:oral
    #      side_effects:nausea
    #
    # 3) Frázové dopyty (uvozovky)
    #    "chronic plaque psoriasis"
    #    indications:"rheumatoid arthritis"
    #    wiki_summary:"tumor necrosis factor"
    #
    # 4) Boolean operátory (AND, OR, NOT, +, -)
    #      psoriasis AND subcutaneous
    #      indications:psoriasis AND wiki_routes:subcutaneous
    #      psoriasis OR eczema
    #      wiki_routes:oral OR wiki_routes:topical
    #      psoriasis NOT arthritis
    #      psoriasis -arthritis
    #      +psoriasis +biologic -topical
    #
    # 5) Wildcards (* a ?)
    #   (ľubovoľný koniec slova):
    #      wiki_synonyms:acetyl*
    #      side_effects:nause*
    #    Jednoznakové:
    #      wiki_atc:N02BE0?
    #
    # 6) Kombinované multi-field dopyty
    #    indications:psoriasis AND wiki_routes:subcutaneous AND availability:rx-only
    #    wiki_tradename:Humira OR brand_names:Humira
    #    generic_name:adalimumab AND wiki_pregnancy:C


