import lucene
from java.io import File
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, BoostQuery
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import QueryBuilder

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
    # (StandardAnalyzer) – aby QueryBuilder rovnako tokenizoval dopyty ako mám tokenizované dáta
    analyzer = StandardAnalyzer()

    # QueryParser prekladá textový dopyt na Lucene Query objekt.
    # - podporuje syntax: field:value, "frazove dopyty", AND, OR, atď.
    # V praxi tu použijeme QueryBuilder, ktorý nám vie vytvoriť fulltext query nad jedným poľom.
    qb = QueryBuilder(analyzer)

    # kde sa ma defaultne vyhľadávať ak v dopyte nezadám konkretny field
    # default_field = "drug_name"
    # default_field = "indications"
    # default field nastavím na full_text, aby voľný text hľadal nad celým dokumentom
    default_field = "full_text"

    # váhy (boosty) pre jednotlivé polia pri VOĽNOM texte (bez "field:")
    field_boosts = [
        ("drug_name", 3.0),
        ("generic_name", 3.0),
        ("brand_names", 3.0),
        ("wiki_tradename", 3.0),
        ("indications", 2.0),
        ("wiki_summary", 2.0),
        ("full_text", 1.0),
    ]

    # pomocný výpis na začiatok
    print(f"Načítaný index z: {INDEX_DIR}")
    print(f"Počet dokumentov v indexe: {reader.numDocs()}")
    print("Môžeš skúsiť napríklad:")
    print("  psoriasis")
    print("  generic_name:ibuprofen")
    print("  availability:rx-only")
    print('  indications:Crohn disease')
    print()

    # pomocná funkcia na vytvorenie query pre jednoduchý field:hodnota dopyt
    def build_field_query(q_text: str):
        # Jednoduchý parser pre dopyty typu 'field:hodnota'.
        if ":" not in q_text:
            return None

        field, value = q_text.split(":", 1)
        field = field.strip()
        value = value.strip()

        # odstraň okolité úvodzovky, ak sú
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]

        if not field or not value:
            return None

        # QueryBuilder.createBooleanQuery(field, text) – spraví OR nad termami (podľa analyzera)
        q = qb.createBooleanQuery(field, value)
        return q

    # pomocná funkcia: voľný text → multi-field váhovaný BooleanQuery
    def build_weighted_multi_field_query(text: str):
        # vytvorí BooleanQuery, ktorý hľadá text v niekoľkých poliach naraz, pričom každé pole má svoju váhu
        builder = BooleanQuery.Builder()
        for field, boost in field_boosts:
            q_field = qb.createBooleanQuery(field, text)
            if q_field is None:
                continue
            if boost != 1.0:
                q_field = BoostQuery(q_field, boost)
            builder.add(q_field, BooleanClause.Occur.SHOULD)
        return builder.build()

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
            # Rozlíšenie typu dopytu:
            # - ak obsahuje ":", skúsime ho interpretovať ako jednoduchý field:hodnota
            # - inak ho berieme ako voľný text nad viacerými poliami s váhami
            if ":" in query_str:
                query = build_field_query(query_str)
                if query is None:
                    # fallback: ak sa nepodarilo spraviť field query, použijem voľný text
                    query = build_weighted_multi_field_query(query_str)
            else:
                query = build_weighted_multi_field_query(query_str)

        except Exception as e:
            # ak dopyt nie je správny (napr. prázdne pole), vypíšem chybu a spýtam sa používateľa znova
            print("Chyba pri vytváraní dopytu:", e)
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
    # 1) Voľný text (váhovaný multi-field, boostované názvy a indikácie)
    #    psoriasis
    #    treatment options for severe plaque psoriasis with subcutaneous biologic administration
    #
    # 2) Jednoduchý field:hodnota (cez QueryBuilder)
    #      generic_name:ibuprofen
    #      availability:rx-only
    #      indications:Crohn disease
    #      wiki_tradename:Humira
