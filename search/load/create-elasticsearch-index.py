import string

from elasticsearch import Elasticsearch
from ..search.library.postgresql import PostgresQL
from ..search.config import config

month_conversion = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12"
}


if __name__=='__main__':
    database = config.ProductionConfig.DATABASE["database"]
    password = config.ProductionConfig.DATABASE["password"]

    print("Establish connection")
    # prepare postgresql connection
    pg = PostgresQL()
    pg.connect(database, password)
    # prepare elasticsearch connection
    print("Prepare elasticsearch index")
    es = Elasticsearch()
    # delete the envirolens index, if existing
    es.indices.delete(index='envirolens', ignore=[400, 404])
    # create a new, fresh index
    es.indices.create(index='envirolens', ignore=400, body={
        "settings": {
            "index": {
                "max_result_window": 500000,
                "max_inner_result_window": 500000
            },
            "analysis": {
                "normalizer": {
                    "lowercase_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "document_id": { "type": "keyword" },
                "title": { "type": "text" },
                "abstract": { "type": "text" },
                "fulltext": { "type": "text" },
                "name": { "type": "text" },
                "link": { "type": "keyword" },

                "category": { "type": "keyword" },
                "date": { "type": "date" },

                "basin": { "type": "keyword" },
                "source": { "type": "keyword" },
                "status": { "type": "text" },
                "celex": { "type": "keyword" },

                "ref_number": { "type": "keyword" },

                "named_entities": {
                    "type": "nested",
                    "properties": {
                        "name": {
                            "type": "keyword",
                            "normalizer": "lowercase_normalizer"
                        },
                        "type": { "type": "keyword" }
                    }
                },

                "wikipedia": {
                    "type": "nested",
                    "properties": {
                        "name": { "type": "text" },
                        "cosine": { "type": "float" },
                        "pagerank": { "type": "float" },
                        "lang": { "type": "keyword" },
                        "url": { "type": "keyword" }
                    }
                },
                # "informea" is an array of strings
                # "keywords" is an array of strings
                # "languages" is an array of strings
                # "areas" is an array of strings
                # "subjects" is an array of strings
            }
        }
    });

    print("Get documents from postgres")
    # get the documents from the database
    statement = """
        WITH KEYWORDS AS (
            SELECT
                document_id,
                array_agg(DISTINCT keyword ORDER BY keyword) AS keywords
            FROM document_keywords
            GROUP BY document_id
        ),
        LANGUAGES AS (
            SELECT
                document_id,
                array_agg(DISTINCT language ORDER BY language) AS languages
            FROM document_languages
            GROUP BY document_id
        ),
        WIKIPEDIA AS (
            SELECT
                document_id,
                json_agg(json_build_object('name', wikipedia_term, 'cosine', cosine, 'pagerank', pagerank, 'lang', w.language, 'url', w.url)) AS wikipedia
            FROM wikipedia_annotations
            LEFT JOIN wikipedia_concepts w ON w.concept_name = wikipedia_annotations.wikipedia_term
            GROUP BY document_id
        ),
        INFORMEA AS (
            SELECT
                document_id,
                array_agg(DISTINCT o.term ORDER BY o.term) AS informea
            FROM ontology_annotations
            LEFT JOIN ontology_terms o ON o.term = ontology_annotations.term
            GROUP BY document_id
        ),
        NAMED_ENTITIES AS (
            SELECT
                document_id,
                json_agg(json_build_object('name', ne.entity_name, 'type', ne.type)) AS named_entities
            FROM ne_annotations
            LEFT JOIN named_entities ne ON ne.entity_name = ne_annotations.named_entity
            WHERE ne.type IN ('PERSON', 'LOCATION', 'ORGANIZATION', 'MISC')
            GROUP BY document_id
        ),
        AREAS AS (
            SELECT
                document_id,
                array_agg(DISTINCT area ORDER BY area) AS areas
            FROM document_areas
            GROUP BY document_id
        ),
        SUBJECTS AS (
            SELECT
                document_id,
                array_agg(DISTINCT subject ORDER BY subject) AS subjects
            FROM document_subjects
            GROUP BY document_id
        )

        SELECT
            d.document_id,
            d.title,
            d.abstract,
            d.fulltext_cleaned AS fulltext,
            d.fulltextlink AS link,
            d.name,
            d.date,

            d.basin,
            d.document_source AS source,
            d.status,
            d.celex_num AS celex,

            d.publisher,
            d.referencenumber AS reference_number,

            ne.named_entities AS named_entities,
            w.wikipedia AS wikipedia,
            i.informea AS informea,
            k.keywords AS keywords,
            l.languages AS languages,
            a.areas AS areas,
            s.subjects AS subjects
        FROM documents d
        LEFT JOIN KEYWORDS k ON k.document_id = d.document_id
        LEFT JOIN LANGUAGES l ON l.document_id = d.document_id
        LEFT JOIN WIKIPEDIA w ON w.document_id = d.document_id
        LEFT JOIN INFORMEA i ON i.document_id = d.document_id
        LEFT JOIN NAMED_ENTITIES ne ON ne.document_id = d.document_id
        LEFT JOIN AREAS a ON a.document_id = d.document_id
        LEFT JOIN SUBJECTS s ON s.document_id = d.document_id
        ORDER BY d.document_id;
    """

    documents = pg.execute(statement)
    print("Number of documents", len(documents))

    print("Populate elasticsearch index")
    count = 1
    # send the documents to the index
    for document in documents:
        if "date" in document and document["date"]:
            # format the date value
            print(document["date"])

            if "/" in document["date"]:
                date = document["date"].split("/")
                document["date"] = "{}-{}-{}".format(date[2], date[1], date[0])

            elif " " in document["date"]:
                date = document["date"].split(" ")
                day = '0{}'.format(date[1][:-1])[-2:]
                month = month_conversion[date[0]]
                document["date"] = "{}-{}-{}".format(date[2], month, day)

            elif len(document["date"]) == 4:
                document["date"] = "{}-01-01".format(document["date"])

        elif "date" in document and document["date"] == '':
            document["date"] = None


        # limit the number of entities
        document["named_entities"] = document["named_entities"][:5000] if document["named_entities"] else None
        document["wikipedia"] = document["wikipedia"][:5000] if document["wikipedia"] else None

        if "informea" in document and document["informea"]:
            document["informea"] = [informea.strip() for informea in document["informea"][:5000]]

        if "keywords" in document and document["keywords"]:
            keywords = []
            for keyword in document["keywords"][:5000]:
                keyword_split = keyword.split(", ")
                keywords = keywords + [k.strip() for k in keyword_split]
            document["keywords"] = keywords

        if "areas" in document and document["areas"]:
            areas = []
            for area in document["areas"][:5000]:
                area_split = area.split(", ")
                areas = areas + [a.strip() for a in area_split]
            document["areas"] = areas

        if "subjects" in document and document["subjects"]:
            subjects = []
            for subject in document["subjects"][:5000]:
                subject_split = subject.split(", ")
                subjects = subjects + [s.strip() for s in subject_split]
            document["subjects"] = subjects

        if "languages" in document and document["languages"]:
            langs = []
            for lang in document["languages"][:5000]:
                if len(lang) == 2:
                    language = pycountry.languages.get(alpha_2=lang)
                    langs.append(language.capitalize())
                else:
                    langs.append(lang.capitalize())

            document["languages"] = langs

        print(document["languages"])
        print(document["document_id"])

        try:
            es.index(index="envirolens", id=document["document_id"], body=document)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(str(e))
            pass

        if count % 1000 == 0:
            print("Number of indexed documents:", count)
        count = count + 1

    print("Refresh elasticsearch index")
    # refresh the index
    es.indices.refresh(index="envirolens")


    print("Done!")



