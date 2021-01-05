# Formatter functions
# The functions used to format different objects

from urllib.parse import urlparse, parse_qsl, urlunparse, urlencode

def format_document(document):
    """Formats the given document

    Args:
        document (obj): The json object containing the document metadata.

    Returns:
        obj: the formatted document object.

    """
    languages = [lang.strip().capitalize() for lang in document["_source"]["languages"]] if document["_source"]["languages"] else None
    return {
        "score": document["_score"],
        "document_id": document["_source"]["document_id"],
        "title": document["_source"]["title"],
        "abstract": document["_source"]["abstract"],
        "link": document["_source"]["link"],
        "date": document["_source"]["date"],
        "celex": document["_source"]["celex"],
        "keywords": document["_source"]["keywords"],
        "source": document["_source"]["source"],
        "informea": document["_source"]["informea"],
        "languages": languages,
        "subjects": document["_source"]["subjects"],
        "areas": document["_source"]["areas"]
    }


def format_url(url, params):
    # part the url
    url_parts = list(urlparse(url))
    # get the query parameters of the url
    query = dict(parse_qsl(url_parts[4]))
    # add the query parameters
    query.update(params)
    # encode the query parameters
    url_parts[4] = urlencode(query)
    # create the url
    return urlunparse(url_parts)