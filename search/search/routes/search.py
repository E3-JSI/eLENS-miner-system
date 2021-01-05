# Embedding Route
# Routes related to creatiung text embeddings

import sys
import json
import math
import requests


from flask import (
    Blueprint, flash, g, redirect, request, session, url_for, jsonify, current_app as app
)
from werkzeug.exceptions import abort

#################################################
# Static values
#################################################

BASE_URL = "http://envirolens.ijs.si/api/v1/documents/search"
MIN_LIMIT = 1
MAX_LIMIT = 100
DEFAULT_LIMIT = 20
MIN_PAGE = 1

#################################################
# Initialize the elasticsearch component
#################################################

# get the database configuration object
from ..config import config_es
from ..library.formatter import format_document, format_url

#################################################
# Setup the embeddings blueprint
#################################################

bp = Blueprint('search', __name__, url_prefix='/api/v1/search')


@bp.route('/', methods=['GET'])
def search():
    try:
        text = None
        sources = None
        locations = None
        languages = None
        informea = None
        limit = None
        page = None
        if request.method == 'GET':
            text = request.args.get('text', default=None, type=str)
            sources = request.args.get('sources', default='eurlex', type=str)
            locations = request.args.get('locations', default=None, type=str)
            languages = request.args.get('languages', default=None, type=str)
            informea = request.args.get('informea', default=None, type=str)
            limit = request.args.get('limit', default=20, type=int)
            page = request.args.get('page', default=1, type=int)
        else:
            # TODO: log exception
            return abort(405)

        #HOST = app.config.get('TEXT_EMBEDDING_HOST', 'localhost')
        #PORT = app.config.get('TEXT_EMBEDDING_PORT', '4222')

        # query_params = {
        #     'query': text
        # }

        #r = requests.get(f"http://{HOST}:{PORT}/api/v1/embeddings/expand", params=query_params)
        #r = json.loads(r.text)
        #tokens = r.get("expanded_query", [])

        # establish connection with elasticsearch
        es = config_es.get_es()

        #########################################
        # Prepare the must query params
        #########################################

        must_query = []

        if informea:
            es_informea = informea.split(",")
            must_query.append({
                "terms": { "informea": es_informea }
            })

        #########################################
        # Prepare the should query params
        #########################################

        should_query = []

        if text:
            should_query.append({
                "match": { "title": text }
            })
            should_query.append({
                "match": { "fulltext": text }
            })
            should_query.append({
                "match": { "abstract": text }
            })

        #########################################
        # Prepare the filter query
        #########################################
        filter_query = []

        # prepare the sources
        filter_query.append({
            "terms": { "source": sources.split(",") }
        })

        # prepare the locations
        if locations:
            es_locations = locations.split(",")
            filter_query.append({
                "nested": {
                    "path": "named_entities",
                    "query": {
                        "bool": {
                            "must": [{
                                "terms": { "named_entities.name": es_locations }
                            }, {
                                "term": { "named_entities.type": "LOCATION" }
                            }]
                        }
                    }
                }
            })

        if languages:
            es_languages = languages.split(",")
            filter_query.append({
                "terms": { "languages": es_languages }
            })

        #########################################
        # Prepare the pagination params
        #########################################

        if page < MIN_PAGE:
            page = MIN_PAGE

        if limit < MIN_LIMIT:
            limit = MIN_LIMIT
        elif limit > MAX_LIMIT:
            limit = MAX_LIMIT


        offset = (page - 1) * limit
        size = limit

        #########################################
        # Construct the query object
        #########################################

        es_query = {
            "from": offset,
            "size": size,
            "sort" : [
                { "date": { "order": "desc" } },
                "_score"
            ],
            "query": {
                "bool": {
                    "filter": filter_query
                }
            },
            "min_score": 2,
            "track_total_hits": True
        }

        if len(must_query) != 0:
            es_query["query"]["bool"]["must"] = must_query

        if len(should_query) != 0:
            es_query["query"]["bool"]["should"] = should_query;

        # run the query on elasticsearch
        results = es.search(index="envirolens", body=es_query)

        # prepare output of the elasticsearch response
        documents = [format_document(document) for document in results["hits"]["hits"]]

        # prepare metadata information for easier navigation
        TOTAL_HITS = results["hits"]["total"]["value"]
        TOTAL_PAGES = math.ceil(TOTAL_HITS / size)

        prev_page = format_url(BASE_URL, {
            "text": text,
            "sources": sources,
            "locations": locations,
            "languages": languages,
            "informea": informea,
            "limit": limit,
            "page": page - 1
        }) if page - 1 > 0 else None

        next_page = format_url(BASE_URL, {
            "text": text,
            "sources": sources,
            "locations": locations,
            "languages": languages,
            "informea": informea,
            "limit": limit,
            "page": page + 1
        }) if page + 1 <= TOTAL_PAGES else None

    except Exception as e:
        # TODO: log exception
        # something went wrong with the request
        return abort(400, str(e))
    else:
        # return the documents
        return jsonify({
            "query": {
                "text": text,
                "sources": sources,
                "locations": locations,
                "languages": languages,
                "informea": informea,
                "limit": limit,
                "page": page
            },
            "documents": documents,
            "metadata": {
                "total_hits": TOTAL_HITS,
                "total_pages": TOTAL_PAGES,
                "prev_page": prev_page,
                "next_page": next_page
            }
        })
