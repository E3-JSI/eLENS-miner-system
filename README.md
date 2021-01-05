# eLENS Miner System

[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![Build Status](https://travis-ci.org/JozefStefanInstitute/eLENS-miner-system.svg?branch=master)](https://travis-ci.org/JozefStefanInstitute/eLENS-miner-system)
[![Python 3.6](https://img.shields.io/badge/python-3.6+-green.svg)](https://www.python.org/downloads/release/python-360/)
![Platform](https://img.shields.io/badge/platform-linux-green.svg)

The eLENS miner system retrieves, processes and analyzes legal documents and maps them to specific geographical areas.

The system follows the microservice architecture and is written in Python 3. It consists of the following microservices:

* [Document Retrieval.](document_retrieval) The service responsible for providing documents based on the user's query. It leverages [query expansion](https://en.wikipedia.org/wiki/Query_expansion) to improve the query results.

* [Document Similarity.](document_similarity) This service calculates the semantic similarity of the documents and can provide a list of most similar documents to a user selected one. Here, we integrate state-of-the-art methods using word and document embeddings to capture the semantic meaning of the documents and use it to compare the documents.

* [Text Embeddings.](text_embeddings) The service is a collection of text embedding methods. For a given text it generates the text embedding which is then used in the previous microservices.

* [Entrypoint.](entrypoint) This service is the interface and connects the previous microservices together. It is the entrypoint for the users to access the services.

## Prerequisites

You may want to create separate virtual environments for each of the microservices or you can create one for all of them. We advise to use virtual environments if you are developing multiple projects with Python, due to clashing of dependencies between projects. (Suppose one project only supports numpy < 1.0 and the other needs numpy=1.5).

To create a virtual environment navigate to the desired directory (usually the main folder of the project) and write
```bash
python -m venv venv
```
To activate this virtual environment navigate into venv/Scripts and then execute `activate`. To deactivate a virtual environment execute `deactivate`.

You can see that your virtual environment is being used if you see __(venv)__ before the command line.

Each microservice must be run separately. Each service can be used for themself or one can employ the `entrypoint` microservice that connects all of the microservices together.

What follows is a short description of how to run each microservice. A more detailed description of the microservice can be found in their designated folders.

### Text Embeddings Microservice

Currently you are able to run only one version of the text embedding so that it will be connected to the main component. But later you will be able to connect more.

* Activate virtual environment if you wish to do so
* Navigate into `text_embeddings` folder
* Execute
  ```bash
  pip install -r requirements.txt
  ```
* Run   
  ```bash
  python -m nltk.downloader all
  ```
* Place a copy of your `word2vec` or `fasttext` word embeddings in the [data/embeddings](text_embeddings/data/embeddings) folder
* Navigate back to the base of the `text_embeddings` folder and run the service with
  ```bash
  # linux or mac
  python -m text_embedding.main start \
         -e production \
         -H localhost \
         -p 4001 \
         -mp (path to the model) \
         -ml (language of the model)

  # windows
  python -m text_embedding.main start -e production -H localhost -p 4001 -mp (path to the model) -ml (language of the model)
  ```


### Document Retrieval Microservice

* Activate virtual environment if you wish to do so
* Navigate into `document_retrieval` folder
* Execute
  ```bash
  pip install -r requirements.txt
  ```
* Navigate into `microservice/config` folder
* Create `.env` file and inside define the following variables:
  ```bash
  PROD_PG_DATABASE=
  PROD_PG_USERNAME=
  PROD_PG_PASSWORD=
  PROD_TEXT_EMBEDDING_HOST=
  PROD_TEXT_EMBEDDING_PORT=

  DEV_PG_DATABASE=
  DEV_PG_USERNAME=
  DEV_PG_PASSWORD=
  DEV_TEXT_EMBEDDING_HOST=
  DEV_TEXT_EMBEDDING_PORT=
  ```
* Navigate to the base of `document_retrieval` folder and run the service with:
  ```bash
  # linux or mac
  python -m microservice.main start \
         -e production \
         -H localhost \
         -p 4100

  # windows
  python -m microservice.main start -e production -H localhost -p 4100
  ```

If you want you can also run the service on custom host and port.


### Document Similarity Microservice

* Activate virtual environment if you wish to do so
* Navigate into `document_similarity` folder
* Execute
  ```
  pip install -r requirements.txt
  ```
* Navigate into microservice/config folder
* Create a `.env` file with the following variables
  ```
  PROD_DATABASE_NAME =
  PROD_DATABASE_USER =
  PROD_DATABASE_PASSWORD =
  PROD_TEXT_EMBEDDING_URL =

  DEV_DATABASE_NAME =
  DEV_DATABASE_USER =
  DEV_DATABASE_PASSWORD =
  DEV_TEXT_EMBEDDING_URL =
  ```
* Set the text embedding url to `http://{HOST}:{PORT}/api/v1/embeddings/create` where HOST and PORT are the values used to run text embedding microservice
* Navigate back into the base of the `document_similarity` folder and run the service with
  ```bash
  # linux or mac
  python -m microservice.main start \
         -e production \
         -H localhost \
         -p 4200

  # windows
  python -m microservice.main start -e production -H localhost -p 4200
  ```

You can also use custom host and port.

### Entrypoint

* Activate virtual environment if you wish to do so
* Navigate into `entrypoint` folder
* Run
  ```
  pip install -r requirements.txt
  ```
* Navigate into `microservice/config` folder
* Create `.env` file with contents
  ```
  DEV_DATABASE_USER =
  DEV_DATABASE_HOST =
  DEV_DATABASE_PORT =
  DEV_DATABASE_PASSWORD =
  DEV_DATABASE_NAME =

  PROD_DATABASE_USER =
  PROD_DATABASE_HOST =
  PROD_DATABASE_PORT =
  PROD_DATABASE_PASSWORD =
  PROD_DATABASE_NAME =
  ```
* Navigame back into `entrypoint` folder
* Run the main service with
  ```bash
  # linux or mac
  python -m microservice.main start \
         -e production \
         -H localhost \
         -p 4500

  # windows
  python -m microservice.main start -e production -H localhost -p 4500
  ```
  However if you routed other microservices to different hosts/ports, you can provide this values in the following way:
  ```bash
  # linux or mac
  python -m microservice.main start -H localhost -p 4500 \
    -teh {host of the text embedding microservice} \
    -tep {port of the text embedding microservice} \
    -drh {host of the document retrieval microservice} \
    -drp {port of the document retrieval microservice} \
    -dsh {host of the document similarity microservice} \
    -dsp {port of the document similarity microservice}

  # windows
  python -m microservice.main start -H localhost -p 4500 -teh {host of the text embedding microservice} -tep {port of the text embedding microservice} -drh {host of the document retrieval microservice} -drp {port of the document retrieval microservice} -dsh {host of the document similarity microservice} -dsp {port of the document similarity microservice}
  ```

### Usage:

Available endpoints:
* **GET** `{HOST}/{PORT}/api/v1/documents/search` __query_params__ query, m
  * query -> your text query
  * m -> number of results

  #### Example request:
  ```{BASE_URL}/api/v1/documents/search?query=deforestation&m=10```
  You will receive top 10 documents similar to query "deforestation".
* **GET** `{HOST}/{PORT}/api/v1/documents/<document_id>/similar` __query_params__ get_k
  * document_id -> id of the document
  * get_k -> number of results
  #### Example request:
  ```{BASE_URL}/api/v1/documents/123/similar?get_k=5 ```
  You will receive 5 of the most similar documents to document with id 123.
* **POST** `{HOST}/{PORT}/api/v1/documents/<document_id>/similarity_update`
  * document_id -> id of the document
  #### Example request:
  ```{BASE_URL}/api/v1/documents/similarity_update```
  Recalculates similarities of the document with the given id to the other documents.
* **GET** `{HOST}/{PORT}/api/v1/embeddings/create` __query_params__ text, language
  * text -> your text
  * language -> language of the text
  #### Example request:
  ```{BASE_URL}/api/v1/embedding/create?text=ice cream&language=en```
  You will receive the embedding of the text "ice cream" from the english word embedding model.
* **GET** `{HOST}/{PORT}/api/v1/documents` __query_params__  document_ids
  * document_ids : (comma separated document ids)
  #### Example request:
  ```{BASE_URL}/api/v1/documents?document_ids=1,3,17```
  With the **GET** request at this endpoint you will receive documents data for documents ids 1, 3 and 17.
* **GET** `{HOST}/{PORT}/api/v1/documents/<document_id>`
  * document_id : (id of the document)
  #### Example request:
  ```{BASE_URL}/api/v1/documents/3```
  With the **GET** request at this endpoint you will receive documents data for document with id 3.


# Acknowledgments
This work is developed by [AILab](http://ailab.ijs.si/) at [Jozef Stefan Institute](https://www.ijs.si/).

The work is supported by the [EnviroLENS project](https://envirolens.eu/),
a project that demonstrates and promotes the use of Earth observation as direct evidence for environmental law enforcement,
including in a court of law and in related contractual negotiations.
