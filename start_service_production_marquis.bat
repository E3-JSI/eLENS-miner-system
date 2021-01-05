:: Script that will run all the components of our service
:: By changing args parameters you can set up the services at arbitrary hosts and ports but remember
:: to give the right hosts and ports in the main service command

:: Activating document retrieval microservice
start cmd /k "venv\Scripts\activate.bat && cd search && python -m search.main start -H localhost -p 4322 -e production"

:: Activating document similarity microservice
start cmd /k "venv\Scripts\activate.bat && cd document_similarity && python -m microservice.main start -H localhost -p 4323 -e production"

:: Activating text embedding microservice (for 1 language)
REM start cmd /k "venv\Scripts\activate.bat && cd text_embeddings && python -m text_embedding.main start -H localhost -p 4001 -e production -mp ./data/embeddings/wiki-news-300d-1M-en.vec -ml en"
start cmd /k "venv\Scripts\activate.bat && cd text_embeddings && python -m text_embedding.main start -H localhost -p 4324 -e production -mp ./data/embeddings/wiki-en-small-10k.vec -ml en"

:: Activating the main component
start cmd /k "venv\Scripts\activate.bat && cd entrypoint && python -m microservice.main start -H envirolens.ijs.si -p 4321 -teh localhost -tep 4324 -drh localhost -drp 4322 -dsh localhost -dsp 4323"
