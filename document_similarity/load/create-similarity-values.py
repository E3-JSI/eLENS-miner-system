import numpy as np
from ..microservice.library.postgresql import PostgresQL
from ..microservice.config import config


def prepare_groups(documents, group_size=1000):
    """Splits the documents into groups of size group_size

    """
    n_docs = len(documents)
    groups = []
    for start in range(0, n_docs, group_size):
        # get the document set and prepare the embeddings and its ids
        doc_set = documents[start:start+group_size]
        embeddings = np.array([doc["vector"] for doc in doc_set])
        document_ids = [doc["document_id"] for doc in doc_set]
        groups.append({
            "embeddings": embeddings,
            "document_ids": document_ids
        })
    return groups


def calculate_similarities(embeddings_1, embeddings_2):
    """Calculates the similarities between the two embedding matrices

    Args:
        embeddings_1: The embeddings of the first set of documents.
        embeddings_2: The embeddings of the second set of documents.

    Returns:
        similarities: The similarities between the documents represented
            by the embeddings. The rows represent documents associated with
            the first argument, the columns represent documents associated
            with the second argument.
    """
    return np.matmul(embeddings_1, embeddings_2.T)


def store_the_similarities(similarities, row_doc_ids, col_doc_ids, pg, diagonal_flag):
    """Stores the similarities between the documents into the database

    """
    # get the number of documents
    n_rows, n_cols = similarities.shape
    # check if the lengths match
    assert n_rows == len(row_doc_ids), "Inconsistencies between the number of rows in similarities and row_docs"
    assert n_cols == len(col_doc_ids), "Inconsistencies between the number of columns in similarities and col_docs"

    # prepare statement for inserting documents into the table
    statement = """
        INSERT INTO similarities
        VALUES (%s,%s,%s);
    """

    for i in range(n_rows):
        doc_id_i = row_doc_ids[i]
        for j in range(n_cols):
            if diagonal_flag and j <=i:
                continue
            doc_id_j = col_doc_ids[j]
            similarity_ij = similarities[i][j]
            # prepare the parameters for updating the table
            params = (doc_id_i, doc_id_j, similarity_ij,)
            print(statement, params)
            # execute the
            print(pg.execute(statement, params))
            pg.commit()


if __name__=='__main__':
    database = config.ProductionConfig.DATABASE_NAME
    password = config.ProductionConfig.DATABASE_PASSWORD

    print("Establish connection")
    # prepare postgresql connection
    pg = PostgresQL()
    pg.connect(database, password)


    statement = "SELECT * FROM document_embeddings;"
    documents = pg.execute(statement)
    # get the groups
    groups = prepare_groups(documents)

    n_groups = len(groups)
    for i in range(n_groups):
        group_i = groups[i]
        for j in range(i, n_groups):
            group_j = groups[j]
            # calculate the similarities
            similarities_ij = calculate_similarities(
                group_i["embeddings"],
                group_j["embeddings"]
            )
            # store the similarities
            store_the_similarities(
                similarities_ij,
                group_i["document_ids"],
                group_j["document_ids"],
                pg,
                i == j # if groups are the same, store only the diagonal values
            )