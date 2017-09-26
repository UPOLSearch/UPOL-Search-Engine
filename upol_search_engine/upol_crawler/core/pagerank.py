import networkx as nx


def build_graph(database):
    graph = nx.DiGraph()

    graph_batch = database['PageRank'].find()

    for edge in graph_batch:
        from_hash = edge.get('from_hash')
        to_hash = edge.get('to_hash')

        from_document = database['Urls'].find_one({'_id': from_hash})
        to_document = database['Urls'].find_one({'_id': to_hash})

        if from_document is not None and to_document is not None:
            from_canonical_group = from_document.get('canonical_group')
            to_canonical_group = to_document.get('canonical_group')

            if (from_canonical_group is not None) and (to_canonical_group is not None):
                graph.add_edge(from_canonical_group, to_canonical_group)

    return graph


def calculate_pagerank(graph, database):
    pagerank = nx.pagerank(graph, alpha=0.9)

    return pagerank


def insert_pagerank_db(pagerank, database):
    for key, value in pagerank.items():
        representative = database['CanonicalGroups'].find_one(
            {'_id': key}).get('representative')

        database['Urls'].find_one_and_update(
            {'_id': representative}, {'$set': {'pagerank': value}})
