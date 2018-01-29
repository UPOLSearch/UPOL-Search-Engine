from upol_search_engine import settings
from upol_search_engine.celery_app import app


@app.task(rate_limit=settings.CONFIG.get('Crawler', 'crawl_task_frequency'),
          queue='crawler',
          ignore_result=True,
          task_compression='zlib')
def crawl_url_task(url, depth, crawler_settings, ignore_blacklist=False):
    from upol_search_engine.upol_crawler.core import crawler

    crawler.crawl_url(url, depth, crawler_settings, ignore_blacklist)


def feeder_task(crawler_settings, seed, batch_size,
                delay_between_feeding, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.utils import urls
    from upol_search_engine.upol_crawler.core import feeder

    client = mongodb.create_client()
    database = mongodb.get_database(crawler_settings.get('limit_domain'), client)
    regex = urls.generate_regex(crawler_settings.get('limit_domain'))

    mongodb.drop_database(
        urls.domain_replace_dots(crawler_settings.get('limit_domain')))

    # Init database
    mongodb.init(database)

    feeder.load_seed(
        seed, database, regex, crawler_settings.get('max_depth'),
        crawler_settings.get('blacklist'))

    blacklist = crawler_settings.get('blacklist')

    for blacklisted_domain in blacklist:
        crawl_url_task.delay('http://' + blacklisted_domain,
                             crawler_settings.get('max_depth'),
                             crawler_settings,
                             ignore_blacklist=True)

    sleeping = False
    number_of_waiting = 0
    number_of_added_links = 0

    while True:
        if sleeping is False:

            feeder.feed_crawler(
                database, crawler_settings, batch_size)

            sleeping = True
        else:
            mongodb.update_crawler_progress(client, database, task_id)

            number_of_waiting = feeder.sleep_crawler(database,
                                                     number_of_waiting,
                                                     delay_between_feeding)

            if number_of_waiting >= 2:
                break

            sleeping = False

    mongodb.update_crawler_progress(client, database, task_id)
    client.close()


def calculate_pagerank_task(crawler_settings, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.upol_crawler.core import pagerank

    client = mongodb.create_client()
    database = mongodb.get_database(crawler_settings.get('limit_domain'), client)

    mongodb.update_pagerank_progress(client, task_id, 'building_graph')
    graph = pagerank.build_graph(database)

    mongodb.update_pagerank_progress(client, task_id, 'calculation')
    graph_pagerank = pagerank.calculate_pagerank(graph, database)

    mongodb.update_pagerank_progress(client, task_id, 'uploading')
    pagerank.insert_pagerank_db(graph_pagerank, database)

    client.close()
