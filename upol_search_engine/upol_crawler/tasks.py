from upol_search_engine import settings
from upol_search_engine.celery_app import app


@app.task(rate_limit=settings.CONFIG.get('Settings', 'crawl_task_frequency'),
          queue='crawler',
          ignore_result=True,
          task_compression='zlib')
def crawl_url_task(url, depth, crawler_settings):
    from upol_search_engine.upol_crawler.core import crawler

    crawler.crawl_url(url, depth, crawler_settings)


def feeder_task(crawler_settings, seed, batch_size,
                delay_between_feeding, task_id):
    from upol_search_engine.db import mongodb
    from upol_search_engine.utils import urls
    from upol_search_engine.upol_crawler.core import feeder
    from datetime import datetime

    start_time = datetime.now()

    # self.update_state(state='STARTING', meta={'start': start_time})

    client = mongodb.create_client()
    database = mongodb.get_database(crawler_settings.get('limit_domain'), client)
    regex = urls.generate_regex(crawler_settings.get('limit_domain'))

    mongodb.drop_database(
        urls.domain_replace_dots(crawler_settings.get('limit_domain')))

    # Init database
    mongodb.init(database)

    # if mongodb.is_first_run(database):
        # Load seed into database

    feeder.load_seed(
        seed, database, regex, crawler_settings.get('max_depth'),
        crawler_settings.get('blacklist'))
    # else:
    #     mongodb.reset_visited_for_fast_recrawl(database)

    # self.update_state(state='RUNNING', meta={'start': start_time})

    sleeping = False
    number_of_waiting = 0
    number_of_added_links = 0
    # stats = mongodb.get_crawler_stats(database)

    while True:
        if sleeping is False:
            # self.update_state(state='RUNNING_FEEDING',
                            #   meta={'start': start_time,
                            #         'added_links': number_of_added_links,
                            #         'stats': stats})

            number_of_added_links = feeder.feed_crawler(
                database, crawler_settings, batch_size)

            sleeping = True
        else:
            # self.update_state(state='RUNNING_SLEEPING',
            #                   meta={'start': start_time,
            #                         'added_links': number_of_added_links,
            #                         'stats': stats})

            # stats = mongodb.get_crawler_stats(database)
            mongodb.update_crawler_progress(database, task_id)

            number_of_added_links = 0

            number_of_waiting = feeder.sleep_crawler(database,
                                                     number_of_waiting,
                                                     delay_between_feeding)

            if number_of_waiting >= 2:
                break

            sleeping = False

    # self.update_state(state='DONE', meta={'start': start_time,
    #                                       'end': datetime.now()})
    mongodb.update_crawler_progress(database, task_id)
    client.close()


# @app.task(queue='search_engine_sub_tasks', bind=True)
def calculate_pagerank_task(crawler_settings):
    from upol_search_engine.db import mongodb
    from upol_search_engine.upol_crawler.core import pagerank

    from datetime import datetime

    start_time = datetime.now()

    # self.update_state(state='STARTING', meta={'start': start_time})

    client = mongodb.create_client()
    database = mongodb.get_database(crawler_settings.get('limit_domain'), client)

    # self.update_state(state='BUILDING_GRAPH', meta={'start': start_time})

    graph = pagerank.build_graph(database)

    # self.update_state(state='CALCULATING_PAGERANK', meta={'start': start_time})

    graph_pagerank = pagerank.calculate_pagerank(graph, database)

    # self.update_state(state='INSERTING_PAGERANK', meta={'start': start_time})

    pagerank.insert_pagerank_db(graph_pagerank, database)

    # self.update_state(state='DONE', meta={'start': start_time,
                                        #   'end': datetime.now()})

    client.close()
