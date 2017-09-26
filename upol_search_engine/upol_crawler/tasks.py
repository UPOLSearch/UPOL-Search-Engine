from upol_search_engine import settings
from upol_search_engine.celery_app import app
from upol_search_engine.upol_crawler.tools import logger


# log_collector = logger.universal_logger('collector')
# log_crawler = logger.universal_logger('crawl_url')


@app.task(rate_limit=settings.CONFIG.get('Settings', 'crawl_task_frequency'),
          queue='crawler',
          ignore_result=True,
          task_compression='zlib')
def crawl_url_task(url, depth, crawler_settings):
    # try:
    from upol_search_engine.upol_crawler.core import crawler

    crawler.crawl_url(url, depth, crawler_settings)
    # except Exception as e:
    #     log_crawler.exception('Exception: {0}'.format(url))
    #     raise


@app.task(queue='feeder', bind=True)
def feeder_task(self, crawler_settings, seed, batch_size, delay_between_feeding):
    from upol_search_engine.upol_crawler import db
    from upol_search_engine.upol_crawler.utils import urls
    from upol_search_engine.upol_crawler.core import feeder
    from datetime import datetime

    start_time = datetime.now()

    self.update_state(state='STARTING', meta={'start': start_time})

    client = db.create_client()
    database = db.get_database(crawler_settings.get('limit_domain'), client)
    regex = urls.generate_regex(crawler_settings.get('limit_domain'))

    # Init database
    db.init(database)

    # Load seed into database
    number_of_url_added = feeder.load_seed(seed,
                                           database,
                                           regex,
                                           crawler_settings.get('max_depth'),
                                           crawler_settings.get('blacklist'))

    self.update_state(state='RUNNING', meta={'start': start_time})

    sleeping = False
    number_of_waiting = 0
    number_of_added_links = 0
    stats = db.get_crawler_stats(database)

    while True:
        if sleeping is False:
            self.update_state(state='RUNNING_FEEDING',
                              meta={'start': start_time,
                                    'added_links': number_of_added_links,
                                    'stats': stats})

            number_of_added_links = feeder.feed_crawler(database, crawler_settings, batch_size)

            sleeping = True
        else:
            self.update_state(state='RUNNING_SLEEPING',
                              meta={'start': start_time,
                                    'added_links': number_of_added_links,
                                    'stats': stats})

            stats = db.get_crawler_stats(database)

            number_of_added_links = 0

            number_of_waiting = feeder.sleep_crawler(database,
                                                     number_of_waiting,
                                                     delay_between_feeding)

            if number_of_waiting >= 2:
                break

            sleeping = False

    self.update_state(state='DONE', meta={'start': start_time,
                                          'end': datetime.now()})
