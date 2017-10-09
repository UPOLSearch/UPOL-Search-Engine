from datetime import datetime
from time import sleep

from upol_search_engine.upol_crawler import tasks as crawler_tasks
from upol_search_engine.upol_indexer import tasks as indexer_tasks


def main():
    blacklist = """portal.upol.cz
    stag.upol.cz
    library.upol.cz
    adfs.upol.cz
    portalbeta.upol.cz
    idp.upol.cz
    famaplus.upol.cz
    es.upol.cz
    smlouvy.upol.cz
    menza.upol.cz
    edis.upol.cz
    courseware.upol.cz
    m.zurnal.upol.cz"""

    # crawler_settings = {'limit_domain': 'upol.cz',
    #                     'max_depth': 10,
    #                     'connect_max_timeout': 3.05,
    #                     'read_max_timeout': 10,
    #                     'frequency_per_server': 0.5,
    #                     'blacklist': blacklist}

    crawler_settings = {'limit_domain': 'inf.upol.cz',
                        'max_depth': 10,
                        'connect_max_timeout': 3.05,
                        'read_max_timeout': 10,
                        'frequency_per_server': 0.2,
                        'blacklist': blacklist}

    indexer_settings = {'batch_size': 300,
                        'table_name': 'index'}

    # seed = """https://www.upol.cz
    # https://www.cmtf.upol.cz
    # https://www.lf.upol.cz
    # https://www.ff.upol.cz
    # https://www.prf.upol.cz
    # https://www.pdf.upol.cz
    # https://ftk.upol.cz
    # https://www.pf.upol.cz
    # https://www.fzv.upol.cz"""

    seed = """http://inf.upol.cz"""

    print("Launching crawler")

    feeder = crawler_tasks.feeder_task.delay(
        crawler_settings=crawler_settings,
        seed=seed,
        batch_size=300,
        delay_between_feeding=2)

    start_time = datetime.now()

    while feeder.status != 'SUCCESS':
        print(feeder.status)
        print(feeder.info)
        duration = datetime.now() - start_time
        print(duration)
        sleep(10)

    print("Crawler done")
    print("Launching pagerank calculation")

    pagerank = crawler_tasks.calculate_pagerank_task.delay(crawler_settings)

    while pagerank.status != 'SUCCESS':
        print(pagerank.status)
        sleep(5)

    print("Pagerank done")

    print("Launching indexer")

    indexer = indexer_tasks.indexer_task.delay(
        crawler_settings, indexer_settings)

    while indexer.status != 'SUCCESS':
        print(indexer.status)
        sleep(5)

    end_time = datetime.now()
    duration = end_time - start_time
    print(duration)
    print("Indexer done")


if __name__ == "__main__":
    main()
