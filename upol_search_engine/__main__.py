from time import sleep

from upol_search_engine.upol_crawler import tasks


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
    courseware.upol.cz"""

    crawler_settings = {'limit_domain': 'inf.upol.cz',
                        'max_depth': 10,
                        'connect_max_timeout': 3.05,
                        'read_max_timeout': 10,
                        'frequency_per_server': 0.5,
                        'blacklist': blacklist}

    seed = """https://www.upol.cz
    https://www.cmtf.upol.cz
    https://www.lf.upol.cz
    https://www.ff.upol.cz
    https://www.prf.upol.cz
    https://www.pdf.upol.cz
    https://ftk.upol.cz
    https://www.pf.upol.cz
    https://www.fzv.upol.cz"""

    print("Launching crawler")

    feeder = tasks.feeder_task.delay(
        crawler_settings=crawler_settings,
        seed=seed,
        batch_size=300,
        delay_between_feeding=30)

    while feeder.status != 'SUCCESS':
        print(feeder.status)
        print(feeder.info)
        sleep(10)

    print("Crawler done")
    print("Launching pagerank calculation")

    pagerank = tasks.calculate_pagerank_task.delay(crawler_settings)

    while pagerank.status != 'SUCCESS':
        print(pagerank.status)
        sleep(5)

    print("Pagerank done")

if __name__ == "__main__":
    main()
