from time import sleep

from upol_search_engine.upol_crawler import tasks


def main():
    crawler_settings = {'limit_domain': 'trnecka.inf.upol.cz',
                'max_depth': 10,
                'connect_max_timeout': 3.05,
                'read_max_timeout': 10,
                'frequency_per_server': 0.5,
                'blacklist': ""}

    seed = "http://trnecka.inf.upol.cz"

    print("Launching crawler")

    feeder = tasks.feeder_task.delay(
        crawler_settings=crawler_settings,
        seed=seed,
        batch_size=300,
        delay_between_feeding=5)

    while feeder.status != 'SUCCESS':
        print(feeder.status)
        sleep(5)

    print("Crawler done")
    print("Launching pagerank calculation")

    pagerank = tasks.calculate_pagerank_task.delay(crawler_settings)

    while pagerank.status != 'SUCCESS':
        print(pagerank.status)
        sleep(5)

    print("Pagerank done")

if __name__ == "__main__":
    main()
