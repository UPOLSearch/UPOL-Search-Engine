from upol_search_engine.upol_crawler import tasks


def main():
    crawler_settings = {'limit_domain': 'inf.upol.cz',
                'max_depth': 10,
                'connect_max_timeout': 3.05,
                'read_max_timeout': 10,
                'frequency_per_server': 0.5,
                'blacklist': ""}

    seed = "http://inf.upol.cz"

    tasks.feeder_task.delay(
        crawler_settings=crawler_settings,
        seed=seed,
        batch_size=300,
        delay_between_feeding=5)


if __name__ == "__main__":
    main()
