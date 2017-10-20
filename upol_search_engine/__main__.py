import sys
from datetime import datetime
from time import sleep

from upol_search_engine import tasks
from upol_search_engine.db import postgresql


def setup_database(arg):
    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()

    if arg == 'functions':
        postgresql.create_function(
            postgresql_client, postgresql_cursor)
    elif arg == 'languages':
        postgresql.reset_and_init_languages(
            postgresql_client, postgresql_cursor)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()


def main():
    argv = sys.argv[1:]
    if len(argv) > 0:
        if argv[1] == 'setup':
            setup_database(argv[2])
            return

    search_engine = tasks.main_task.delay()

    start_time = datetime.now()

    while search_engine.status != 'SUCCESS':
        print(search_engine)
        duration = datetime.now() - start_time
        print(duration)
        sleep(10)


if __name__ == "__main__":
    main()
