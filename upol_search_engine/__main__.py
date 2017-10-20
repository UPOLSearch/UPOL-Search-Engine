import sys
from datetime import datetime
from time import sleep

from upol_search_engine import tasks
from upol_search_engine.db import postgresql


def setup_database():
    postgresql_client = postgresql.create_client()
    postgresql_cursor = postgresql_client.cursor()

    postgresql.create_function(postgresql_client, postgresql_cursor)

    postgresql_client.commit()
    postgresql_cursor.close()
    postgresql_client.close()


def main():
    argv = sys.argv[1:]
    if len(argv) > 0:
        if argv[0] == 'setup':
            setup_database()
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
