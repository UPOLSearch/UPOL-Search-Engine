from celery.exceptions import SoftTimeLimitExceeded
from upol_search_engine.celery_app import app
from upol_search_engine.db import mongodb
from upol_search_engine.upol_crawler import tasks as crawler_tasks
from upol_search_engine.upol_indexer import tasks as indexer_tasks


# Time limit, soft 1.8day, hard 2days
@app.task(queue='search_engine',
          bind=True,
          time_limit=172800,
          soft_time_limit=164160)
def main_task(self):
    """Main task of the project"""

    try:
        # Later load these settings from DB

        blacklist = """
        portal.upol.cz
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
        m.zurnal.upol.cz
        """

        seed = """http://trnecka.inf.upol.cz"""

        crawler_settings = {'limit_domain': 'trnecka.inf.upol.cz',
                            'max_depth': 10,
                            'connect_max_timeout': 3.05,
                            'read_max_timeout': 10,
                            'frequency_per_server': 0.2,
                            'blacklist': blacklist}

        indexer_settings = {'batch_size': 300,
                            'table_name': 'index_tmp',
                            'table_name_production': 'index'}

        mongodb_client = mongodb.create_client()

        task_id = self.request.id

        mongodb.insert_engine_start(mongodb_client, task_id, crawler_settings)

        mongodb.insert_sub_task_start(mongodb_client, task_id, "crawler")

        crawler_tasks.feeder_task(
            crawler_settings=crawler_settings,
            seed=seed,
            batch_size=300,
            delay_between_feeding=2,
            task_id=task_id)

        mongodb.insert_sub_task_finish(
            mongodb_client, task_id, "crawler", "finished")

        mongodb.insert_sub_task_start(mongodb_client, task_id, "pagerank")

        crawler_tasks.calculate_pagerank_task(crawler_settings, task_id)

        mongodb.insert_sub_task_finish(
            mongodb_client, task_id, "pagerank", "finished")

        mongodb.insert_sub_task_start(mongodb_client, task_id, "indexer")

        indexer_tasks.indexer_task(crawler_settings, indexer_settings, task_id)

        mongodb.insert_sub_task_finish(
            mongodb_client, task_id, "indexer", "finished")

        mongodb.insert_engine_finish(mongodb_client, task_id, "finished")

        mongodb_client.close()
    except SoftTimeLimitExceeded:
        mongodb.insert_engine_finish(mongodb_client, task_id, "killed")

        mongodb_client.close()
