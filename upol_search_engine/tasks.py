from celery.exceptions import SoftTimeLimitExceeded
from upol_search_engine import settings
from upol_search_engine.celery_app import app
from upol_search_engine.db import mongodb
from upol_search_engine.upol_crawler import tasks as crawler_tasks
from upol_search_engine.upol_indexer import tasks as indexer_tasks
from upol_search_engine.utils import urls


@app.task(queue='search_engine',
          bind=True)
def main_task(self):
    """Main task of the project"""

    try:
        # Later load these settings from DB

        # blacklist = """
        # portal.upol.cz
        # stag.upol.cz
        # library.upol.cz
        # adfs.upol.cz
        # portalbeta.upol.cz
        # idp.upol.cz
        # famaplus.upol.cz
        # es.upol.cz
        # smlouvy.upol.cz
        # menza.upol.cz
        # edis.upol.cz
        # courseware.upol.cz
        # m.zurnal.upol.cz
        # stagservices.upol.cz
        # """
        #
        #
        # seed = "https://www.upol.cz \n https://www.cmtf.upol.cz \n https://www.lf.upol.cz \n https://www.ff.upol.cz \n https://www.prf.upol.cz \n https://www.pdf.upol.cz \n https://ftk.upol.cz \n https://www.pf.upol.cz \n https://www.fzv.upol.cz \n http://upcrowd.upol.cz \n http://vychodil.inf.upol.cz/kmi/pp1/ \n http://vychodil.inf.upol.cz/"

        blacklist = urls.load_urls_from_file(settings.blacklist_path)
        seed = urls.load_urls_from_file(settings.seed_path)

        crawler_settings = {'limit_domain': settings.CONFIG.get('Crawler', 'limit_domain'),
                            'max_depth': settings.CONFIG.getint('Crawler', 'max_depth'),
                            'connect_max_timeout': settings.CONFIG.getfloat('Crawler', 'connect_max_timeout'),
                            'read_max_timeout': settings.CONFIG.getint('Crawler', 'read_max_timeout'),
                            'frequency_per_server': settings.CONFIG.getfloat('Crawler', 'frequency_per_server'),
                            'blacklist': blacklist}

        indexer_settings = {'batch_size': settings.CONFIG.getint('Indexer', 'batch_size'),
                            'table_name':  settings.CONFIG.get('General', 'postgresql_table_name_tmp'),
                            'table_name_production': settings.CONFIG.get('General', 'postgresql_table_name'),
                            'metadata_table_name':  settings.CONFIG.get('General', 'postgresql_metadata_table_name_tmp'),
                            'metadata_table_name_production': settings.CONFIG.get('General', 'postgresql_metadata_table_name'),}

        mongodb_client = mongodb.create_client()

        task_id = self.request.id

        mongodb.insert_engine_start(mongodb_client, task_id, crawler_settings)

        mongodb.insert_sub_task_start(mongodb_client, task_id, "crawler")

        crawler_tasks.feeder_task(
            crawler_settings=crawler_settings,
            seed=seed,
            batch_size=settings.CONFIG.getint('Crawler', 'batch_size'),
            delay_between_feeding=settings.CONFIG.getint('Crawler', 'delay_between_feeding'),
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
