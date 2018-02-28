import configparser
import logging
import time
import extract_tool as et
import task_agent as ta
import feature_cache as fc
import oss_mgr as om


logger = logging.getLogger()
localfile = 'tmp.jpg'

def do_extract(extractor, task, oss_manager):
    logger.info('extract good {} in task {}'.format(task.good_id, task.job_id))

    try:
        oss_manager.get_file(task.pic_url, localfile)
        task.status = 3
        task.pic_features = extractor.extract_bytes(localfile)

    except Exception as ex:
        logger.error('extract failed good {} in task {}. {}'.format(task.good_id, task.job_id, str(ex)))


def compare_result(task, feature_cache):
    result = feature_cache.compare(task)
    task.result_list = result

def do_search_tasks(extractor, task_agent, oss_manager, feature_cache):
    tasks = task_agent.fetch_search_tasks()
    while len(tasks) > 0:
        for task in tasks:
            do_extract(extractor, task, oss_manager)
            compare_result(task, feature_cache)

        task_agent.update_search_tasks(tasks)
        tasks = task_agent.fetch_search_tasks()
        if len(tasks) == 0:
            return True
    else:
        return False


def do_goods_tasks(extractor, task_agent, oss_manager):
    tasks = task_agent.fetch_goods_tasks()
    if len(tasks) > 0:
        for task in tasks:
            do_extract(extractor, task, oss_manager)

        task_agent.update_goods_tasks(tasks)


def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    now = time.time
    last = 0
    CHECK_INTERVAL = 1

    logger.info('program started.')

    extractor = et.FeatureExtractor()
    task_agent = ta.TaskAgent(config)
    feature_cache = fc.FeatureCache(config)
    oss_manager = om.OSSManager(config)
    feature_cache.load_features()

    while True:
        elapsed = now() - last
        if elapsed < CHECK_INTERVAL:
            time.sleep(CHECK_INTERVAL - elapsed)
            continue

        if not do_search_tasks(extractor, task_agent, oss_manager, feature_cache):
            # only do goods task when idle
            do_goods_tasks(extractor, task_agent, oss_manager)

        last = now()


if __name__ == '__main__':
    main()
