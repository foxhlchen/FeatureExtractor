import configparser
import logging
import time
import extract_tool as et
import task_agent as ta

logger = logging.getLogger()


def do_extract(extractor, task):
    logger.info('extract good {} in task {}'.format(task.good_id, task.job_id))

    # TODO fetching img from aliyun OSO
    try:
        #task.status = 3
        pass

    except Exception as ex:
        logger.error('extract failed good {} in task {}. {}'.format(task.good_id, task.job_id, str(ex)))


def compare_result(task):
    pass


def do_search_tasks(extractor, task_agent):
    tasks = task_agent.fetch_search_tasks()
    while len(tasks) > 0:
        for task in tasks:
            do_extract(extractor, task)
            compare_result()

        task_agent.update_search_tasks(tasks)
        tasks = task_agent.fetch_search_tasks()
        if len(tasks) == 0:
            return True
    else:
        return False


def do_goods_tasks(extractor, task_agent):
    tasks = task_agent.fetch_goods_tasks()
    if len(tasks) > 0:
        for task in tasks:
            do_extract(extractor, task)

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

    while True:
        elapsed = now() - last
        if elapsed < CHECK_INTERVAL:
            time.sleep(CHECK_INTERVAL - elapsed)
            continue

        if not do_search_tasks(extractor, task_agent):
            # only do goods task when idle
            do_goods_tasks(extractor, task_agent)

        last = now()


if __name__ == '__main__':
    main()
