import mysql.connector
import logging

logger = logging.getLogger()


class Task(object):
    def __init__(self):
        self.id = 0
        self.status = 0
        self.pic_url = None
        self.product_type = None
        self.goods_id = -1
        self.job_id = -1

class GoodsTask(Task):
    def __init__(self, job_id, good_id, company_id, status, pic_url, product_type):
        Task.__init__(self)

        self.id = job_id
        self.job_id = job_id
        self.goods_id = good_id
        self.company_id = company_id
        self.status = status
        self.product_type = product_type
        self.pic_url = pic_url
        self.pic_features = None


class SearchTask(Task):
    def __init__(self, search_id, status, pic_url, result_cnt, product_type):
        Task.__init__(self)

        self.id = search_id
        self.search_id = search_id
        self.status = status
        self.pic_url = pic_url
        self.pic_features = None
        self.product_type = product_type
        self.result_cnt = result_cnt
        self.job_id = 0 - self.search_id
        # result
        self.result_list = []


class SearchResult(object):
    def __init__(self, result_pic_url, similarity_degree, goods_id, company_id):
        self.result_pic_url = result_pic_url
        self.similarity_degree = similarity_degree
        self.goods_id = goods_id
        self.company_id = company_id


class TaskAgent(object):
    def __init__(self, conf):
        self.mysql_host = conf['mysql']['host']
        self.mysql_port = int(conf['mysql']['port'])
        self.mysql_user = conf['mysql']['user']
        self.mysql_pw = conf['mysql']['pw']
        self.mysql_db = conf['mysql']['db']

    def fetch_goods_tasks(self) -> list:
        pending_tasks = list()
        logger.info('fetching pending goods tasks...')

        try:
            cnx = mysql.connector.connect(user=self.mysql_user, password=self.mysql_pw,
                                          host=self.mysql_host,
                                          database=self.mysql_db, port=self.mysql_port, connection_timeout=10,
                                          connect_timeout=10)
            cursor = cnx.cursor()

            qry = 'SELECT j.id, j.good_id, i.company_id, j.status, j.pic_uri, i.product_type FROM ' \
                  'm_good_pic_compute_job j ' \
                  'LEFT JOIN m_good_info i ON j.good_id = i.good_id ' \
                  'WHERE j.status = 1 ORDER BY j.insert_time '
            cursor.execute(qry)

            for i, (job_id, good_id, company_id, status, pic_url, product_type) in enumerate(cursor):
                if i == 5:  # fetch max 5 tasks a time
                    break

                pending_tasks.append(GoodsTask(job_id, good_id, company_id, status, pic_url, product_type))

            cursor.close()
            cnx.close()

            logger.info('{} pending goods tasks were found.'.format(len(pending_tasks)))

        except Exception as ex:
            logger.error('fetching goods tasks error - {}'.format(str(ex)))

        return pending_tasks

    def update_goods_tasks(self, task_list: list):
        try:
            cnx = mysql.connector.connect(user=self.mysql_user, password=self.mysql_pw,
                                          host=self.mysql_host,
                                          database=self.mysql_db, port=self.mysql_port, connection_timeout=10,
                                          connect_timeout=10)
            cursor = cnx.cursor()

            update_good = 'UPDATE m_good_info SET pic_digits = %s WHERE good_id = %s'
            update_job = 'UPDATE m_good_pic_compute_job SET status = %s WHERE id = %s'

            cnt_done = 0
            for task in task_list:
                if task.pic_features is not None:
                    cursor.execute(update_good, (task.pic_features, task.goods_id))

                cursor.execute(update_job, (task.status, task.job_id))
                logger.debug('update goods task id:{}  goods_id:{}'.format(task.job_id, task.goods_id))

                cnt_done += 1

            cnx.commit()

            cursor.close()
            cnx.close()

            logger.info('{} goods tasks were updated.'.format(cnt_done))

            return True
        except Exception as ex:
            logger.error('update goods tasks error - {}'.format(str(ex)))

        return False

    def fetch_search_tasks(self) -> list:
        pending_tasks = list()
        logger.info('fetching pending search tasks...')
        result_cnt = 20

        try:
            cnx = mysql.connector.connect(user=self.mysql_user, password=self.mysql_pw,
                                          host=self.mysql_host,
                                          database=self.mysql_db, port=self.mysql_port, connection_timeout=10,
                                          connect_timeout=10)
            cursor = cnx.cursor()

            qry = "SELECT value FROM conf_fextractor_search_settings WHERE `key` = 'search_cnt'"
            cursor.execute(qry)
            result_cnt = int(cursor.fetchone()[0])
            cursor.close()

            cursor = cnx.cursor()

            qry = 'SELECT id, status, search_pic_uri, product_type FROM action_user_search_pic ' \
                  'WHERE status = 1 ORDER BY action_time'
            cursor.execute(qry)

            for i, (search_id, status, pic_url, product_type) in enumerate(cursor):
                if i == 5:  # fetch max 5 tasks a time
                    break
                pending_tasks.append(SearchTask(search_id, status, pic_url, result_cnt, product_type))

            cursor.close()
            cnx.close()

            logger.info('{} pending search tasks were found.'.format(len(pending_tasks)))

        except Exception as ex:
            logger.error('fetching search tasks error - {}'.format(str(ex)))

        return pending_tasks

    def update_search_tasks(self, task_list: list):
        try:
            cnx = mysql.connector.connect(user=self.mysql_user, password=self.mysql_pw,
                                          host=self.mysql_host,
                                          database=self.mysql_db, port=self.mysql_port, connection_timeout=10,
                                          connect_timeout=10)
            cursor = cnx.cursor()

            update_result = 'INSERT action_user_search_pic_result(search_id, result_pic_uri, ' \
                            'similarity_degree, goods_id, company_id) VALUES(%s, %s, %s, %s, %s)'
            update_job = 'UPDATE action_user_search_pic SET status = %s, result_cnt = %s WHERE id = %s'

            cnt_done = 0
            for task in task_list:
                for result in task.result_list:
                    cursor.execute(update_result, (task.search_id, result.result_pic_url, result.similarity_degree,
                                                   result.goods_id, result.company_id))

                logger.debug('update search task {}'.format(task.search_id))
                cursor.execute(update_job, (task.status, task.result_cnt, task.search_id))
                cnt_done += 1

            cnx.commit()

            cursor.close()
            cnx.close()

            logger.info('{} search tasks were updated.'.format(cnt_done))

        except Exception as ex:
            logger.error('update search tasks error - {}'.format(str(ex)))
