import mysql.connector
import logging
import extract_tool as et
import numpy as np
import task_agent as tg
from sklearn.neighbors import NearestNeighbors

logger = logging.getLogger()


class FeatureEntry(object):

    def __init__(self, goods_id, company_id, pic_url, feature_np):
        self.goods_id = goods_id
        self.company_id = company_id
        self.pic_url = pic_url
        self.features_np = feature_np


class FeatureCache(object):
    def __init__(self, conf):
        logger.info('FeatureCache Initializing...')

        self.mysql_host = conf['mysql']['host']
        self.mysql_user = conf['mysql']['user']
        self.mysql_pw = conf['mysql']['pw']
        self.mysql_db = conf['mysql']['db']
        self.feature_entries = []
        self.feature_arrays = []

    def load_features(self):
        logger.info('loading goods features...')

        self.feature_entries.clear()

        try:
            cnx = mysql.connector.connect(user=self.mysql_user, password=self.mysql_pw,
                                          host=self.mysql_host,
                                          database=self.mysql_db)
            cursor = cnx.cursor()

            qry = 'SELECT good_id, company_id, pic_uri, pic_digits FROM m_good_info ' \
                  ' ORDER BY good_id'
            cursor.execute(qry)

            for i, (good_id, company_id, pic_url, pic_bytes) in enumerate(cursor):
                if pic_bytes is None:
                    continue

                try:
                    feature_np = np.array(et.FeatureExtractor.unpack(pic_bytes))
                    self.feature_entries.append(FeatureEntry(good_id, company_id, pic_url, feature_np))
                    self.feature_arrays.append(feature_np)
                except Exception as ex:
                    logger.error('load goods_id {} feature error - {}'.format(good_id, str(ex)))

            cursor.close()
            cnx.close()

            logger.info('{} goods features loaded.'.format(len(self.feature_entries)))

        except Exception as ex:
            logger.error('load goods feature error - {}'.format(str(ex)))

    def insert(self, task):
        if task.pic_features is None:
            return False
        if task.status != 3:
            return False
        if task.company_id is None:
            return False

        logger.info('Insert feature cache task {}, pic_url {}'.format(task.id, task.pic_url))

        try:
            feature_np = np.array(et.FeatureExtractor.unpack(task.pic_features))
            self.feature_entries.append(FeatureEntry(task.goods_id, task.company_id, task.pic_url, feature_np))
            self.feature_arrays.append(feature_np)

            return True
        except Exception as ex:
            logger.error('Insert feature cache {} error - {}'.format(task.id, str(ex)))

        return False

    def insert_batch(self, tasklist: list):
        logger.info('Insert feature cache batch... count {}, before cache count {}'.format(len(tasklist),
                                                                                           len(self.feature_entries)))

        for task in tasklist:
            self.insert(task)

        logger.info('Insert feature cache batch done. cache count now {}'.format(len(self.feature_entries)))

    def compare(self, task) -> list:
        # np.linalg.norm(result['35.jpg'] - result['30-1.jpg'])

        logger.info('Task file {} do comparing'.format(task.pic_url))
        result_list = []

        try:
            X = [np.array(et.FeatureExtractor.unpack(task.pic_features))] + self.feature_arrays
            nbrs = NearestNeighbors(n_neighbors=task.result_cnt, algorithm='auto').fit(X)
            distances, indices = nbrs.kneighbors()
            for idx, i in enumerate(indices[0]):
                i -= 1  # remove source pic
                entry = self.feature_entries[i]
                rs = tg.SearchResult(entry.pic_url, int(distances[0][idx]), entry.goods_id, entry.company_id)
                result_list.append(rs)
        except Exception as ex:
            result_list = []
            task.status = -111
            logger.error('Task file {} comparing failed - {}'.format(task.pic_url, str(ex)))

        return result_list
