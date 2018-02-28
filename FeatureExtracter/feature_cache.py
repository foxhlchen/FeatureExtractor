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

            qry = 'SELECT good_id, company_id, pic_url, pic_digits FROM m_good_info ' \
                  ' ORDER BY insert_time'
            cursor.execute(qry)

            for i, (good_id, company_id, pic_url, pic_bytes) in enumerate(cursor):
                feature_np = np.array(et.FeatureExtractor.unpack(pic_bytes))
                self.feature_entries.append(FeatureEntry(good_id, company_id, pic_url, feature_np))
                self.feature_arrays.append(np.array(et.FeatureExtractor.unpack(feature_np)))

            cursor.close()
            cnx.close()

            logger.info('{} goods features loaded.'.format(len(self.feature_entries)))

        except Exception as ex:
            logger.error('load goods feature error - {}'.format(str(ex)))

    def compare(self, task) -> list:
        #np.linalg.norm(result['35.jpg'] - result['30-1.jpg'])

        X = [np.array(et.FeatureExtractor.unpack(task.pic_features))] + self.feature_arrays
        nbrs = NearestNeighbors(n_neighbors=task.result_cnt, algorithm='auto').fit(X)
        distances, indices = nbrs.kneighbors()
        result_list = []
        for i in indices[0]:
            entry = self.feature_entries[i]
            rs = tg.SearchResult(entry.pic_url, distances[0][i], entry.goods_id, entry.company_id)
            result_list.append(rs)

        return result_list

