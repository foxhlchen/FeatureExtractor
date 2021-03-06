import oss2
import logging

logger = logging.getLogger()


class OSSManager(object):
    def __init__(self, conf):
        logger.info('OSS Manager Initializing...')

        self.access_key = conf['oss']['accesskey']
        self.access_secret = conf['oss']['accesssecret']
        self.endpoint = conf['oss']['endpoint']

    def get_file_url(self, url, local):
        logger.info('fetching file {} from oss to {}'.format(url, local))

        auth = oss2.Auth(self.access_key, self.access_secret)
        info = oss2.urlparse(url)
        sep_loc = info.netloc.find('.')
        bucket_name = info.netloc[:sep_loc]
        endpoint_name = info.netloc[sep_loc + 1:]
        path = info.path[1:]
        bucket = oss2.Bucket(auth, endpoint_name, bucket_name)
        bucket.get_object_to_file(path, local)

    def get_file(self, url, local):
        logger.info('fetching file {} from oss to {}'.format(url, local))

        auth = oss2.Auth(self.access_key, self.access_secret)
        url = url.replace('oss://', '')
        sep_loc = url.find('/')
        bucket_name = url[:sep_loc]
        endpoint_name = self.endpoint
        path = url[sep_loc + 1:]
        bucket = oss2.Bucket(auth, endpoint_name, bucket_name)
        bucket.get_object_to_file(path, local)


if __name__ == '__main__':
    import configparser

    config = configparser.ConfigParser()
    config.read('config.ini')
    ossmgr = OSSManager(config)
    #ossmgr.get_file_url('http://sxh-search-test.oss-cn-hangzhou.aliyuncs.com/34.jpg', 'tmp.jpg')
    ossmgr.get_file('sxh-search-test/34.jpg', 'tmp1.jpg')

