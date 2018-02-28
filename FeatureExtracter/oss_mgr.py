import oss2


class OSSManager(object):
    def __init__(self, conf):
        self.access_key = conf['oss']['accesskey']
        self.access_secret = conf['oss']['accesssecret']

    def get_file(self, url, local):
        auth = oss2.Auth(self.access_key, self.access_secret)
        info = oss2.urlparse(url)
        bucket = oss2.Bucket(auth, '您的Endpoint', '您的Bucket名')
        bucket.get_object_to_file('remote.txt', local)

