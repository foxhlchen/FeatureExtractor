import redis as r
from redis import RedisError


class RedisManager(object):
    def __init__(self, conf):
        self.redis_host = conf['redis']['host']
        self.redis_port = conf['redis']['port']
        self.redis_db = conf['redis']['db']
        self.redis_auth = conf['redis']['auth'] if "auth" in conf['redis'] else None
        self.pool = None
        self.r = None
        self.p = None

    def connect(self):
        self.pool = r.ConnectionPool(host=self.redis_host, port=int(self.redis_port), db=int(self.redis_db),
                                     password=self.redis_auth)
        self.r = r.Redis(connection_pool=self.pool)
        self.p = self.r.pubsub()

    def psub(self, channel):
        self.p.psubscribe(channel)

    def unsub(self):
        self.p.unsubscribe()

    def listen(self):
        return self.p.listen()

    def get_message(self):
        return self.p.get_message()

