import asyncio
from unittest import TestCase
from unittest.mock import MagicMock

import mongoengine


def async_test(coroutine):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coroutine(*args, **kwargs))
    return wrapper


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class MockedMongoTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(MockedMongoTestCase, self).__init__(*args, **kwargs)
        self.db_name = 'MockDB'

    def setUp(self):
        self.conn = mongoengine.connect(db=self.db_name, host='mongomock://localhost/' + self.db_name)

    def tearDown(self):
        self.conn.drop_database(self.db_name)
