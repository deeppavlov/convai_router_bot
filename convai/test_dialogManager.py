from datetime import datetime
from unittest.mock import Mock

from apscheduler.schedulers.base import BaseScheduler

from convai.conversation_gateways import AbstractGateway
from convai.dialog_manager import DialogManager
from model import util, User, UserPK, PersonProfile
from model.test_common import MockedMongoTestCase, async_test, AsyncMock


class DummyScheduler(BaseScheduler):
    def __init__(self, *args, **kwargs):
        super(DummyScheduler, self).__init__(*args, **kwargs)
        self.add_job = Mock()

    def shutdown(self, wait=True):
        super(DummyScheduler, self).shutdown(wait)

    def wakeup(self):
        pass


class TestDialogManager(MockedMongoTestCase):
    def setUp(self):
        super(TestDialogManager, self).setUp()

        self.bots_gateway = self.create_gateway()
        self.humans_gateway = self.create_gateway()
        # noinspection PyTypeChecker
        self.dm = DialogManager(max_time_in_lobby=120,
                                human_bot_ratio=0.5,
                                inactivity_timeout=120,
                                length_threshold=100,
                                bots_gateway=self.bots_gateway,
                                humans_gateway=self.humans_gateway,
                                dialog_eval_min=1,
                                dialog_eval_max=5,
                                scheduler=DummyScheduler())

        self.bots_gateway.send_message = AsyncMock()
        self.humans_gateway.send_message = AsyncMock()

        util.fill_db_with_stub()

    @staticmethod
    def create_gateway() -> Mock:
        gw = Mock(AbstractGateway)
        gw.send_message = AsyncMock()
        gw.start_conversation = AsyncMock()
        gw.start_evaluation = AsyncMock()
        gw.finish_conversation = AsyncMock()
        gw.on_conversation_failed = AsyncMock()

        return gw

    @async_test
    async def test_on_human_initiated_dialog_exceptions(self):
        banned_user = User.objects(banned=True)[0]
        normal_user = User.objects(banned=False)[0]

        self.dm.human_bot_ratio = 0

        with self.assertRaises(ValueError):
            await self.dm.on_human_initiated_dialog(banned_user)

        await self.dm.on_human_initiated_dialog(normal_user)

        with self.assertRaises(ValueError):
            await self.dm.on_human_initiated_dialog(normal_user)

        the_same_user = User.objects.get(user_key=normal_user.user_key)
        with self.assertRaises(ValueError):
            await self.dm.on_human_initiated_dialog(the_same_user)

        one_more_user = User.objects.get(user_key=UserPK(platform=normal_user.user_key.platform,
                                                         user_id=normal_user.user_key.user_id))
        with self.assertRaises(ValueError):
            await self.dm.on_human_initiated_dialog(one_more_user)

    @async_test
    async def test_on_human_initiated_dialog(self):
        user, user2, user3 = User.objects(banned=False)[:3]

        self.dm.human_bot_ratio = 0
        await self.dm.on_human_initiated_dialog(user)

        self.bots_gateway.start_conversation.assert_called_once()
        self.humans_gateway.start_conversation.assert_called_once()

        self.assertEqual(self.humans_gateway.start_conversation.call_args[0][1], user)

        self.assertEqual(self.humans_gateway.start_conversation.call_args[0][0],
                         self.bots_gateway.start_conversation.call_args[0][0])

        self.bots_gateway.start_conversation.reset_mock()
        self.humans_gateway.start_conversation.reset_mock()

        self.dm.human_bot_ratio = 1
        await self.dm.on_human_initiated_dialog(user2)
        await self.dm.on_human_initiated_dialog(user3)

        self.assertEqual(self.humans_gateway.start_conversation.call_count, 2)
        self.assertIn(user2, map(lambda call: call[0][1], self.humans_gateway.start_conversation.call_args_list))
        self.assertIn(user3, map(lambda call: call[0][1], self.humans_gateway.start_conversation.call_args_list))

    @async_test
    async def test_on_message_received_evaluated(self):
        user, user2 = User.objects(banned=False)[:2]

        self.dm.human_bot_ratio = 0
        await self.dm.on_human_initiated_dialog(user)

        conv_id = self.humans_gateway.start_conversation.call_args[0][0]
        bot_peer = self.bots_gateway.start_conversation.call_args[0][1]

        msg_id1 = await self.dm.on_message_received(conv_id, user, "Human message", datetime.now())
        self.bots_gateway.send_message.assert_called_once_with(conv_id, msg_id1, "Human message", bot_peer)

        msg_id2 = await self.dm.on_message_received(conv_id, bot_peer, "Bot message", datetime.now())
        self.humans_gateway.send_message.assert_called_once_with(conv_id, msg_id2, "Bot message", user)

        with self.assertRaises(ValueError):
            await self.dm.on_message_received(conv_id*2, user, "err", datetime.now())

        with self.assertRaises(ValueError):
            await self.dm.on_message_received(conv_id, user2, "err", datetime.now())

        await self.dm.on_message_evaluated(conv_id, user, 0, msg_id2)
        await self.dm.on_message_evaluated(conv_id, bot_peer, 1)

        with self.assertRaises(ValueError):
            await self.dm.on_message_evaluated(conv_id, user, 0, msg_id1)
        with self.assertRaises(ValueError):
            await self.dm.on_message_evaluated(conv_id, bot_peer, 20, msg_id1)
        with self.assertRaises(ValueError):
            await self.dm.on_message_evaluated(conv_id, bot_peer, -1, msg_id1)
        with self.assertRaises(ValueError):
            await self.dm.on_message_evaluated(conv_id, bot_peer, 1, msg_id1*100500 + 100500)
        with self.assertRaises(ValueError):
            await self.dm.on_message_evaluated(conv_id, user2, 1, msg_id1)

    @async_test
    async def test_trigger_dialog_end(self):
        user, user2 = User.objects(banned=False)[:2]

        self.dm.human_bot_ratio = 0
        await self.dm.on_human_initiated_dialog(user)

        conv_id = self.humans_gateway.start_conversation.call_args[0][0]
        bot_peer = self.bots_gateway.start_conversation.call_args[0][1]

        await self.dm.on_message_received(conv_id, user, "Human message", datetime.now())
        await self.dm.on_message_received(conv_id, bot_peer, "Bot message", datetime.now())

        with self.assertRaises(ValueError):
            await self.dm.trigger_dialog_end(conv_id, user2)
        with self.assertRaises(ValueError):
            await self.dm.trigger_dialog_end(conv_id*123 + 15, user)

        await self.dm.trigger_dialog_end(conv_id, user)

        self.humans_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.humans_gateway.start_evaluation.call_args[0][:2], (conv_id, user))
        self.bots_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.bots_gateway.start_evaluation.call_args[0][:2], (conv_id, bot_peer))

        with self.assertRaises(ValueError):
            await self.dm.on_message_received(conv_id, bot_peer, "Failed message to the finished dialog",
                                              datetime.now())

    @async_test
    async def test_evaluation(self):
        user = User.objects(banned=False)[0]

        self.dm.human_bot_ratio = 0
        await self.dm.on_human_initiated_dialog(user)

        conv_id = self.humans_gateway.start_conversation.call_args[0][0]
        bot_peer = self.bots_gateway.start_conversation.call_args[0][1]

        await self.dm.on_message_received(conv_id, user, "Human message", datetime.now())
        await self.dm.on_message_received(conv_id, bot_peer, "Bot message", datetime.now())

        with self.assertRaises(ValueError):
            await self.dm.evaluate_dialog(conv_id, user, 4)

        with self.assertRaises(ValueError):
            await self.dm.select_other_peer_profile(conv_id, user, 123)

        await self.dm.trigger_dialog_end(conv_id, user)

        with self.assertRaises(ValueError):
            await self.dm.select_other_peer_profile(conv_id, user, 123)

        with self.assertRaises(ValueError):
            await self.dm.evaluate_dialog(conv_id, bot_peer, 10500)

        with self.assertRaises(ValueError):
            await self.dm.evaluate_dialog(conv_id, bot_peer, -1)

        await self.dm.select_other_peer_profile(conv_id, bot_peer, 0)
        await self.dm.evaluate_dialog(conv_id, bot_peer, 3)

        await self.dm.select_other_peer_profile(conv_id, user, 0)

        self.humans_gateway.finish_conversation.assert_not_called()
        self.bots_gateway.finish_conversation.assert_not_called()

        await self.dm.evaluate_dialog(conv_id, user, 4)

        self.humans_gateway.finish_conversation.assert_called_once_with(conv_id)
        self.bots_gateway.finish_conversation.assert_called_once_with(conv_id)

    @async_test
    async def test_timeouts(self):
        user = User.objects(banned=False)[0]

        self.dm.human_bot_ratio = 0
        await self.dm.on_human_initiated_dialog(user)

        conv_id = self.humans_gateway.start_conversation.call_args[0][0]
        bot_peer = self.bots_gateway.start_conversation.call_args[0][1]

        await self.dm.on_message_received(conv_id, user, "Human message", datetime.now())
        cancelled_job = self.dm.scheduler.add_job.return_value
        cancelled_job.remove.reset_mock()

        await self.dm.on_message_received(conv_id, bot_peer, "Bot message", datetime.now())
        cancelled_job.remove.assert_called_once()

        # Conversation timeout...
        timeout_action = self.dm.scheduler.add_job.call_args[0][0]
        args, kwargs = self.dm.scheduler.add_job.call_args[1]['args'], self.dm.scheduler.add_job.call_args[1]['kwargs']

        await timeout_action(*args, **kwargs)

        self.humans_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.humans_gateway.start_evaluation.call_args[0][:2], (conv_id, user))
        self.bots_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.bots_gateway.start_evaluation.call_args[0][:2], (conv_id, bot_peer))

        # Evaluation timeout...
        timeout_action = self.dm.scheduler.add_job.call_args[0][0]
        args, kwargs = self.dm.scheduler.add_job.call_args[1]['args'], self.dm.scheduler.add_job.call_args[1]['kwargs']

        await timeout_action(*args, **kwargs)

        self.humans_gateway.finish_conversation.assert_called_once_with(conv_id)
        self.bots_gateway.finish_conversation.assert_called_once_with(conv_id)

    @async_test
    async def test_length_threshold(self):
        user = User.objects(banned=False)[0]

        self.dm.human_bot_ratio = 0
        self.dm.length_threshold = 2
        await self.dm.on_human_initiated_dialog(user)

        conv_id = self.humans_gateway.start_conversation.call_args[0][0]
        bot_peer = self.bots_gateway.start_conversation.call_args[0][1]

        await self.dm.on_message_received(conv_id, user, "Human message", datetime.now())
        await self.dm.on_message_received(conv_id, bot_peer, "Bot message", datetime.now())

        self.humans_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.humans_gateway.start_evaluation.call_args[0][:2], (conv_id, user))
        self.bots_gateway.start_evaluation.assert_called_once()
        self.assertSequenceEqual(self.bots_gateway.start_evaluation.call_args[0][:2], (conv_id, bot_peer))
