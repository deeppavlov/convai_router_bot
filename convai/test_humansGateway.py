import asyncio
from datetime import datetime
from unittest.mock import Mock, PropertyMock

from convai.conversation_gateways import HumansGateway, AbstractDialogHandler, NoopDialogHandler, AbstractGateway
from convai.messenger_interfaces import AbstractMessenger
from model import User, UserPK, PersonProfile
from model.test_common import MockedMongoTestCase, AsyncMock, async_test


class TestHumansGateway(MockedMongoTestCase):
    @property
    async def user(self) -> User:
        return await self.gw._update_user_record_in_db(User(user_key=UserPK(platform=UserPK.PLATFORM_TELEGRAM,
                                                                            user_id="123")))

    def setUp(self):
        super(TestHumansGateway, self).setUp()
        self.gw = HumansGateway(False)
        self.dialog_handler = Mock(AbstractDialogHandler)
        self.dialog_handler.on_human_initiated_dialog = AsyncMock()
        self.dialog_handler.on_message_received = AsyncMock()
        self.dialog_handler.on_message_evaluated = AsyncMock()
        self.dialog_handler.trigger_dialog_end = AsyncMock()
        self.dialog_handler.evaluate_dialog = AsyncMock()
        self.dialog_handler.select_other_peer_profile = AsyncMock()

        self.gw.dialog_handler = self.dialog_handler
        self.messenger = Mock(AbstractMessenger)
        p = PropertyMock(return_value="UniversalMock")
        type(self.messenger).platform = p
        p = PropertyMock(return_value="")
        type(self.messenger).messenger_specific_help = p
        self.messenger.supports_platform.return_value = True

        self.messenger.send_message_to_user = AsyncMock()
        self.messenger.send_message_to_user.return_value = 100500
        self.messenger.request_dialog_evaluation = AsyncMock()
        self.messenger.request_profile_selection = AsyncMock()

        self.gw.add_messengers(self.messenger)

    @property
    def stub_profile(self):
        return PersonProfile(sentences=["blah", "blah"])

    @property
    def stub_profile_choices(self):
        return [PersonProfile(sentences=["blah1", "blah2"]), PersonProfile(sentences=["blah3", "blah4"])]

    def test_dialog_handler(self):
        self.assertEqual(self.gw.dialog_handler, self.dialog_handler)
        del self.gw.dialog_handler
        self.assertIsInstance(self.gw.dialog_handler, NoopDialogHandler)

    def test_add_messengers(self):
        tg_user = User(user_key=UserPK(platform=UserPK.PLATFORM_TELEGRAM, user_id="123"))
        fb_user = User(user_key=UserPK(platform=UserPK.PLATFORM_FACEBOOK, user_id="123"))

        self.assertEqual(self.gw._messenger_for_user(tg_user), self.messenger)
        self.assertEqual(self.gw._messenger_for_user(fb_user), self.messenger)

        tg_messenger = Mock(AbstractMessenger)
        p = PropertyMock(return_value=UserPK.PLATFORM_TELEGRAM)
        type(tg_messenger).platform = p
        tg_messenger.supports_platform = lambda x: x == UserPK.PLATFORM_TELEGRAM

        fb_messenger = Mock(AbstractMessenger)
        p = PropertyMock(return_value=UserPK.PLATFORM_FACEBOOK)
        type(fb_messenger).platform = p
        fb_messenger.supports_platform = lambda x: x == UserPK.PLATFORM_FACEBOOK

        self.gw.add_messengers(tg_messenger, fb_messenger)

        self.assertEqual(self.gw._messenger_for_user(tg_user), tg_messenger)
        self.assertEqual(self.gw._messenger_for_user(fb_user), fb_messenger)

    @async_test
    async def test_on_begin(self):
        user = await self.user
        await self.gw.on_begin(user)
        self.messenger.send_message_to_user.assert_called_once_with(user, "Searching for peer. Please wait...",
                                                                    False)
        self.dialog_handler.on_human_initiated_dialog.assert_called_once_with(user)

        await self.gw.on_begin(user)
        self.assertIn('/help', self.messenger.send_message_to_user.call_args[0][1])

        self.assertEqual(self.gw._user_states[user], self.gw.UserState.IN_LOBBY)

    @async_test
    async def test_on_help(self):
        user = await self.user
        await self.gw.on_help(user)
        self.messenger.send_message_to_user.assert_called_once_with(user, "Some help message. To be filled...",
                                                                    False)

    @async_test
    async def test_on_get_started(self):
        user = await self.user
        await self.gw.on_get_started(user)
        self.messenger.send_message_to_user.assert_called_once_with(user,
                                                                    "Some welcome message. To be filled...",
                                                                    False, keyboard_buttons=['/begin', '/help'])

    @async_test
    async def test_on_message_received(self):
        user = await self.user
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        self.assertIn('Unexpected message', self.messenger.send_message_to_user.call_args[0][1])

        await self.gw.on_begin(user)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        self.assertIn('Unexpected message', self.messenger.send_message_to_user.call_args[0][1])

        await self.gw.start_conversation(1, user, self.stub_profile)
        t = datetime.now()
        await self.gw.on_message_received(user, "123", t, '123')
        self.dialog_handler.on_message_received.assert_called_once_with(1, user, "123", t)

    @async_test
    async def test_on_evaluate_message(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_evaluate_message(user, 1)
        self.assertEqual(self.dialog_handler.on_message_evaluated.call_args[0][:3], (1, user, 1))

    @async_test
    async def test_on_end_dialog(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_end_dialog(user)
        self.dialog_handler.trigger_dialog_end.assert_called_once_with(1, user)

    @async_test
    async def test_on_evaluate_dialog(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_end_dialog(user)

        options = self.stub_profile_choices
        await self.gw.start_evaluation(1, user, options, options[0], range(1, 6))
        await self.gw.on_evaluate_dialog(user, 3)
        self.dialog_handler.evaluate_dialog.assert_called_once_with(1, user, 3)
        self.assertEqual(user, self.messenger.request_profile_selection.call_args[0][0])
        self.assertIn('Select a profile', self.messenger.request_profile_selection.call_args[0][1])
        self.assertEqual([x.description for x in options], self.messenger.request_profile_selection.call_args[0][2])

    @async_test
    async def test_on_other_peer_profile_selected(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_end_dialog(user)

        options = self.stub_profile_choices
        await self.gw.start_evaluation(1, user, options, options[0], range(1, 6))
        await self.gw.on_evaluate_dialog(user, 3)
        await self.gw.on_other_peer_profile_selected(user, 0)
        self.dialog_handler.select_other_peer_profile.assert_called_once_with(1, user, 0)

    @async_test
    async def test_start_conversation(self):
        user = await self.user
        await self.gw.on_begin(user)
        self.messenger.send_message_to_user.reset_mock()
        await self.gw.start_conversation(1, user, self.stub_profile)
        self.assertEqual(self.messenger.send_message_to_user.call_count, 3)

    @async_test
    async def test_send_message(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        self.messenger.send_message_to_user.reset_mock()
        await self.gw.send_message(1, 345, '123', user)
        self.messenger.send_message_to_user.assert_called_once_with(user, '123', True)

    @async_test
    async def test_start_evaluation(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_end_dialog(user)
        options = self.stub_profile_choices
        await self.gw.start_evaluation(1, user, options, options[0], range(1, 6))
        self.messenger.request_dialog_evaluation.assert_called_once_with(user,
                                                                         'Please evaluate the whole dialog using one '
                                                                         'of the buttons below',
                                                                         range(1, 6))

    @async_test
    async def test_finish_conversation(self):
        user = await self.user
        await self.gw.on_begin(user)
        await self.gw.start_conversation(1, user, self.stub_profile)
        await self.gw.on_message_received(user, "123", datetime.now(), '123')
        await self.gw.send_message(1, 345, '123', user)
        await self.gw.on_end_dialog(user)

        options = self.stub_profile_choices
        await self.gw.start_evaluation(1, user, options, options[0], range(1, 6))
        await self.gw.on_evaluate_dialog(user, 3)
        await self.gw.on_other_peer_profile_selected(user, 0)
        self.messenger.send_message_to_user.reset_mock()
        await self.gw.finish_conversation(1)
        self.messenger.send_message_to_user.assert_called_once_with(user,
                                                                    'Dialog is finished. Thank you for '
                                                                    'participation!',
                                                                    False, keyboard_buttons=['/begin', '/help'])

    @async_test
    async def test_on_conversation_failed(self):
        user = await self.user
        await self.gw.on_conversation_failed(user, AbstractGateway.ConversationFailReason.PEER_NOT_FOUND)
        self.messenger.send_message_to_user.assert_called_once_with(user, 'No peers found 😔\nTry again later',
                                                                    False, keyboard_buttons=['/begin', '/help'])
        self.messenger.send_message_to_user.reset_mock()
        await self.gw.on_conversation_failed(user, AbstractGateway.ConversationFailReason.BANNED)
        self.messenger.send_message_to_user.assert_called_once_with(user, 'You are banned from using the system',
                                                                    False, keyboard_buttons=['/begin', '/help'])
