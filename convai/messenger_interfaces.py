import asyncio
import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from functools import partial
from typing import List, Union, Dict, Callable, Any, Coroutine, Optional, Awaitable, Tuple

import aiohttp
import telepot
from telepot.aio.loop import OrderedWebhook
from telepot.exception import TelegramError
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup

from model import User, UserPK

log = logging.getLogger(__name__)


class AbstractHumansGateway(ABC):
    @abstractmethod
    async def on_begin(self, user: User):
        pass

    @abstractmethod
    async def on_get_started(self, user: User):
        pass

    @abstractmethod
    async def on_help(self, user: User):
        pass

    @abstractmethod
    async def on_complain(self, user: User):
        pass

    @abstractmethod
    async def on_message_received(self, sender: User, text: str, time: datetime, msg_id: str = None):
        pass

    @abstractmethod
    async def on_evaluate_message(self, user: User, score: int, msg_id: str = None) -> bool:
        pass

    @abstractmethod
    async def on_evaluate_dialog(self, evaluator: User, score: int) -> bool:
        pass

    @abstractmethod
    async def on_other_peer_profile_selected(self, evaluator: User, profile_idx: int,
                                             sentence_idx: Optional[int] = None) -> bool:
        pass

    @abstractmethod
    async def on_end_dialog(self, initiator: User):
        pass

    @abstractmethod
    async def on_set_bot(self, user: User):
        pass


class AbstractMessenger(ABC):
    gateway: AbstractHumansGateway

    def __init__(self, gateway: AbstractHumansGateway):
        self.gateway = gateway

    @property
    def log(self) -> logging.Logger:
        return logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    @property
    def messenger_specific_help(self) -> str:
        return ""

    @property
    @abstractmethod
    def platform(self) -> str:
        pass

    def supports_platform(self, p: str) -> bool:
        return self.platform.lower() == p.lower()

    @abstractmethod
    def feed(self, data):
        pass

    async def send_message_to_user(self, user: User,
                                   msg_text: str,
                                   include_inline_evaluation_query: bool,
                                   **kwargs) -> str:
        self._validate_platform(user)
        return await self._send_message(user.user_key.user_id, msg_text, include_inline_evaluation_query, **kwargs)

    async def request_dialog_evaluation(self, user: User, msg_text: str, scores_range: range):
        self._validate_platform(user)
        await self._request_dialog_evaluation(user.user_key.user_id, msg_text, scores_range)

    async def request_profile_selection(self, user: User, msg_text: str, profiles: List[str],
                                        sentence_idx: Optional[int] = None):
        self._validate_platform(user)
        await self._request_profile_selection(user.user_key.user_id, msg_text, profiles, sentence_idx)

    def _validate_platform(self, user: User):
        if not self.supports_platform(user.user_key.platform):
            raise ValueError('Platform {} is not supported'.format(user.user_key.platform))

    @abstractmethod
    async def _send_message(self, user_id: str, msg_text: str, include_inline_evaluation_query: bool, **kwargs) -> str:
        pass

    @abstractmethod
    async def _request_dialog_evaluation(self, user_id: str, msg_text: str, scores_range: range):
        pass

    @abstractmethod
    async def _request_profile_selection(self, user_id: str, msg_text: str, profiles: List[str],
                                         sentence_idx: Optional[int] = None):
        pass


class TelegramMessenger(AbstractMessenger):
    def __init__(self, gateway: AbstractHumansGateway, tg_bot: telepot.aio.Bot, webhook_address: str):
        super().__init__(gateway)
        self.webhook_address = webhook_address
        self._tg_bot = tg_bot
        self._webhook = OrderedWebhook(self._tg_bot,
                                       {
                                           'chat': self._on_chat_msg,
                                           'callback_query': self._on_callback_query
                                       })

        loop = asyncio.get_event_loop()
        loop.create_task(self._webhook.run_forever())

    @property
    def platform(self) -> str:
        return UserPK.PLATFORM_TELEGRAM

    def feed(self, data: Union[str, bytes, Dict]):
        self.log.info('webhook received data')
        self._webhook.feed(data)

    async def perform_initial_setup(self):
        self.log.info('webhook address set')
        await self._tg_bot.setWebhook(self.webhook_address)

    async def _send_message(self, user_id: str,
                            msg_text: str,
                            include_inline_evaluation_query: bool,
                            keyboard_buttons: Optional[List[str]] = None,
                            **kwargs) -> str:
        if include_inline_evaluation_query:
            kb = self._get_evaluate_msg_keyboard()
        elif keyboard_buttons is not None:
            kb = ReplyKeyboardMarkup(keyboard=[keyboard_buttons], resize_keyboard=True)
        else:
            kb = None
        kwargs = {'reply_markup': kb} if kb is not None else {}
        reply = await self._send_msg_with_timeouts_handling(user_id, msg_text, **kwargs)

        self.log.info(f'message sent to {user_id}')
        return str(reply['message_id'])

    async def _request_dialog_evaluation(self, user_id: str, msg_text: str, scores_range: range):
        self._cached_scores_range = scores_range
        kb = self._get_evaluate_dialog_keyboard()
        await self._send_msg_with_timeouts_handling(user_id, msg_text, reply_markup=kb)
        self.log.info(f'dialog evaluation requested from {user_id}')

    async def _request_profile_selection(self, user_id: str, msg_text: str, profiles: List[str],
                                         sentence_idx: Optional[int] = None):
        if sentence_idx is not None:
            kb = self._get_select_profile_keyboard(str(sentence_idx))
        else:
            kb = self._get_select_profile_keyboard()
        text = f'{msg_text}\n\n1️⃣:\n{profiles[0]}\n\n2️⃣:\n{profiles[1]}'
        await self._send_msg_with_timeouts_handling(user_id, text, reply_markup=kb)
        self.log.info(f'partner profile selection requested from {user_id}')

    async def _send_msg_with_timeouts_handling(self, user_id, msg_text, *args, **kwargs):
        while True:
            try:
                return await self._tg_bot.sendMessage(user_id, msg_text, *args, **kwargs)
            except TelegramError as e:
                if e.error_code == 429:
                    params = e.json['parameters'] if 'parameters' in e.json else {}
                    timeout = params['retry_after'] if 'retry_after' in params else 1
                    self.log.warning(f'Too many requests. Waiting for {timeout} seconds...')
                    await asyncio.sleep(timeout)
                elif e.error_code == 504:
                    self.log.warning(f'Timeout. Retrying...')
                else:
                    raise

    async def _on_chat_msg(self, msg: Dict):
        self.log.info(f'chat message received')
        parsed_msg = telepot.namedtuple.Message(**msg)
        internal_user = self._internal_user_from_tg_user(parsed_msg.from_)

        def extract_entity(e: telepot.namedtuple.MessageEntity):
            slice_from = e.offset
            slice_to = slice_from + e.length
            return parsed_msg.text[slice_from: slice_to]

        commands = [extract_entity(e).lower() for e in parsed_msg.entities if e.type == 'bot_command'] \
            if parsed_msg.entities else []

        command_handlers = {'/help': partial(self.gateway.on_help, internal_user),
                            '/begin': partial(self.gateway.on_begin, internal_user),
                            '/end': partial(self.gateway.on_end_dialog, internal_user),
                            '/start': partial(self.gateway.on_get_started, internal_user),
                            '/complain': partial(self.gateway.on_complain, internal_user),
                            '/setbot': partial(self.gateway.on_set_bot, internal_user)}

        valid_commands = [c for c in commands if c in command_handlers]

        if valid_commands:
            self.log.info(f'handling command: {valid_commands[0]}')
            await command_handlers[valid_commands[0]]()
            return

        date = datetime.fromtimestamp(parsed_msg.date)
        await self.gateway.on_message_received(internal_user, parsed_msg.text, date, str(parsed_msg.message_id))

    async def _on_callback_query(self, msg: Dict):
        self.log.info(f'callback query received')
        query_id, sender_id, data = telepot.glance(msg, flavor='callback_query')
        parsed_msg = telepot.namedtuple.CallbackQuery(**msg)
        internal_user = self._internal_user_from_tg_user(parsed_msg.from_)

        self.log.debug(f'callback query data: {data}')

        match = re.match(r'/(\w+) (.*)', data)
        command, cmd_args = match.groups()

        def get_command_handler(cmd: str, args: str) -> \
                Tuple[Callable[[], Awaitable[bool]], Callable[[], InlineKeyboardMarkup]]:
            arg, *args = args.split(' ')
            arg = int(arg)

            if cmd == 'select_profile':
                if len(args) > 0:
                    return (partial(self.gateway.on_other_peer_profile_selected, internal_user, arg, int(args[0])),
                            partial(self._get_select_profile_keyboard, args[0], arg))
                else:
                    return (partial(self.gateway.on_other_peer_profile_selected, internal_user, arg),
                            partial(self._get_select_profile_keyboard, selected_button_idx=arg))
            if cmd == 'rate_dialog':
                return (partial(self.gateway.on_evaluate_dialog, internal_user, arg),
                        partial(self._get_evaluate_dialog_keyboard, arg))
            if cmd == 'rate_msg':
                return (partial(self.gateway.on_evaluate_message, internal_user, arg, parsed_msg.message.message_id),
                        partial(self._get_evaluate_msg_keyboard, arg))

        feedback = 'Evaluation saved!'

        handler_and_kb = get_command_handler(command, cmd_args)

        if handler_and_kb is None:
            self.log.error(f'unexpected callback query: {command}')
            feedback = f'Internal error: no handler for "{command}"'
        else:
            self.log.info(f'handling callback query: {command}')
            handler, kb_provider = handler_and_kb
            result = await handler()
            if not result:
                self.log.error(f'callback query handling failed: {command}')
                feedback = 'Error!'
            else:
                await self._update_inline_keyboard(parsed_msg.from_.id,
                                                   parsed_msg.message.message_id,
                                                   kb_provider())

        await self._tg_bot.answerCallbackQuery(query_id, text=feedback)

    async def _update_inline_keyboard(self, chat_id: int, msg_id: int, kb: InlineKeyboardMarkup):
        await self._tg_bot.editMessageReplyMarkup(msg_identifier=(chat_id, msg_id),
                                                  reply_markup=kb)

    @staticmethod
    def _get_flat_inline_keyboard_buttons(texts: List[str],
                                          command: Callable[[int, str], str],
                                          selected_button_idx: Optional[int] = None) -> List[InlineKeyboardButton]:
        if selected_button_idx is not None:
            texts[selected_button_idx] = '⭐' + texts[selected_button_idx]

        return [InlineKeyboardButton(text=text, callback_data=command(i, text)) for i, text in enumerate(texts)]

    def _get_evaluate_msg_keyboard(self, selected_button_idx: Optional[int] = None) -> InlineKeyboardMarkup:
        buttons = self._get_flat_inline_keyboard_buttons(['👎', '👍'],
                                                         lambda i, txt: '/rate_msg ' + str(i),
                                                         selected_button_idx)
        return InlineKeyboardMarkup(inline_keyboard=[buttons])

    def _get_select_profile_keyboard(self, additional_data: str = '',
                                     selected_button_idx: Optional[int] = None) -> InlineKeyboardMarkup:
        def command_provider(i: int, txt: str) -> str:
            res = f'/select_profile {i}'
            if additional_data:
                res += f' {additional_data}'
            return res

        buttons = self._get_flat_inline_keyboard_buttons(['1️⃣', '2️⃣'],
                                                         command_provider,
                                                         selected_button_idx)
        return InlineKeyboardMarkup(inline_keyboard=[buttons])

    def _get_evaluate_dialog_keyboard(self,
                                      score: Optional[int] = None) -> InlineKeyboardMarkup:
        scores_range = self._cached_scores_range if hasattr(self, '_cached_scores_range') else range(1, 6)

        btn_idx = score - scores_range.start if score is not None else None

        flat_buttons = self._get_flat_inline_keyboard_buttons(list(map(str, scores_range)),
                                                              lambda i, txt: '/rate_dialog ' + txt,
                                                              btn_idx)

        from math import ceil
        row_length = max(5, int(ceil(len(flat_buttons) ** 0.5)))
        buttons = [flat_buttons[row_length * i:row_length * (i + 1)]
                   for i in range((len(flat_buttons) + row_length - 1) // row_length)]
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    def _internal_user_from_tg_user(self, tg_user: Union[telepot.namedtuple.User, Dict]) -> User:
        if not isinstance(tg_user, telepot.namedtuple.User):
            tg_user = telepot.namedtuple.User(**tg_user)
        username = tg_user.username or ' '.join([x for x in (tg_user.first_name, tg_user.last_name) if x])
        return User(user_key=UserPK(platform=self.platform, user_id=str(tg_user.id)), username=username)


class FacebookMessenger(AbstractMessenger):
    class QuickReplyOption:
        def __init__(self, title: str, payload: str):
            self.payload = payload
            self.title = title

        @property
        def dict_value(self):
            return {
                "content_type": "text",
                "title": self.title,
                "payload": self.payload
            }

    def __init__(self, gateway: AbstractHumansGateway, fb_page_access_token: str):
        super().__init__(gateway)
        self.access_token = fb_page_access_token

        self._queue = asyncio.Queue()
        loop = asyncio.get_event_loop()
        loop.create_task(self._run_forever())

    @property
    def platform(self) -> str:
        return UserPK.PLATFORM_FACEBOOK

    async def feed(self, data):
        self.log.info('webhook received data')
        await self._queue.put(data)

    async def perform_initial_setup(self, setup_profile: bool = False):
        if setup_profile:
            self.log.info('profile setup')
            await self.setup_profile()

    async def _run_forever(self):
        while 1:
            # noinspection PyBroadException
            try:
                data = await self._queue.get()
                await self._process_incoming(data)
            except Exception as e:
                self.log.exception(e)

    async def _process_incoming(self, data):
        json_data = json.loads(data)
        if json_data['object'] != 'page':
            self.log.warning(f"unexpected update {json_data['object']}. Full payload: {json_data}")
            return
        for entry in json_data['entry']:
            for msg in entry['messaging']:
                sender_id = msg['sender']['id']
                sender = self._internal_user_from_fb_user_id(sender_id)
                timestamp = msg['timestamp']
                dt = datetime.fromtimestamp(timestamp / 1000)
                if 'message' in msg:
                    msg_text = msg['message']['text']
                    msg_id = msg['message']['mid']
                    if 'quick_reply' in msg['message']:
                        payload = msg['message']['quick_reply']['payload']
                        await self._on_quick_action_response(sender, payload)
                    else:
                        await self._on_chat_msg(sender, msg_text, dt, msg_id)
                elif 'postback' in msg:
                    payload = msg['postback']['payload']
                    await self._on_postback(sender, payload)
                else:
                    self.log.warning(f'Unexpected messaging entry: {msg}')

    async def _send_message(self, user_id: str, msg_text: str, include_inline_evaluation_query: bool, **kwargs) -> str:
        qr = [self.QuickReplyOption('👍', '/rate_msg 1'), self.QuickReplyOption('👎', '/rate_msg 0')]
        if not include_inline_evaluation_query:
            qr = None

        self.log.info(f'message sent to {user_id}')
        return await self._send_text_message(int(user_id), msg_text, qr)

    async def _request_dialog_evaluation(self, user_id: str, msg_text: str, scores_range: range):
        buttons = [self.QuickReplyOption(title=str(x), payload='/rate_dialog ' + str(x)) for x in scores_range]
        await self._send_text_message(int(user_id), msg_text, buttons)
        self.log.info(f'dialog evaluation requested from {user_id}')

    async def _request_profile_selection(self, user_id: str, msg_text: str, profiles: List[str],
                                         sentence_idx: Optional[int] = None):
        buttons = [self.QuickReplyOption(title='1️⃣', payload='/select_profile 0'),
                   self.QuickReplyOption(title='2️⃣', payload='/select_profile 1')]

        text = '{}\n\n1️⃣:\n{}\n2️⃣:\n{}'.format(msg_text, profiles[0], profiles[1])
        await self._send_text_message(int(user_id), text, buttons)
        self.log.info(f'partner profile selection requested from {user_id}')

    async def _send_text_message(self, recipient_id: int, text: str,
                                 quick_replies: Union[None, List[QuickReplyOption]] = None) -> str:
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": text}
        }

        if quick_replies is not None:
            # noinspection PyTypeChecker
            payload['message']['quick_replies'] = [qr.dict_value for qr in quick_replies]

        return (await self._call_messaging_api(payload))['message_id']

    async def _call_api(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = "https://graph.facebook.com/v2.6/me/" + endpoint
        querystring = {"access_token": self.access_token}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, params=querystring, json=payload) as response:
                return await response.json()

    async def _call_messaging_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._call_api('messages', payload)

    async def _call_profile_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._call_api('messenger_profile', payload)

    @property
    def _postback_command_handlers(self) -> Dict[str, Callable[[User], Coroutine]]:
        return {'/help': self.gateway.on_help,
                '/begin': self.gateway.on_begin,
                '/end': self.gateway.on_end_dialog,
                '/start': self.gateway.on_get_started,
                '/complain': self.gateway.on_complain}

    async def _on_chat_msg(self, sender: User, text: str, date: datetime, msg_id: str):
        self.log.info(f'chat message received')
        valid_commands = [x.lower() for x in re.findall(r'/\w+', text) if x.lower() in self._postback_command_handlers]
        if valid_commands:
            handler = self._postback_command_handlers[valid_commands[0]]
            await handler(sender)
            return

        await self.gateway.on_message_received(sender, text, date, msg_id)

    async def _on_quick_action_response(self, sender: User, payload: str):
        self.log.info(f'quick action response received')
        match = re.match(r'/(\w+) (.*)', payload)
        command, arg = match.groups()
        arg = int(arg)

        handlers: Dict[str, Callable[[int], Coroutine[Any, Any, bool]]] = {
            'select_profile': partial(self.gateway.on_other_peer_profile_selected, sender),
            'rate_dialog': partial(self.gateway.on_evaluate_dialog, sender),
            'rate_msg': partial(self.gateway.on_evaluate_message, sender)
        }

        if command not in handlers:
            raise ValueError('No handler for "{}"'.format(command))
        else:
            await handlers[command](arg)

    async def _on_postback(self, sender: User, payload: str):
        self.log.info(f'postback action received')
        if payload not in self._postback_command_handlers:
            raise ValueError('No handler for "{}"'.format(payload))
        else:
            handler = self._postback_command_handlers[payload]
            await handler(sender)

    def _internal_user_from_fb_user_id(self, fb_user_id: int) -> User:
        return User(user_key=UserPK(platform=self.platform, user_id=str(fb_user_id)))

    async def setup_profile(self):
        payload = {
            "get_started": {
                "payload": "/start"
            },
            "greeting": [
                {
                    "locale": "default",
                    "text": "Some greeting text. To be filled..."
                }
            ],
            "persistent_menu": [
                {
                    "locale": "default",
                    "call_to_actions": [
                        {
                            "title": "Begin",
                            "type": "postback",
                            "payload": "/begin"
                        },
                        {
                            "title": "End",
                            "type": "postback",
                            "payload": "/end"
                        },
                        {
                            "title": "More...",
                            "type": "nested",
                            "call_to_actions": [
                                {
                                    "title": "Help",
                                    "type": "postback",
                                    "payload": "/help"
                                },
                                {
                                    "title": "Complain",
                                    "type": "postback",
                                    "payload": "/complain"
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        res = await self._call_profile_api(payload)
        return res
