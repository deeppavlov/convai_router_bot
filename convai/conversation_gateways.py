import asyncio
import calendar
import enum
import json
import logging
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from random import randrange, shuffle
from typing import Union, List, Dict, DefaultDict, Optional, Set, Tuple

from convai import run_sync_in_executor
from convai.exceptions import BotNotRegisteredError, ProfileTrigramDetectedInMessageError
from convai.messenger_interfaces import AbstractMessenger, AbstractHumansGateway
from model import PersonProfile, Bot, User


class AbstractDialogHandler(ABC):
    @abstractmethod
    async def on_human_initiated_dialog(self, user: User):
        """
        Should be called when human user wants to start a conversation
        :param user: a human that initiated a dialog
        """
        pass

    @abstractmethod
    async def on_message_received(self, conversation_id: int, sender: Union[Bot, User], text: str,
                                  time: datetime) -> int:
        """
        Should be called when a message from one of the dialog parties is received

        :param conversation_id: integer id of the dialog
        :param sender: a person or bot who sent the message
        :param text: message contents
        :param time: time when the message was sent

        :returns message_id: unique ID of the message within this conversation
        """
        pass

    @abstractmethod
    async def on_message_evaluated(self, conversation_id: int, evaluator: Union[Bot, User], score: int,
                                   msg_id: int = None):
        """
        Should be called when a single message is evaluated by one of the peers

        :param conversation_id: integer id of the dialog
        :param evaluator: a person or bot who evaluated the message
        :param score: evaluation score. Should be within a range [0, 1]
        :param msg_id: Id of the evaluated message. Default value is None. In such case the last message from the other
         party is implied
        """
        pass

    @abstractmethod
    async def trigger_dialog_end(self, conversation_id: int, peer: Union[Bot, User]):
        """
        Should be called when one of the parties wants to complete the dialog either intentionally (by sending /end
        command) or unintentionally (e.g. bot fails to provide an acceptable answer after many retries)

        :param conversation_id: integer id of the dialog
        :param peer: a peer willing to complete the talk
        """
        pass

    @abstractmethod
    async def evaluate_dialog(self, conversation_id: int, evaluator: Union[User, Bot], score: Optional[int]):
        """
        Should be called when the whole dialog is evaluated by one of the peers at the end of the talk

        :param conversation_id: integer id of the dialog
        :param evaluator: a person or bot who evaluated the dialog
        :param score: evaluation score. Should be within a range [1, 5]
        """
        pass

    @abstractmethod
    async def select_other_peer_profile(self, conversation_id: int, evaluator: Union[User, Bot],
                                        profile_idx: Optional[int]):
        """
        Should be called when one of the peers guesses profile belonging to another peer

        :param conversation_id: integer id of the dialog
        :param evaluator: a person or bot who was selecting the profile
        :param profile_idx: index of the selected profile
        """
        pass

    @abstractmethod
    async def select_other_peer_profile_sentence(self, conversation_id: int, evaluator: Union[User, Bot],
                                                 sentence: str, sentence_idx: Optional[int] = None):
        """
        Should be called when one of the peers guesses another sentence of the profile belonging to another peer

        :param conversation_id: integer id of the dialog
        :param evaluator: a person or bot who was selecting the profile
        :param sentence: selected sentence
        :param sentence_idx: index of the sentence selected
        """
        pass

    @abstractmethod
    async def complain(self, conversation_id: int, complainer: User) -> bool:
        """
        Should be called when the user wants to report about insults or inappropriate behavior

        :param conversation_id: integer id of the dialog
        :param complainer: a person who complains
        """
        pass


class AbstractGateway(ABC):
    class ConversationFailReason(enum.Enum):
        BANNED = enum.auto()
        PEER_NOT_FOUND = enum.auto()

    _dialog_handler: AbstractDialogHandler

    def __init__(self):
        self._dialog_handler = NoopDialogHandler()

    @property
    def log(self) -> logging.Logger:
        return logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    @property
    def dialog_handler(self) -> AbstractDialogHandler:
        return self._dialog_handler

    @dialog_handler.setter
    def dialog_handler(self, value: AbstractDialogHandler):
        self._dialog_handler = value

    @dialog_handler.deleter
    def dialog_handler(self):
        self._dialog_handler = NoopDialogHandler()

    @abstractmethod
    async def start_conversation(self, conversation_id: int, own_peer: Union[User, Bot], profile: PersonProfile):
        """
        Handles the start of the conversation

        :param conversation_id: unique id of the conversation
        :param own_peer: User or Bot participating in a talk
        :param profile: a profile to "role-play" during this conversation
        """
        pass

    @abstractmethod
    async def send_message(self, conversation_id: int, msg_id: int, msg_text: str, receiving_peer: Union[User, Bot]):
        """
        Handles message delivery

        :param conversation_id: unique id of the conversation
        :param msg_id: unique id of the message within the current dialog
        :param msg_text: message contents
        :param receiving_peer: a peer which should receive this message
        """
        pass

    @abstractmethod
    async def start_evaluation(self, conversation_id: int, peer: Union[User, Bot],
                               other_peer_profile_options: List[PersonProfile],
                               other_peer_profile_correct: PersonProfile, scores_range: range):
        """
        Handles initiation of the evaluation stage of the talk

        :param conversation_id: unique id of the conversation
        :param peer: user or bot which should evaluate the dialog
        :param other_peer_profile_options: a list of PersonProfiles. Evaluator has to choose the one used
        :param other_peer_profile_correct: true profile of another peer
        :param scores_range: a range of valid scores
        by the other peer
        """
        pass

    @abstractmethod
    async def finish_conversation(self, conversation_id: int):
        """
        Handles completion of the conversation. E.g. Sending "Thank you" messages to humans, doing some internal
        cleanups, etc.

        :param conversation_id: unique id of the conversation
        """
        pass


class NoopDialogHandler(AbstractDialogHandler):

    async def on_human_initiated_dialog(self, user: User):
        pass

    async def on_message_received(self, conversation_id: int, sender: Union[Bot, User], text: str,
                                  time: datetime) -> int:
        return -1

    async def on_message_evaluated(self, conversation_id: int, evaluator: Union[Bot, User], score: int,
                                   msg_id: int = None):
        pass

    async def trigger_dialog_end(self, conversation_id: int, peer: Union[Bot, User]):
        pass

    async def evaluate_dialog(self, conversation_id: int, evaluator: Union[User, Bot], score: Optional[int]):
        pass

    async def select_other_peer_profile(self, conversation_id: int, evaluator: Union[User, Bot],
                                        profile_idx: Optional[int]):
        pass

    async def select_other_peer_profile_sentence(self, conversation_id: int, evaluator: Union[User, Bot],
                                                 sentence: str, sentence_idx: Optional[int] = None):
        pass

    async def complain(self, conversation_id: int, complainer: User):
        return True


class HumansGateway(AbstractGateway, AbstractHumansGateway):
    class ConversationRecord:
        message_ids_map: Dict[str, int]
        opponent_profile_options: List[PersonProfile]
        opponent_profile_correct: PersonProfile
        sentences_selected: int
        shuffled_sentences: List[Tuple[str, str]]

        def __init__(self, conv_id):
            self.conv_id = conv_id
            self.message_ids_map = {}
            self.opponent_profile_options = None
            self.opponent_profile_correct = None
            self.sentences_selected = 0
            self.shuffled_sentences = []

    class UserState(enum.Flag):
        IDLE = enum.auto()
        IN_LOBBY = enum.auto()
        IN_DIALOG = enum.auto()
        EVALUATING = enum.auto()
        WAITING_FOR_PARTNER_EVALUATION = enum.auto()

    _messengers: Dict[str, AbstractMessenger]
    _conversations: Dict[User, ConversationRecord]
    _user_states: DefaultDict[User, UserState]
    guess_profile_sentence_by_sentence: bool

    def __init__(self, guess_profile_sentence_by_sentence: bool):
        super().__init__()
        self._messengers = {}
        self._conversations = {}
        self._user_states = defaultdict(lambda: self.UserState.IDLE)

        self.guess_profile_sentence_by_sentence = guess_profile_sentence_by_sentence

    def add_messengers(self, *messengers: AbstractMessenger):
        self._messengers.update({m.platform: m for m in messengers if isinstance(m, AbstractMessenger)})

    async def on_begin(self, user: User):
        self.log.info(f'dialog begin requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user, self.UserState.IDLE, 'Cannot start a new conversation. Please '
                                                                          'finish your current dialog first. Use '
                                                                          '/help command for usage instructions'):
            return

        if user.banned:
            await self.on_conversation_failed(user, AbstractGateway.ConversationFailReason.BANNED)
            return

        self._user_states[user] = self.UserState.IN_LOBBY

        wait_txt = "Searching for peer. Please wait..."
        await asyncio.gather(messenger.send_message_to_user(user, wait_txt, False),
                             self.dialog_handler.on_human_initiated_dialog(user))

    async def on_help(self, user: User):
        self.log.info(f'help requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)
        help_txt = "Some help message. To be filled..."
        if messenger.messenger_specific_help:
            help_txt += '\n\n' + messenger.messenger_specific_help
        await messenger.send_message_to_user(user, help_txt, False)

    async def on_get_started(self, user: User):
        self.log.info(f'welcome message requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)
        welcome_txt = "Some welcome message. To be filled..."
        await messenger.send_message_to_user(user, welcome_txt, False, keyboard_buttons=['/begin', '/help'])

    async def on_complain(self, user: User):
        self.log.info(f'user complained')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.IN_DIALOG |
                                               self.UserState.EVALUATING |
                                               self.UserState.WAITING_FOR_PARTNER_EVALUATION,
                                               'You are not in a dialog. Complaining is not available'):
            return
        conv = self._conversations[user]
        result = await self.dialog_handler.complain(conv.conv_id, user)
        info_txt = "Your complaint has been recorded and will be examined by the system administrator. Note that " \
                   "your conversation is still active. You can always use /end command to end it"
        fail_msg = 'Could not save your complaint. Have the dialog even started? You cannot complain when there is ' \
                   'no messages in a dialog'
        await messenger.send_message_to_user(user,
                                             info_txt if result else fail_msg,
                                             False)

    async def on_message_received(self, sender: User, text: str, time: datetime, msg_id: str = None):
        self.log.info(f'message received')
        user = await self._update_user_record_in_db(sender)
        if not await self._validate_user_state(user, self.UserState.IN_DIALOG, 'Unexpected message. You are not in a '
                                                                               'dialog yet or the dialog has already '
                                                                               'been finished. Use /help command for '
                                                                               'usage instructions'):
            return

        conv = self._conversations[user]

        internal_id = await self._dialog_handler.on_message_received(conv.conv_id, user, text, time)
        if msg_id is not None:
            conv.message_ids_map[msg_id] = internal_id

    async def on_evaluate_message(self, user: User, score: int, msg_id: str = None) -> bool:
        self.log.info(f'message evaluated')
        user = await self._update_user_record_in_db(user)
        if not await self._validate_user_state(user,
                                               self.UserState.IN_DIALOG |
                                               self.UserState.EVALUATING |
                                               self.UserState.WAITING_FOR_PARTNER_EVALUATION):
            return False

        conv = self._conversations[user]
        internal_id = conv.message_ids_map[msg_id] if msg_id in conv.message_ids_map else None

        await self.dialog_handler.on_message_evaluated(conv.conv_id, user, score, internal_id)
        return True

    async def on_end_dialog(self, initiator: User):
        self.log.info(f'dialog end requested')
        user = await self._update_user_record_in_db(initiator)
        if not await self._validate_user_state(user, self.UserState.IN_DIALOG, "You're not in a dialog now."):
            return

        conv = self._conversations[user]
        await self.dialog_handler.trigger_dialog_end(conv.conv_id, user)

    async def on_evaluate_dialog(self, evaluator: User, score: int) -> bool:
        self.log.info(f'dialog evaluated')
        user = await self._update_user_record_in_db(evaluator)
        messenger = self._messenger_for_user(user)
        if not await self._validate_user_state(user, self.UserState.EVALUATING, 'Evaluation is not allowed at the '
                                                                                'moment. Use /help command for usage '
                                                                                'instructions'):
            return False

        conv = self._conversations[user]
        await self.dialog_handler.evaluate_dialog(conv.conv_id, user, score)

        if self.guess_profile_sentence_by_sentence:
            if not conv.shuffled_sentences:
                await self._prepare_profile_sentences(user)
                await self._request_next_profile_sentence_guess(user)
        else:
            msg = 'Select a profile which, in your opinion, belongs to your partner: '
            await messenger.request_profile_selection(user, msg, [x.description for x in conv.opponent_profile_options])
        return True

    async def on_other_peer_profile_selected(self, evaluator: User, profile_idx: int,
                                             sentence_idx: Optional[int] = None) -> bool:
        self.log.info(f'partner profile selected')
        user = await self._update_user_record_in_db(evaluator)
        messenger = self._messenger_for_user(user)
        if not await self._validate_user_state(user,
                                               self.UserState.EVALUATING |
                                               self.UserState.WAITING_FOR_PARTNER_EVALUATION,
                                               'Partner profile choosing is not '
                                               'allowed at the moment. Use /help '
                                               'command for usage instructions'):
            return False

        conv = self._conversations[user]
        notify_user = True
        if self.guess_profile_sentence_by_sentence:
            notify_user = await self._on_profile_sentence_selected(user, profile_idx, sentence_idx)
        else:
            self._user_states[user] = self.UserState.WAITING_FOR_PARTNER_EVALUATION
            await self.dialog_handler.select_other_peer_profile(conv.conv_id,
                                                                user,
                                                                profile_idx)
        if self._user_states[user] == self.UserState.WAITING_FOR_PARTNER_EVALUATION and notify_user:
            await messenger.send_message_to_user(user,
                                                 'Evaluation saved. Waiting for your partner to finish evaluation',
                                                 False)
        return True

    async def start_conversation(self, conversation_id: int, own_peer: User, profile: PersonProfile):
        self.log.info(f'conversation start')
        user = await self._update_user_record_in_db(own_peer)
        messenger = self._messenger_for_user(user)

        self._conversations[user] = self.ConversationRecord(conversation_id)
        self._user_states[user] = self.UserState.IN_DIALOG

        await messenger.send_message_to_user(user, "Partner found!", False)
        await messenger.send_message_to_user(user, "This is your profile. During the dialog pretend to be this person",
                                             False)
        await messenger.send_message_to_user(user, profile.description, False, keyboard_buttons=['/end', '/complain'])

    async def send_message(self, conversation_id: int, msg_id: int, msg_text: str, receiving_peer: User):
        self.log.info(f'sending message to user {receiving_peer} in conversation {conversation_id}')
        user = await self._update_user_record_in_db(receiving_peer)
        messenger = self._messenger_for_user(user)
        conv = self._conversations[user]
        external_id = await messenger.send_message_to_user(receiving_peer, msg_text, True)
        conv.message_ids_map[external_id] = msg_id

    async def start_evaluation(self, conversation_id: int, peer: User, other_peer_profile_options: List[PersonProfile],
                               other_peer_profile_correct: PersonProfile, scores_range: range):
        self.log.info(f'starting dialog evaluation {conversation_id}')
        user = await self._update_user_record_in_db(peer)
        messenger = self._messenger_for_user(user)
        conv = self._conversations[user]
        conv.opponent_profile_options = other_peer_profile_options
        conv.opponent_profile_correct = other_peer_profile_correct

        msg = 'Please evaluate the whole dialog using one of the buttons below'

        self._user_states[user] = self.UserState.EVALUATING
        await messenger.request_dialog_evaluation(user, msg, scores_range)

    async def finish_conversation(self, conversation_id: int):
        self.log.info(f'dialog {conversation_id} finished. Sending thank you message and cleaning up')
        users = [u for u, c in self._conversations.items() if c.conv_id == conversation_id]
        thanks_text = 'Dialog is finished. Thank you for participation! Save somewhere your secret conversation ID.'
        messages_to_send = []
        for user in users:
            messenger = self._messenger_for_user(user)
            messages_to_send.append(messenger.send_message_to_user(user,
                                                                   f'Your secret id: {str(conversation_id)}',
                                                                   False))
            messages_to_send.append(messenger.send_message_to_user(user,
                                                                   thanks_text,
                                                                   False,
                                                                   keyboard_buttons=['/begin', '/help']))
            del self._conversations[user]
            del self._user_states[user]
        await asyncio.gather(*messages_to_send)

    async def on_conversation_failed(self, initiator: User, reason: AbstractGateway.ConversationFailReason):
        """
        Gets called if conversation has failed to start

        :param initiator: the peer that attempted to start conversation
        :param reason: a reason why the conversation could not be started
        """
        messenger = self._messenger_for_user(initiator)
        self.log.info(f'failed to start conversation: {reason}')
        if reason == AbstractGateway.ConversationFailReason.PEER_NOT_FOUND:
            text = 'No peers found 😔\nTry again later'
        elif reason == AbstractGateway.ConversationFailReason.BANNED:
            text = 'You are banned from using the system'
        else:
            self.log.error(f'Unexpected reason: {reason}')
            text = 'INTERNAL_ERROR'
        await messenger.send_message_to_user(initiator, text, False, keyboard_buttons=['/begin', '/help'])
        if initiator in self._conversations:
            del self._conversations[initiator]
        if initiator in self._user_states:
            del self._user_states[initiator]

    async def _on_profile_sentence_selected(self, user: User, profile_idx: int,
                                            sentence_idx: Optional[int] = None) -> bool:
        conv = self._conversations[user]
        if sentence_idx is None:
            sentence_idx = conv.sentences_selected

        sentence = conv.shuffled_sentences[sentence_idx][profile_idx]
        new_choice = sentence_idx == conv.sentences_selected
        if new_choice:
            conv.sentences_selected += 1

        if conv.sentences_selected == len(conv.shuffled_sentences):
            self._user_states[user] = self.UserState.WAITING_FOR_PARTNER_EVALUATION

        await self.dialog_handler.select_other_peer_profile_sentence(conv.conv_id, user, sentence, sentence_idx)

        if conv.sentences_selected < len(conv.shuffled_sentences) and new_choice:
            await self._request_next_profile_sentence_guess(user)
        return new_choice

    async def _prepare_profile_sentences(self, user: User):
        conv = self._conversations[user]

        for i in range(len(conv.opponent_profile_correct.sentences)):
            async def get_sentence(profile: PersonProfile) -> str:
                if len(profile.sentences) > i:
                    return profile.sentences[i]

                def get_random_sentence(idx: int) -> str:
                    query = f'sentences__{idx}__exists'
                    profiles = PersonProfile.objects(**{query: True})
                    count = profiles.count()

                    return profiles[randrange(count)].sentences[idx]

                return await run_sync_in_executor(get_random_sentence, i)

            sentences = await asyncio.gather(*map(get_sentence, conv.opponent_profile_options))
            shuffle(sentences)
            conv.shuffled_sentences.append(tuple(sentences))

    async def _request_next_profile_sentence_guess(self, user: User):
        messenger = self._messenger_for_user(user)
        conv = self._conversations[user]

        msg = f'Which one of these sentences describes your partner better ' \
              f'({conv.sentences_selected + 1}/{len(conv.shuffled_sentences)})?'

        sentences = conv.shuffled_sentences[conv.sentences_selected]
        await messenger.request_profile_selection(user, msg, list(sentences), conv.sentences_selected)

    def _messenger_for_user(self, user: User):
        if user.user_key.platform in self._messengers:
            return self._messengers[user.user_key.platform]
        supported = list(filter(lambda x: x.supports_platform(user.user_key.platform), self._messengers.values()))
        if len(supported) > 1:
            self.log.warning(f'more than 1 messenger capable of handling {user.user_key.platform} platform was found')
        if len(supported) > 0:
            return supported[0]
        raise ValueError(f'No messengers capable of handling {user.user_key.platform} platform were found')

    @staticmethod
    async def _update_user_record_in_db(user: User) -> User:
        db_records = await run_sync_in_executor(User.objects, user_key=user.user_key)
        if (await run_sync_in_executor(db_records.count)) == 0:
            return await run_sync_in_executor(user.save)
        else:
            res = await run_sync_in_executor(lambda: db_records[0])
            if user.username:
                res.username = user.username
            return await run_sync_in_executor(res.save)

    async def _validate_user_state(self, user: User, ok_states: UserState, error_message: str = '') -> bool:
        state = self._user_states[user]
        result = bool(state & ok_states)

        if not result:
            self.log.info(f'invalid state for user {user}. Expected {ok_states} but got {state} instead')

        if state == self.UserState.IDLE:
            del self._user_states[user]
        if error_message and not result:
            messenger = self._messenger_for_user(user)
            await messenger.send_message_to_user(user, error_message, False)
        return result


class BotsGateway(AbstractGateway):
    class TrigramsStorage:
        bad_messages_in_a_row: int
        trigrams: Set[str]

        def __init__(self, profile_description: str):
            self.trigrams = self._text_to_trigrams(profile_description)
            self.bad_messages_in_a_row = 0

        def is_message_ok(self, text):
            text_trigrams = self._text_to_trigrams(text)
            intersection = text_trigrams & self.trigrams

            return len(intersection) == 0

        @staticmethod
        def _text_to_trigrams(text: str) -> Set[str]:
            preprocessed_text = re.sub(r'\W+', ' ', text).lower()
            words = preprocessed_text.split(' ')
            return {tuple(words[i:i + 3]) for i in range(len(words) - 3)}

    _active_chats_trigrams: DefaultDict[int, Dict[str, TrigramsStorage]]
    _n_trigrams_from_profile_threshold: int
    _bot_queues: Dict[str, asyncio.Queue]

    def __init__(self, n_bad_messages_threshold: int):
        super().__init__()
        self._n_bad_messages_threshold = n_bad_messages_threshold
        self._bot_queues = {}
        self._active_chats_trigrams = defaultdict(dict)

    async def start_conversation(self, conversation_id: int, own_peer: Bot, profile: PersonProfile):
        self.log.info(f'conversation {conversation_id} started with bot {own_peer.token}')
        bot = await self._get_bot(own_peer.token)
        q = self._get_queue(bot)
        self._active_chats_trigrams[conversation_id][bot.token] = self.TrigramsStorage(profile.description)
        msg = self._get_message_dict(f'/start\n{profile.description}',
                                     datetime.utcnow(),
                                     conversation_id,
                                     0)
        q.put_nowait(msg)

    async def send_message(self, conversation_id: int, msg_id: int, msg_text: str, receiving_peer: Bot):
        self.log.info(f'sending message to bot {receiving_peer.token} in conversation {conversation_id}')
        bot = await self._get_bot(receiving_peer.token)
        q = self._get_queue(bot)
        msg = self._get_message_dict(msg_text,
                                     datetime.utcnow(),
                                     conversation_id,
                                     msg_id)
        q.put_nowait(msg)

    async def start_evaluation(self, conversation_id: int, peer: Bot, other_peer_profile_options: List[PersonProfile],
                               other_peer_profile_correct: PersonProfile, scores_range: range):
        self.log.info(f'sending chat evaluation request to bot {peer.token} in conversation {conversation_id}')
        bot = await self._get_bot(peer.token)
        q = self._get_queue(bot)
        profiles_desc = '\n'.join([f'/profile_{i}\n{p.description}' for i, p in enumerate(other_peer_profile_options)])
        q.put_nowait(self._get_message_dict(f'/end {scores_range.start} {scores_range.stop - 1}\n{profiles_desc}',
                                            datetime.utcnow(),
                                            conversation_id,
                                            10 ** 6))

    async def finish_conversation(self, conversation_id: int):
        del self._active_chats_trigrams[conversation_id]

    async def get_updates(self, token: str, timeout: int = None, limit: int = None) -> List[Dict]:
        timeout = timeout or 0
        limit = limit or 100

        self.log.info(f'getUpdates {token}')
        bot = await self._get_bot(token)
        q = self._get_queue(bot)
        messages = []
        limit = min(100, max(limit, 1))

        try:
            messages.append(await asyncio.wait_for(q.get(), timeout))
            for _ in range(limit - 1):
                messages.append(q.get_nowait())
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            pass  # The queue is either empty or has fewer than 'limit' entries. In either case simply returning what
            # we've got so far
        updates = [{"update_id": bot.last_update_id + i, "message": msg} for i, msg in enumerate(messages)]

        bot.last_update_id += len(messages)
        await run_sync_in_executor(bot.save)
        return updates

    async def on_message_received(self, token: str, chat_id: int, msg_raw: str) -> dict:
        bot = await self._get_bot(token)
        msg_data = json.loads(msg_raw)
        text = msg_data['text']
        date = datetime.utcnow()

        if text == '/end':
            msg_id = 10 ** 6
            await self.dialog_handler.trigger_dialog_end(chat_id, bot)

            score = None
            profile_idx = None

            if 'evaluation' in msg_data:
                evaluation = msg_data['evaluation']
                if not isinstance(evaluation, dict):
                    evaluation = {}

                score = evaluation['score'] if 'score' in evaluation else None
                profile_idx = evaluation['profile_idx'] if 'profile_idx' in evaluation else None

            await self.dialog_handler.evaluate_dialog(chat_id, bot, score)
            await self.dialog_handler.select_other_peer_profile(chat_id, bot, profile_idx)
        else:
            await self._validate_trigrams(text, chat_id, bot)
            msg_id = await self.dialog_handler.on_message_received(chat_id, bot, text, date)

            if 'msg_evaluation' in msg_data:
                if isinstance(msg_data['msg_evaluation'], dict):
                    score = msg_data['msg_evaluation']['score']
                    evaluated_msg_id = msg_data['msg_evaluation']['message_id']
                else:
                    score = int(msg_data['msg_evaluation'])
                    evaluated_msg_id = None
                await self.dialog_handler.on_message_evaluated(chat_id,
                                                               bot,
                                                               score,
                                                               evaluated_msg_id)

        return self._get_message_dict(msg_raw, date, chat_id, msg_id)

    async def _validate_trigrams(self, msg: str, chat_id: int, bot: Bot):
        storage = self._active_chats_trigrams[chat_id][bot.token]
        if not storage.is_message_ok(msg):
            storage.bad_messages_in_a_row += 1
            if storage.bad_messages_in_a_row >= self._n_bad_messages_threshold:
                self.log.info(f'Profile 3-gram detected in a bot {bot.token} message for '
                              f'{storage.bad_messages_in_a_row} times in a row. Threshold reached. Finishing '
                              f'conversation')
                await self.dialog_handler.trigger_dialog_end(chat_id, bot)
            else:
                self.log.info(f'Profile 3-gram detected in a bot {bot.token} message for '
                              f'{storage.bad_messages_in_a_row} times in a row')

            raise ProfileTrigramDetectedInMessageError
        else:
            storage.bad_messages_in_a_row = 0

    @staticmethod
    async def _get_bot(token: str) -> Bot:
        db_bot = await run_sync_in_executor(Bot.objects.with_id, token)
        if db_bot is None:
            raise BotNotRegisteredError
        return db_bot

    def _get_queue(self, bot: Bot) -> asyncio.Queue:
        if bot.token not in self._bot_queues:
            self._bot_queues[bot.token] = asyncio.Queue()
        return self._bot_queues[bot.token]

    @staticmethod
    def _get_message_dict(text: str, date: datetime, conv_id: int, msg_id: int) -> dict:
        return {
            "message_id": msg_id,
            "from": {
                "id": conv_id,
                "is_bot": True,
                "first_name": f"{msg_id}"
            },
            "chat": {
                "id": conv_id,
                "first_name": f"{msg_id}",
                "type": "private"
            },
            "date": calendar.timegm(date.utctimetuple()),
            "text": text
        }
