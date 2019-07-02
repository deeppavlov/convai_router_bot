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
from convai.messages_wrapper import MessagesWrapper
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
    async def switch_to_next_topic(self, conversation_id: int, peer: User) -> bool:
        """
        Should be called when a switch to next conversation topic is requested for one of the peers

        :param conversation_id: integer id of the dialog
        :param peer: a peer willing to switch conversation topic
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
    async def start_conversation(self, conversation_id: int, own_peer: Union[User, Bot], profile: PersonProfile,
                                 peer_conversation_guid: str):
        """
        Handles the start of the conversation

        :param conversation_id: unique id of the conversation
        :param own_peer: User or Bot participating in a talk
        :param profile: a profile to "role-play" during this conversation
        :param peer_conversation_guid: unique key which identifies User-Conversation pair
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

    async def switch_to_next_topic(self, conversation_id: int, peer: User) -> bool:
        return False

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
        peer_conversation_guid: str

        def __init__(self, conv_id, peer_conversation_guid):
            self.conv_id = conv_id
            self.peer_conversation_guid = peer_conversation_guid
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
        WAITING_FOR_BOT_TOKEN = enum.auto()

    _messengers: Dict[str, AbstractMessenger]
    _conversations: Dict[User, ConversationRecord]
    _user_states: DefaultDict[User, UserState]
    guess_profile_sentence_by_sentence: bool

    def __init__(self, dialog_options: dict, evaluation_options: dict, messages: MessagesWrapper, keyboards: dict):
        super().__init__()
        self._messengers = {}
        self._conversations = {}
        self._user_states = defaultdict(lambda: self.UserState.IDLE)

        self.dialog_options = dialog_options
        self.evaluation_options = evaluation_options

        self.guess_profile_sentence_by_sentence = evaluation_options['guess_profile_sentence_by_sentence']
        self.allow_set_bot = dialog_options['allow_set_bot']
        self.reveal_dialog_id = dialog_options['reveal_dialog_id']

        self.messages = messages
        self.keyboards = keyboards

    def add_messengers(self, *messengers: AbstractMessenger):
        self._messengers.update({m.platform: m for m in messengers if isinstance(m, AbstractMessenger)})

    async def on_begin(self, user: User):
        self.log.info(f'dialog begin requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.IDLE,
                                               self.messages('start_conversation_can_not')):
            return

        if user.banned:
            await self.on_conversation_failed(user, AbstractGateway.ConversationFailReason.BANNED)
            return

        self._user_states[user] = self.UserState.IN_LOBBY

        wait_txt = self.messages('start_conversation_searching_for_peer')
        await asyncio.gather(messenger.send_message_to_user(user, wait_txt, False),
                             self.dialog_handler.on_human_initiated_dialog(user))

    async def on_help(self, user: User):
        self.log.info('help requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)
        help_txt = self.messages('help')
        if messenger.messenger_specific_help:
            help_txt += '\n\n' + messenger.messenger_specific_help
        await messenger.send_message_to_user(user, help_txt, False)

    async def on_get_started(self, user: User):
        self.log.info(f'welcome message requested')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)
        welcome_txt = self.messages('start')

        await messenger.send_message_to_user(user, welcome_txt, False, keyboard_buttons=self.keyboards['idle'])

    async def on_complain(self, user: User):
        self.log.info(f'user complained')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.IN_DIALOG |
                                               self.UserState.EVALUATING |
                                               self.UserState.WAITING_FOR_PARTNER_EVALUATION,
                                               self.messages('complaining_not_available')):
            return
        conv = self._conversations[user]
        result = await self.dialog_handler.complain(conv.conv_id, user)
        info_txt = self.messages('complaining_success')
        fail_msg = self.messages('complaining_fail')

        await messenger.send_message_to_user(user,
                                             info_txt if result else fail_msg,
                                             False)

    async def on_topic_switch(self, user: User):
        self.log.info(f'user informed about conversation topic switch')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.IN_DIALOG,
                                               self.messages('not_in_conversation_unexpected_message')):
            return

        else:
            conv = self._conversations[user]

        if not self.dialog_options['show_topics']:
            await messenger.send_message_to_user(user, self.messages('switch_topic_not_allowed'), False)
            return

        if not await self.dialog_handler.switch_to_next_topic(conv.conv_id, user):
            await messenger.send_message_to_user(user, self.messages('switch_topic_not_available'), False)
            return

    async def on_topic_switched(self, user: User, topic_text: str):
        self.log.info(f'user informed about conversation topic switch')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)
        await messenger.send_message_to_user(user, self.messages('switch_topic_info', topic_text), False)

    async def on_enter_set_bot(self, user: User):
        self.log.info(f'user requested for setting bot for conversation')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.IDLE,
                                               self.messages('bot_setting_not_available')):
            return

        if self.allow_set_bot:
            self._user_states[user] = self.UserState.WAITING_FOR_BOT_TOKEN
            bot_name = user.assigned_test_bot.bot_name if user.assigned_test_bot else 'NONE'
            set_bot_txt = self.messages('bot_setting_enter_token', bot_name)
        else:
            set_bot_txt = self.messages('bot_setting_not_allowed')

        await messenger.send_message_to_user(user, set_bot_txt, False, keyboard_buttons=self.keyboards['set_bot'])

    async def on_set_bot(self, user: User, bot_token: str):
        self.log.info(f'user entered bot token')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if await self._validate_user_state(user, self.UserState.WAITING_FOR_BOT_TOKEN):
            bot_token = bot_token.strip()
            bot = Bot.objects.with_id(bot_token)

            if bot:
                user.update(assigned_test_bot=bot)
                set_bot_txt = self.messages('bot_setting_bot_was_set', bot.bot_name)
                keyboard_buttons = self.keyboards['idle']
                self._user_states[user] = self.UserState.IDLE
            else:
                set_bot_txt = self.messages('bot_setting_bot_was_not_found', bot_token)
                keyboard_buttons = self.keyboards['set_bot']

            await messenger.send_message_to_user(user, set_bot_txt, False, keyboard_buttons=keyboard_buttons)
        else:
            await messenger.send_message_to_user(user, self.messages('bot_setting_not_in_set_bot'), False)

        return True

    async def on_list_bot(self, user: User):
        self.log.info(f'user requested for listing bots available for setting')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if await self._validate_user_state(user, self.UserState.WAITING_FOR_BOT_TOKEN):
            bots = Bot.objects(banned=False)
            bot_names = [bot.bot_name for bot in bots]
            bot_tokens = [bot.token for bot in bots]
            await messenger.list_bots(user, bot_names, bot_tokens)
        else:
            await messenger.send_message_to_user(user, self.messages('bot_setting_not_in_set_bot'), False)

        return

    async def on_unset_bot(self, user: User):
        self.log.info(f'user requested for unsetting bot for conversation')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if await self._validate_user_state(user, self.UserState.WAITING_FOR_BOT_TOKEN):
            user.update(assigned_test_bot=None)
            await messenger.send_message_to_user(user,
                                                 self.messages('bot_setting_bot_was_unset'),
                                                 False,
                                                 keyboard_buttons=self.keyboards['idle'])
            self._user_states[user] = self.UserState.IDLE
        else:
            await messenger.send_message_to_user(user, self.messages('bot_setting_not_in_set_bot'), False)

        return

    async def on_cancel_set_bot(self, user: User):
        self.log.info(f'user requested for bot setting mode exit')
        user = await self._update_user_record_in_db(user)
        messenger = self._messenger_for_user(user)

        if await self._validate_user_state(user, self.UserState.WAITING_FOR_BOT_TOKEN):
            await messenger.send_message_to_user(user,
                                                 self.messages('bot_setting_canceled'),
                                                 False,
                                                 keyboard_buttons=self.keyboards['idle'])
            self._user_states[user] = self.UserState.IDLE
        else:
            await messenger.send_message_to_user(user, self.messages('bot_setting_not_in_set_bot'), False)

        return

    async def on_message_received(self, sender: User, text: str, time: datetime, msg_id: str = None):
        self.log.info(f'message received')
        user = await self._update_user_record_in_db(sender)

        # Bot setting mode: bot token handling
        if await self._validate_user_state(user, self.UserState.WAITING_FOR_BOT_TOKEN):
            await self.on_set_bot(user, text)
            return

        if not await self._validate_user_state(user,
                                               self.UserState.IN_DIALOG,
                                               self.messages('not_in_conversation_unexpected_message')):
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
        if not await self._validate_user_state(user, self.UserState.IN_DIALOG, self.messages('not_in_dialog')):
            return

        conv = self._conversations[user]
        await self.dialog_handler.trigger_dialog_end(conv.conv_id, user)

    async def on_evaluate_dialog(self, evaluator: User, score: Optional[int]) -> bool:
        self.log.info(f'dialog evaluated')
        user = await self._update_user_record_in_db(evaluator)
        messenger = self._messenger_for_user(user)
        if not await self._validate_user_state(user,
                                               self.UserState.EVALUATING,
                                               self.messages('evaluation_not_allowed')):
            return False

        conv = self._conversations[user]
        await self.dialog_handler.evaluate_dialog(conv.conv_id, user, score)

        if self.dialog_options['assign_profile'] and self.evaluation_options['guess_profile']:
            if self.guess_profile_sentence_by_sentence:
                if not conv.shuffled_sentences:
                    await self._prepare_profile_sentences(user)
                    await self._request_next_profile_sentence_guess(user)
            else:
                msg = self.messages('profile_selection_invitation')
                await messenger.request_profile_selection(user,
                                                          msg,
                                                          [x.description for x in conv.opponent_profile_options])
        else:
            if self.reveal_dialog_id:
                peer_conversation_guid = self._conversations[user].peer_conversation_guid
                await messenger.send_message_to_user(user,
                                                     self.messages('evaluation_saved_show_id',
                                                                   peer_conversation_guid),
                                                     False)
            else:
                await messenger.send_message_to_user(user, self.messages('evaluation_saved'), False)

        return True

    async def on_other_peer_profile_selected(self, evaluator: User, profile_idx: int,
                                             sentence_idx: Optional[int] = None) -> bool:
        self.log.info(f'partner profile selected')
        user = await self._update_user_record_in_db(evaluator)
        messenger = self._messenger_for_user(user)

        if not await self._validate_user_state(user,
                                               self.UserState.EVALUATING |
                                               self.UserState.WAITING_FOR_PARTNER_EVALUATION,
                                               self.messages('profile_selection_not_allowed')):
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
            if self.reveal_dialog_id:
                peer_conversation_guid = self._conversations[user].peer_conversation_guid
                await messenger.send_message_to_user(user,
                                                     self.messages('evaluation_saved_show_id', peer_conversation_guid),
                                                     False)
            else:
                await messenger.send_message_to_user(user, self.messages('evaluation_saved'), False)

        return True

    async def start_conversation(self, conversation_id: int, own_peer: User, profile: PersonProfile,
                                 peer_conversation_guid: str):

        self.log.info('conversation start')
        user = await self._update_user_record_in_db(own_peer)
        messenger = self._messenger_for_user(user)

        self._conversations[user] = self.ConversationRecord(conversation_id, peer_conversation_guid)
        self._user_states[user] = self.UserState.IN_DIALOG

        if self.dialog_options['assign_profile']:
            await messenger.send_message_to_user(user, self.messages('start_conversation_peer_found'), False)
            await messenger.send_message_to_user(user, self.messages('start_conversation_profile_assigning'), False)
            await messenger.send_message_to_user(user, profile.description, False,
                                                 keyboard_buttons=self.keyboards['in_dialog'])
        else:
            await messenger.send_message_to_user(user, self.messages('start_conversation_peer_found'), False,
                                                 keyboard_buttons=self.keyboards['in_dialog'])

        if self.dialog_options['show_topics'] and profile.topics:
            await self.on_topic_switched(user, profile.topics[0])

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

        msg = self.messages('evaluation_start')

        self._user_states[user] = self.UserState.EVALUATING

        if self.evaluation_options['score_dialog']:
            await messenger.request_dialog_evaluation(user, msg, scores_range)
        else:
            await self.on_evaluate_dialog(user, None)

    async def finish_conversation(self, conversation_id: int):
        self.log.info(f'dialog {conversation_id} finished. Sending thank you message and cleaning up')
        users = [u for u, c in self._conversations.items() if c.conv_id == conversation_id]
        thanks_text = self.messages('finish_conversation')
        messages_to_send = []

        for user in users:
            messenger = self._messenger_for_user(user)

            if self.reveal_dialog_id:
                peer_conversation_guid = self._conversations[user].peer_conversation_guid
                msg = self.messages('finish_conversation_show_id', peer_conversation_guid)
                messages_to_send.append(messenger.send_message_to_user(user, msg, False))

            messages_to_send.append(messenger.send_message_to_user(user, thanks_text, False,
                                                                   keyboard_buttons=self.keyboards['idle']))

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
            text = self.messages('failed_conversation_no_peers')
        elif reason == AbstractGateway.ConversationFailReason.BANNED:
            text = self.messages('failed_conversation_banned')
        else:
            self.log.error(f'Unexpected reason: {reason}')
            text = self.messages('error')

        await messenger.send_message_to_user(initiator, text, False, keyboard_buttons=self.keyboards['idle'])

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

        msg = self.messages('profile_selection_sentences_selection',
                            conv.sentences_selected + 1,
                            len(conv.shuffled_sentences))

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
            n_gr = 5
            if len(words) <= n_gr:
                n_gr_set = {tuple(words)}
            else:
                n_gr_set = {tuple(words[i:i + n_gr]) for i in range(len(words) - n_gr)}
            return n_gr_set

    _active_chats_trigrams: DefaultDict[int, Dict[str, TrigramsStorage]]
    _n_trigrams_from_profile_threshold: int
    _bot_queues: Dict[str, asyncio.Queue]

    def __init__(self, dialog_options: dict):
        super().__init__()
        self._n_bad_messages_threshold = dialog_options['n_bad_messages_in_a_row_threshold']
        self._bot_queues = {}
        self._active_chats_trigrams = defaultdict(dict)

    async def start_conversation(self, conversation_id: int, own_peer: Bot, profile: PersonProfile,
                                 peer_conversation_guid: str):

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
            if self._n_bad_messages_threshold > 0:
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
