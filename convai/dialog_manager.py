import asyncio
import enum
import itertools
import logging
import random
from uuid import uuid4
from datetime import datetime, timedelta
from numbers import Number
from typing import Dict, Union, List, Optional

from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import BaseScheduler
from mongoengine import ValidationError, FieldDoesNotExist, QuerySet

from convai import run_sync_in_executor
from convai.conversation_gateways import AbstractGateway, AbstractDialogHandler, HumansGateway, BotsGateway
from convai.exceptions import UserBannedError, SimultaneousDialogsError
from model import User, Bot, BannedPair, Conversation, ConversationPeer, PersonProfile, Message, Complaint

log = logging.getLogger(__name__)


class DialogManager(AbstractDialogHandler):
    class EvaluationState(enum.Flag):
        NONE = 0
        SCORE_GIVEN = enum.auto()
        PROFILE_SELECTED = enum.auto()
        COMPLETE = SCORE_GIVEN | PROFILE_SELECTED

    _evaluations: Dict[int, List[EvaluationState]]
    _dialog_timeout_handlers: Dict[int, Job]
    _active_dialogs: Dict[int, Conversation]
    _lobby: Dict[User, Job]
    humans_gateway: HumansGateway
    bots_gateway: BotsGateway
    inactivity_timeout: Number
    max_time_in_lobby: Number
    length_threshold: int
    human_bot_ratio: float
    dialog_eval_min: int
    dialog_eval_max: int

    def __init__(self, bots_gateway: BotsGateway, humans_gateway: HumansGateway, dialog_options: dict,
                 evaluation_options: dict, scheduler: BaseScheduler = None):
        """
        Dialog manager is responsible for handling conversations. Including matching with human and bot peers, dialog 
        setup, dialog evaluation, etc.

        :param bots_gateway: An object capable of handling system-to-bot communication
        :param humans_gateway: An object capable of handling system-to-human communication
        :param dialog_options: dialog options
        :param evaluation_options: dialog evaluation options
        :param scheduler: custom non-blocking scheduler object which conforms to the interface of
            apscheduler.schedulers.base.BaseScheduler. Default value is BackgroundScheduler().
        """
        self.humans_gateway = humans_gateway
        self.bots_gateway = bots_gateway
        self.scheduler = scheduler if scheduler is not None else AsyncIOScheduler()

        self.dialog_options = dialog_options
        self.evaluation_options = evaluation_options

        self.dialog_eval_min = evaluation_options['evaluation_score_from']
        self.dialog_eval_max = evaluation_options['evaluation_score_to']
        self.length_threshold = dialog_options['max_length']
        self.inactivity_timeout = dialog_options['inactivity_timeout']
        self.human_bot_ratio = dialog_options['human_bot_ratio']
        self.max_time_in_lobby = dialog_options['max_time_in_lobby']

        self._lobby = {}
        self._active_dialogs = {}
        self._evaluations = {}
        self._dialog_timeout_handlers = {}

        self.scheduler.start()

    async def on_human_initiated_dialog(self, user: User):
        log.info(f'human initiated dialog: {user.user_key}')
        if user.banned:
            raise UserBannedError("Banned users are not allowed to start dialogs")
        active_dialogs_peers = itertools.chain(*map(lambda c: (c.participant1, c.participant2),
                                                    self._active_dialogs.values()))
        if user in self._lobby or user in map(lambda p: p.peer, active_dialogs_peers):
            raise SimultaneousDialogsError("Starting multiple dialogs simultaneously is prohibited")

        if random.random() >= self.human_bot_ratio:
            log.info(f'bot peer selected')
            await self._start_dialog_with_bot(user)
            return
        if await self._try_start_dialog_with_human(user):
            return  # Human peer has been found in the lobby and the dialog started right away
        log.info(f'no humans found in lobby. Setting up the timer')

        # No waiting human peers are available. Putting user in lobby for "max_time_in_lobby" seconds and starting
        # the dialog with bot in the case of timeout
        event = self._schedule(self.max_time_in_lobby, self._start_dialog_with_bot, argument=(user,))
        self._lobby[user] = event

    async def on_message_received(self, conversation_id: int, sender: Union[Bot, User], text: str,
                                  time: datetime) -> int:
        log.info(f'message received for conversation {conversation_id}')
        self._validate_conversation_and_peer(conversation_id, sender)
        conversation = self._active_dialogs[conversation_id]

        if conversation_id in self._evaluations:
            raise ValueError('Conversation is finished. Only evaluation is allowed')

        msg = Message(msg_id=len(conversation.messages),
                      text=text,
                      sender=sender,
                      time=time)

        conversation.messages.append(msg)

        receiver = next((p.peer for p in conversation.participants if p.peer != sender), None)
        if receiver is None:
            raise RuntimeError('Could not find a receiver for the message')

        await self._gateway_for_peer(receiver).send_message(conversation_id, msg.msg_id, msg.text, receiver)

        if len(conversation.messages) >= self.length_threshold:
            log.info(f'conversation length threshold reached. Finishing the conversation')
            await self.trigger_dialog_end(conversation_id, sender)
        else:
            self._reset_inactivity_timer(conversation_id)

        return msg.msg_id

    async def on_message_evaluated(self, conversation_id: int, evaluator: Union[Bot, User], score: int,
                                   msg_id: int = None):
        log.info(f'message evaluated in conversation {conversation_id}')
        self._validate_conversation_and_peer(conversation_id, evaluator)
        conversation = self._active_dialogs[conversation_id]
        if score > 1 or score < 0:
            raise ValueError('Score should be within a range [0, 1]')

        if msg_id is None:
            msg_id = next((m.msg_id for m in conversation.messages[::-1] if m.sender != evaluator), -1)
        msg = next((m for m in conversation.messages if m.msg_id == msg_id), None)

        if msg is None:
            raise ValueError('Could not find a message with id {}'.format(msg_id))

        if msg.sender == evaluator:
            raise ValueError('Could not find evaluate own messages')

        msg.evaluation_score = score

    async def switch_to_next_topic(self, conversation_id: int, peer: User) -> bool:
        log.info('switching to the next conversation topic')
        self._validate_conversation_and_peer(conversation_id, peer)
        conversation: Conversation = self._active_dialogs[conversation_id]

        if conversation.next_topic():
            index = conversation.active_topic_index
            msg = Message(msg_id=len(conversation.messages),
                          text=f'Switched to topic with index {index}',
                          sender=peer,
                          time=datetime.now(),
                          system=True)

            conversation.messages.append(msg)

            for conv_peer in conversation.participants:
                await self._gateway_for_peer(conv_peer.peer).on_topic_switched(conv_peer.peer,
                                                                               conv_peer.assigned_profile.topics[index])

    async def trigger_dialog_end(self, conversation_id: int, peer: Union[Bot, User]):
        log.info(f'end of conversation {conversation_id} triggered')
        self._validate_conversation_and_peer(conversation_id, peer)

        conversation = self._active_dialogs[conversation_id]

        for participant in conversation.participants:
            if participant.peer == peer:
                participant.triggered_dialog_end = True

        await self._initiate_final_evaluation(conversation_id)

    async def evaluate_dialog(self, conversation_id: int, evaluator: Union[User, Bot], score: Optional[int]):
        log.info(f'conversation {conversation_id} evaluated')
        self._validate_conversation_and_peer(conversation_id, evaluator)
        conversation = self._active_dialogs[conversation_id]

        if conversation_id not in self._evaluations:
            raise ValueError('Conversation is not finished yet')

        peer_idx = 0 if conversation.participant1.peer == evaluator else 1

        if score is not None:
            if score > self.dialog_eval_max or score < self.dialog_eval_min:
                raise ValueError('Score should be within a range [{}, {}]'.format(self.dialog_eval_min,
                                                                                  self.dialog_eval_max))

            conversation_peer = next((p for p in conversation.participants if p.peer == evaluator))

            conversation_peer.dialog_evaluation_score = score

        self._evaluations[conversation_id][peer_idx] |= self.EvaluationState.SCORE_GIVEN

        if not self.dialog_options['assign_profile'] or not self.evaluation_options['guess_profile']:
            self._evaluations[conversation_id][peer_idx] |= self.EvaluationState.PROFILE_SELECTED

        await self._handle_evaluation_state(conversation_id)

    async def select_other_peer_profile(self, conversation_id: int, evaluator: Union[User, Bot],
                                        profile_idx: Optional[int]):
        log.info(f'partner profile selected in conversation {conversation_id}')
        self._validate_conversation_and_peer(conversation_id, evaluator)
        conversation = self._active_dialogs[conversation_id]

        if conversation_id not in self._evaluations:
            raise ValueError('Conversation is not finished yet')

        peer_idx = 0 if conversation.participants[0].peer == evaluator else 1

        evaluator_peer = next((p for p in conversation.participants if p.peer == evaluator))

        if profile_idx is not None:
            if profile_idx < 0 or profile_idx >= len(evaluator_peer.other_peer_profile_options):
                raise ValueError('Selected profile was not an option')

            evaluator_peer.other_peer_profile_selected = evaluator_peer.other_peer_profile_options[profile_idx]

        self._evaluations[conversation_id][peer_idx] |= self.EvaluationState.PROFILE_SELECTED
        await self._handle_evaluation_state(conversation_id)

    async def select_other_peer_profile_sentence(self, conversation_id: int, evaluator: Union[User, Bot],
                                                 sentence: str, sentence_idx: Optional[int] = None):
        log.info(f'partner profile sentence selected in conversation {conversation_id}')
        self._validate_conversation_and_peer(conversation_id, evaluator)
        conversation = self._active_dialogs[conversation_id]

        if conversation_id not in self._evaluations:
            raise ValueError('Conversation is not finished yet')

        peer_idx = 0 if conversation.participants[0].peer == evaluator else 1

        evaluator_peer = next((p for p in conversation.participants if p.peer == evaluator))
        other_peer = next((p for p in conversation.participants if p.peer != evaluator))

        if sentence_idx is None:
            sentence_idx = len(evaluator_peer.other_peer_profile_selected_parts)

        nones_to_append = sentence_idx - len(evaluator_peer.other_peer_profile_selected_parts) + 1

        evaluator_peer.other_peer_profile_selected_parts += [None] * nones_to_append
        evaluator_peer.other_peer_profile_selected_parts[sentence_idx] = sentence

        selected_parts = [x for x in evaluator_peer.other_peer_profile_selected_parts if x is not None]

        if len(selected_parts) == len(other_peer.assigned_profile.sentences):
            self._evaluations[conversation_id][peer_idx] |= self.EvaluationState.PROFILE_SELECTED
            await self._handle_evaluation_state(conversation_id)

    async def complain(self, conversation_id: int, complainer: User) -> bool:
        log.info(f'complaint about conversation {conversation_id}')
        self._validate_conversation_and_peer(conversation_id, complainer)
        conversation = self._active_dialogs[conversation_id]

        if len(conversation.messages) == 0:
            return False

        complain_to = [x.peer for x in conversation.participants if x.peer != complainer][0]

        complaint = Complaint(complainer=complainer,
                              complain_to=complain_to,
                              conversation=conversation)

        def save_complaint_and_dialog():
            conversation.save()
            complaint.save()

        await run_sync_in_executor(save_complaint_and_dialog)
        return True

    async def _handle_evaluation_state(self, conversation_id: int):
        conversation = self._active_dialogs[conversation_id]

        def check(i):
            return isinstance(conversation.participants[i].peer, Bot) or \
                   self._evaluations[conversation_id][i] == self.EvaluationState.COMPLETE

        completed = all(map(check, range(2)))

        if completed:
            await self._cleanup_conversation(conversation_id)

    def _validate_conversation_and_peer(self, conversation_id: int, peer: Union[Bot, User]):
        log.debug(f'validating conversation and peer: {conversation_id}, {peer}')
        if conversation_id not in self._active_dialogs:
            raise ValueError('There is no active conversation with id {}'.format(conversation_id))
        conversation = self._active_dialogs[conversation_id]

        if not any(map(lambda p: p.peer == peer, conversation.participants)):
            raise ValueError('Peer is not a part of the conversation')

    def _schedule(self, delay, action, argument=(), kwargs=None):
        if kwargs is None:
            kwargs = {}
        job = self.scheduler.add_job(action,
                                     'date',
                                     args=argument,
                                     kwargs=kwargs,
                                     run_date=datetime.now() + timedelta(seconds=delay))
        return job

    @staticmethod
    def _unschedule_safe(job):
        try:
            job.remove()
        except JobLookupError:
            pass

    def _unschedule_lobby_timeout(self, user: User):
        if user in self._lobby:
            self._unschedule_safe(self._lobby[user])
            del self._lobby[user]

    def _unschedule_inactivity_timer(self, conversation_id):
        if conversation_id in self._dialog_timeout_handlers:
            self._unschedule_safe(self._dialog_timeout_handlers[conversation_id])
            del self._dialog_timeout_handlers[conversation_id]

    def _reset_inactivity_timer(self, conversation_id: int):
        log.debug(f'inactivity timer reset for conversation {conversation_id}')
        self._unschedule_inactivity_timer(conversation_id)

        event = self._schedule(self.inactivity_timeout, self._handle_conversation_timeout, argument=(conversation_id,))
        self._dialog_timeout_handlers[conversation_id] = event

    async def _try_start_dialog_with_human(self, user: User):
        log.info(f'trying to start human-human dialog')
        if len(self._lobby) == 0:
            log.info(f'failed to start human-human dialog. The lobby is empty')
            return False

        log.info(f'human partner found in the lobby')

        peer = random.choice(list(self._lobby.keys()))
        self._unschedule_lobby_timeout(peer)
        await self._instantiate_dialog(user, peer)
        return True

    async def _start_dialog_with_bot(self, user: User):
        log.info(f'starting dialog with bot')
        self._unschedule_lobby_timeout(user)
        if user.assigned_test_bot:
            bots = await run_sync_in_executor(Bot.objects, banned=False, token=user.assigned_test_bot.token)
        else:
            bots = await run_sync_in_executor(Bot.objects, banned=False)
        bots_count = await run_sync_in_executor(bots.count)
        if self.bots_gateway is None or bots_count == 0:
            log.warning(f'no bots found or bots gateway is None')
            await self.humans_gateway.on_conversation_failed(user,
                                                             AbstractGateway.ConversationFailReason.PEER_NOT_FOUND)
            return
        found = False
        bot = None  # Silence PyCharm warning
        while not found:
            bot = await run_sync_in_executor(lambda: bots[random.randrange(bots_count)])
            found = (await run_sync_in_executor(lambda: BannedPair.objects(user=user, bot=bot).count())) == 0
        await self._instantiate_dialog(user, bot)

    def _gateway_for_peer(self, peer: Union[User, Bot, ConversationPeer]):
        if isinstance(peer, ConversationPeer):
            return self._gateway_for_peer(peer.peer)

        if not isinstance(peer, User) and not isinstance(peer, Bot):
            raise RuntimeError('Unexpected peer class: {}. Only {} are supported'.format(type(peer), [User, Bot]))
        return self.humans_gateway if isinstance(peer, User) else self.bots_gateway

    async def _instantiate_dialog(self, user: User, peer: Union[User, Bot]):
        log.info(f'instantiating the dialog')

        conversation = Conversation(participant1=ConversationPeer(peer=user, peer_conversation_guid=uuid4().__str__()),
                                    participant2=ConversationPeer(peer=peer, peer_conversation_guid=uuid4().__str__()))

        profiles: QuerySet = await run_sync_in_executor(PersonProfile.objects)

        first_profile = None
        linked_profile_uuid = None

        for p in conversation.participants:
            if first_profile is None:
                p.assigned_profile = first_profile = random.choice(profiles)
                linked_profile_uuid = first_profile.link_uuid

            else:
                # profiles assignment order:
                # other profile from the same linked group || profile with unmatching sentences || same profile
                second_profile = random.choice(profiles(id__ne=first_profile.id, link_uuid=linked_profile_uuid) or
                                               (profiles(sentences__ne=first_profile.sentences) or [first_profile]))

                p.assigned_profile = second_profile

        while True:
            conv_id = random.getrandbits(31)
            if conv_id not in self._active_dialogs and \
                    await run_sync_in_executor(lambda: Conversation.objects(conversation_id=conv_id).count()) == 0:
                break
        conversation.conversation_id = conv_id
        self._active_dialogs[conv_id] = conversation

        for p in conversation.participants:
            target_gateway = self._gateway_for_peer(p)
            await target_gateway.start_conversation(conv_id, p.peer, p.assigned_profile, p.peer_conversation_guid)

        self._reset_inactivity_timer(conv_id)

    async def _handle_conversation_timeout(self, conversation_id: int):
        log.info(f'dialog inactivity timeout: {conversation_id}')
        if conversation_id in self._evaluations:
            await self._cleanup_conversation(conversation_id)
        else:
            await self._initiate_final_evaluation(conversation_id)

    async def _cleanup_conversation(self, conversation_id: int):
        log.info(f'cleaning up the conversation {conversation_id}')
        conversation = self._active_dialogs[conversation_id]
        all_gateways = set(map(self._gateway_for_peer, conversation.participants))

        await asyncio.gather(*[gw.finish_conversation(conversation_id) for gw in all_gateways], return_exceptions=True)
        try:
            await run_sync_in_executor(conversation.save)
        except ValidationError:
            log.warning('Empty conversation. Not saving')

        self._unschedule_inactivity_timer(conversation_id)
        del self._active_dialogs[conversation_id]
        del self._evaluations[conversation_id]

    async def _initiate_final_evaluation(self, conversation_id: int):
        log.info(f'initiating final evaluation for conversation {conversation_id}')
        conversation = self._active_dialogs[conversation_id]
        self._reset_inactivity_timer(conversation_id)

        self._evaluations[conversation_id] = [self.EvaluationState.NONE] * 2

        db_profiles = await run_sync_in_executor(PersonProfile.objects)
        db_profiles_count = await run_sync_in_executor(db_profiles.count)

        to_await = []
        for i, p in enumerate(conversation.participants):
            true_profile: PersonProfile = conversation.participants[1 - i].assigned_profile
            random_profile: PersonProfile = random.choice(db_profiles(sentences__ne=true_profile.sentences) or
                                                          [true_profile])

            profiles = [true_profile, random_profile]
            random.shuffle(profiles)

            p.other_peer_profile_options = profiles

            to_await.append(
                self._gateway_for_peer(p.peer).start_evaluation(conversation_id,
                                                                p.peer,
                                                                profiles,
                                                                true_profile,
                                                                range(self.dialog_eval_min, self.dialog_eval_max + 1)))

        await asyncio.gather(*to_await)
