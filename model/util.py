import json
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TextIO
from uuid import uuid4

import yaml
from mongoengine import errors
from mongoengine.queryset.visitor import Q

from . import Bot, PersonProfile, User, UserPK, BannedPair, Conversation, ConversationPeer, Message, Complaint


def fill_db_with_stub(n_bots=5,
                      n_bots_banned=2,
                      n_humans=10,
                      n_humans_banned=2,
                      n_banned_pairs=3,
                      n_profiles=20,
                      n_topics=3,
                      n_conversations=20,
                      n_msg_per_conv=15,
                      n_complaints_new=3,
                      n_complaints_processed=2):
    from random import choice, randint
    with open(os.path.join(os.path.split(__file__)[0], "lorem_ipsum.txt"), 'r') as f:
        lorem_ipsum = f.read().split(' ')

    profiles = [PersonProfile(persona=[' '.join(lorem_ipsum[i * 10:(i + 1) * 10])],
                              link_uuid=str(uuid4()),
                              topics=[f'Topic_{i}' for i in range(random.randrange(n_topics + 1))]
                              ).save() for i in range(n_profiles)]
    bots = [Bot(token='stub' + str(uuid4()),
                bot_name='stub bot #' + str(i)).save() for i in range(n_bots)]
    banned_bots = [Bot(token='stub' + str(uuid4()),
                       bot_name='stub banned bot #' + str(i),
                       banned=True).save() for i in range(n_bots_banned)]
    humans = [User(user_key=UserPK(user_id='stub' + str(uuid4()),
                                   platform=choice(UserPK.PLATFORM_CHOICES)),
                   username='stub user #' + str(i)).save() for i in range(n_humans)]
    banned_humans = [User(user_key=UserPK(user_id='stub' + str(uuid4()),
                                          platform=choice(UserPK.PLATFORM_CHOICES)),
                          username='stub banned user #' + str(i),
                          banned=True).save() for i in range(n_humans_banned)]
    all_humans = humans + banned_humans
    all_bots = bots + banned_bots
    all_peers = all_humans + all_bots

    banned_pairs = []
    for _ in range(n_banned_pairs):
        while True:
            try:
                banned_pairs.append(BannedPair(user=choice(all_humans),
                                               bot=choice(all_bots)).save())
                break
            except errors.NotUniqueError:
                # retry...
                pass

    conversations = []
    for i in range(n_conversations):
        human_peer = ConversationPeer(peer=choice(all_humans),
                                      assigned_profile=choice(profiles),
                                      dialog_evaluation_score=randint(1, 5),
                                      other_peer_profile_options=[choice(profiles) for _ in range(2)])
        human_peer.other_peer_profile_selected = choice(human_peer.other_peer_profile_options)
        other_peer = ConversationPeer(peer=choice(all_peers),
                                      assigned_profile=choice(human_peer.other_peer_profile_options),
                                      dialog_evaluation_score=randint(1, 5),
                                      other_peer_profile_options=[choice(profiles)] + [human_peer.assigned_profile])
        other_peer.other_peer_profile_selected = choice(other_peer.other_peer_profile_options)
        conv = Conversation(participant1=human_peer, participant2=other_peer, conversation_id=i + 1)

        msgs = [Message(msg_id=i,
                        text=' '.join(lorem_ipsum[i * 10:(i + 1) * 10]),
                        sender=choice([human_peer.peer, other_peer.peer]),
                        time=datetime.now() + timedelta(hours=i),
                        evaluation_score=randint(0, 1)) for i in range(n_msg_per_conv)]
        conv.messages = msgs
        conversations.append(conv.save())

    complaints_new = [Complaint(complainer=c.participants[0].peer,
                                complain_to=c.participants[1].peer,
                                conversation=c).save() for c in map(lambda _: choice(conversations),
                                                                    range(n_complaints_new))]

    complaints_processed = [Complaint(complainer=c.participants[0].peer,
                                      complain_to=c.participants[1].peer,
                                      conversation=c,
                                      processed=True).save() for c in map(lambda _: choice(conversations),
                                                                          range(n_complaints_processed))]


def get_inactive_bots(n_bots, threshold=None):
    pipeline = [
        {'$match': {'participant2.peer._cls': 'Bot'}},
        {'$group': {'_id': '$participant2.peer',
                    'count': {'$sum': 1}}},
        {'$sort': {'count': 1}}
    ]

    if threshold is not None:
        pipeline.append({'$match': {'count': {'$lte': threshold}}})
    else:
        pipeline.append({'$limit': n_bots})

    ids, counts = zip(*[(group['_id']['_ref'].as_doc()['$id'], group['count'])
                        for group in Conversation.objects.aggregate(*pipeline)])

    bots = Bot.objects.in_bulk(ids)

    for id, count in zip(ids, counts):
        yield bots[id], count


def register_bot(token, name):
    return Bot(token=token,
               bot_name=name).save()


def get_complaints(include_processed=False):
    args = {'processed': False} if not include_processed else {}
    return Complaint.objects(**args)


def mark_complaints_processed(all=False, *ids):
    objects = Complaint.objects if all else Complaint.objects(id__in=ids)
    return objects.update(processed=True)


def ban_human(platform, user_id):
    return User.objects(user_key__platform=platform, user_key__user_id=user_id).update(banned=True)


def ban_bot(token):
    return Bot.objects(token=token).update(banned=True)


def ban_human_bot(platform, user_id, token):
    human = User.objects.get(user_key=UserPK(user_id=user_id, platform=platform))
    bot = Bot.objects.with_id(token)
    return BannedPair(user=human, bot=bot).save()


def set_default_bot(platform, user_id, token):
    user = User.objects.get(user_key=UserPK(user_id=user_id, platform=platform))
    bot = Bot.objects.with_id(token)
    return user.update(assigned_test_bot=bot)


def import_profiles(stream: TextIO):
    _, ext = os.path.splitext(stream.name)
    if ext == '.json':
        linked_groups = json.load(stream)
    elif ext == '.yaml' or ext == '.yml':
        linked_groups = yaml.safe_load(stream)
    else:
        raise ValueError(f'file extension "{ext}" is not supported, it should be either `json` or `yaml/yml`')
    profiles = []

    for linked_group in linked_groups:
        link_uuid = str(uuid4())

        for linked_profile in linked_group:
            profiles.append(PersonProfile(persona=linked_profile['persona'],
                                          tags=linked_profile.get('tags', []),
                                          link_uuid=link_uuid,
                                          topics=linked_profile.get('topics', [])))

    return PersonProfile.objects.insert(profiles)


def export_training_conversations(date_begin=None, date_end=None, reveal_sender=False, reveal_ids=False):
    # TODO: need to process to human conversation scenario
    # TODO: merge with export_bot_scores
    training_convs = []

    if (date_begin is None) and (date_end is None):
        date_begin = '1900-01-01'
        date_end = '2500-12-31'
    elif (date_begin is not None) and (date_end is None):
        date_end = date_begin

    datetime_begin = datetime.strptime(f'{date_begin}_00:00:00.000000', "%Y-%m-%d_%H:%M:%S.%f")
    datetime_end = datetime.strptime(f'{date_end}_23:59:59.999999', "%Y-%m-%d_%H:%M:%S.%f")
    args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end}

    convs = Conversation.objects(**args)

    for conv in convs:
        conv: Conversation = conv

        training_conv = {
            'dialog_id': str(hex(conv.conversation_id)),
            'dialog': [],
            'start_time': str(conv.start_time),
            'end_time': str(conv.end_time),
            'users': []
        }

        users = [conv.participant1, conv.participant2]
        user_map = {}
        for i in range(len(users)):
            # other user's id
            j = (i + 1) % 2
            u = users[i]
            obj = {}
            try:
                uid = str(u.peer.user_key.user_id)
                uclass = 'Human'
            except AttributeError:
                uid = str(u.peer.id)
                uclass = 'Bot'
            obj['user_class'] = uclass
            obj['user_id'] = uid
            obj['user_external_id'] = u.peer_conversation_guid
            user_map[u.peer] = (uid, uclass)

            other_profile_true = users[j].assigned_profile.persona
            if u.other_peer_profile_selected is not None:
                other_profile_hyp = u.other_peer_profile_selected.persona
            else:
                other_profile_hyp = None

            if other_profile_hyp is None:
                obj['profile_match'] = 0
            elif other_profile_hyp == other_profile_true:
                obj['profile_match'] = 1
            else:
                obj['profile_match'] = -1

            other_profile_options = [pr.persona for pr in u.other_peer_profile_options]
            ended_dialog = False
            if 'triggered_dialog_end' in u:
                if u['triggered_dialog_end']:
                    ended_dialog = True

            obj['dialog_evaluation'] = u.dialog_evaluation_score
            obj['profile'] = u.assigned_profile.persona
            obj['topics'] = u.assigned_profile.topics
            obj['other_profile_options'] = other_profile_options
            obj['ended_dialog'] = ended_dialog
            training_conv['users'].append(obj)

        for msg in conv.messages:
            msg: Message = msg
            usr = user_map[msg.sender]
            training_message = {
                #'id': msg.msg_id,
                'sender': usr[0],
                'sender_class': usr[1],
                'text': msg.text,
                'evaluation_score': msg.evaluation_score,
                'system': msg.system,
                'time': msg.time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            training_conv['dialog'].append(training_message)

        training_convs.append(training_conv)

    return training_convs


def export_bot_scores(date_begin=None, date_end=None, daily_stats=False):
    # TODO: refactor this shit with pipeline
    bot_scores = {}

    # ===== maint =====
    convs = {}

    profiles_obj = PersonProfile.objects
    profiles = {str(profile.pk): list(profile.persona) for profile in profiles_obj}

    bot_daily_stats = {}

    for bot in Bot.objects:

        if bot.banned:
            continue

        bot_id = str(bot.id)
        bot_scores[bot_id] = {}

        # ===== maint =====
        convs[bot_id] = {}

        if (date_begin is None) and (date_end is None):
            date_begin = '1900-01-01'
            date_end = '2500-12-31'
        elif (date_begin is not None) and (date_end is None):
            date_end = date_begin

        datetime_begin = datetime.strptime(f'{date_begin}_00:00:00.000000', "%Y-%m-%d_%H:%M:%S.%f")
        datetime_end = datetime.strptime(f'{date_end}_23:59:59.999999', "%Y-%m-%d_%H:%M:%S.%f")
        date_args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end}

        q_date = Q(**date_args)
        q_participant1 = Q(participant1__peer=bot)
        q_participant2 = Q(participant2__peer=bot)
        bot_convs = Conversation.objects(q_date & (q_participant1 | q_participant2))

        user_eval_scores = []
        profile_selected_scores = []
        scored_dialogs = 0
        convs_long_short = defaultdict(list)

        for bot_conv in bot_convs:
            bot_conv: Conversation = bot_conv

            bot_conv_id = str(bot_conv.id)
            num_messages = len(bot_conv.messages)
            count_as_scored = False

            conv_date = str(datetime.date(bot_conv.start_time))
            num_user_messages = 0
            num_bot_messages = 0

            if isinstance(bot_conv.participant1.peer, Bot):
                peer_bot = bot_conv.participant1
                peer_user = bot_conv.participant2
            else:
                peer_bot = bot_conv.participant2
                peer_user = bot_conv.participant1

            for message in bot_conv.messages:
                message: Message = message
                if message.sender == peer_user.peer:
                    num_user_messages += 1
                elif message.sender == peer_bot.peer:
                    num_bot_messages += 1

            long_conv = True if (num_user_messages > 3 and num_bot_messages > 3) else False

            convs_long_short[conv_date].append(long_conv)

            user_eval_score = peer_user.dialog_evaluation_score
            bot_profile = peer_bot.assigned_profile
            user_selected_profile = peer_user.other_peer_profile_selected
            user_selected_profile_parts = peer_user.other_peer_profile_selected_parts

            if user_eval_score is not None:
                eval_score_norm = (int(user_eval_score) - 1) / 4
                user_eval_scores.append(eval_score_norm)
                count_as_scored = count_as_scored | True

            if user_selected_profile is not None:
                profile_selected_score = int(user_selected_profile == bot_profile)
                profile_selected_scores.append(profile_selected_score)
                count_as_scored = count_as_scored | True
            elif len(user_selected_profile_parts) > 0:
                profile_set = set(list(bot_profile.persona))
                selected_set = set(list(user_selected_profile_parts))
                matched_set = profile_set.intersection(selected_set)

                profile_selected_score = len(matched_set) / len(profile_set)
                profile_selected_scores.append(profile_selected_score)
                count_as_scored = count_as_scored | True
            else:
                profile_selected_score = None

            scored_dialogs = scored_dialogs + (int(count_as_scored))

            # ===== maint =====
            convs[bot_id][bot_conv_id] = {
                'user_eval_score': user_eval_score,
                'profile_selected_score': profile_selected_score,
                'profile_set': list(bot_profile.persona),
                'selected_set': list(user_selected_profile_parts),
                'num_messages': num_messages
                }

        daily_statistics = {}

        for date, daily_convs_log_short in convs_long_short.items():
            daily_statistics[date] = {}
            daily_statistics[date]['dialogs_total'] = len(daily_convs_log_short)
            daily_statistics[date]['dialogs_long'] = len([conv for conv in daily_convs_log_short if conv])
            daily_statistics[date]['dialogs_short'] = len([conv for conv in daily_convs_log_short if not conv])

        bot_daily_stats[bot_id] = daily_statistics

        if daily_stats:
            bot_scores[bot_id]['daily_statistics'] = bot_daily_stats[bot_id]

        bot_scores[bot_id]['user_eval_score'] = 0 if len(user_eval_scores) == 0 else \
            sum(user_eval_scores) / len(user_eval_scores)
        bot_scores[bot_id]['profile_selected_score'] = 0 if len(profile_selected_scores) == 0 else \
            sum(profile_selected_scores) / len(profile_selected_scores)
        bot_scores[bot_id]['scored_dialogs'] = scored_dialogs

        bot_scores[bot_id]['dialogs_total'] = sum([daily_statistics[date]['dialogs_total']
                                                   for date in daily_statistics.keys()])
        bot_scores[bot_id]['dialogs_long'] = sum([daily_statistics[date]['dialogs_long']
                                                  for date in daily_statistics.keys()])
        bot_scores[bot_id]['dialogs_short'] = sum([daily_statistics[date]['dialogs_short']
                                                   for date in daily_statistics.keys()])

    # ===== maint =====
    # return {'scores': bot_scores, 'convs': convs}

    def get_default_dict():
        return defaultdict(int)

    total_daily_statistics = defaultdict(get_default_dict)

    for bot in bot_daily_stats.values():
        for date, stats in bot.items():
            total_daily_statistics[date]['dialogs_total'] += stats['dialogs_total']
            total_daily_statistics[date]['dialogs_long'] += stats['dialogs_long']
            total_daily_statistics[date]['dialogs_short'] += stats['dialogs_short']

    bot_scores['total'] = {}

    if daily_stats:
        bot_scores['total']['daily_statistics'] = total_daily_statistics

    bot_scores['total']['dialogs_total'] = sum([day['dialogs_total'] for day in total_daily_statistics.values()])
    bot_scores['total']['dialogs_long'] = sum([day['dialogs_long'] for day in total_daily_statistics.values()])
    bot_scores['total']['dialogs_short'] = sum([day['dialogs_short'] for day in total_daily_statistics.values()])

    return bot_scores


def export_parlai_conversations(date_begin=None, date_end=None):
    # TODO: merge with export_bot_scores
    parlai_convs = {}

    if (date_begin is None) and (date_end is None):
        date_begin = '1900-01-01'
        date_end = '2500-12-31'
    elif (date_begin is not None) and (date_end is None):
        date_end = date_begin

    datetime_begin = datetime.strptime(f'{date_begin}_00:00:00.000000', "%Y-%m-%d_%H:%M:%S.%f")
    datetime_end = datetime.strptime(f'{date_end}_23:59:59.999999', "%Y-%m-%d_%H:%M:%S.%f")
    args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end}

    convs = Conversation.objects(**args)

    def process_conversation(conversation: Conversation):
        id = conversation.conversation_id
        messages = list(conversation.messages)
        msgs_processed = None

        if len(messages) >= 2:
            msgs = []
            msgs.append(messages[0].text)

            for index, message in enumerate(messages[1:]):
                if message.sender == messages[index].sender:
                    msgs[-1] = f'{msgs[-1]} {message.text}'
                else:
                    msgs.append(message.text)

            msgs_odd = msgs[::2]
            msgs_even = msgs[1::2]
            msgs_grouped = list(zip(msgs_odd, msgs_even))

            if msgs_grouped:
                msgs_processed = [f'text:{dialog[0]}\tlabels:{dialog[1]}' for dialog in msgs_grouped]
                msgs_processed = '\n'.join(msgs_processed)
                msgs_processed = f'{msgs_processed}\tepisode_done:True'

        return id, msgs_processed

    for conv in convs:
        conv_id, conv_processed = process_conversation(conv)
        if conv_processed:
            parlai_convs[conv_id] = conv_processed

    return parlai_convs
