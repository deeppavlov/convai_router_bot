import os
from datetime import datetime, timedelta
from io import StringIO
from typing import TextIO, Union
from uuid import uuid4

from mongoengine import errors
from mongoengine.queryset.visitor import Q

from . import Bot, PersonProfile, User, UserPK, BannedPair, Conversation, ConversationPeer, Message, Complaint


def fill_db_with_stub(n_bots=5,
                      n_bots_banned=2,
                      n_humans=10,
                      n_humans_banned=2,
                      n_banned_pairs=3,
                      n_profiles=20,
                      n_conversations=20,
                      n_msg_per_conv=15,
                      n_complaints_new=3,
                      n_complaints_processed=2):
    from random import choice, randint
    with open(os.path.join(os.path.split(__file__)[0], "lorem_ipsum.txt"), 'r') as f:
        lorem_ipsum = f.read().split(' ')

    profiles = [PersonProfile(sentences=[' '.join(lorem_ipsum[i * 10:(i + 1) * 10])]).save() for i in range(n_profiles)]
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


def import_profiles(stream: Union[TextIO, StringIO]):
    profiles = map(lambda x: PersonProfile(sentences=x.splitlines()), stream.read().split('\n\n'))
    return PersonProfile.objects.insert(list(profiles))


def export_training_conversations(export_date):
    training_convs = []

    datetime_begin = datetime.strptime(f'{export_date}_00:00:00.000000', "%Y-%m-%d_%H:%M:%S.%f")
    datetime_end = datetime.strptime(f'{export_date}_23:59:59.999999', "%Y-%m-%d_%H:%M:%S.%f")
    args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end}
    convs = Conversation.objects(**args)

    for conv in convs:
        training_conv = {
            'dialog_id': str(conv.id),
            'dialog': []
        }
        participants = {}
        participants[str(conv.participant1.peer.id)] = 'participant1'
        participants[str(conv.participant2.peer.id)] = 'participant2'

        for msg in conv.messages:
            training_message = {
                'id': msg.msg_id,
                'sender': participants[str(msg.sender.id)],
                'text': msg.text
            }
            training_conv['dialog'].append(training_message)

        training_convs.append(training_conv)

    return training_convs


def export_bot_scores():
    # TODO: refactor with $lookup
    convs = {}

    profiles_obj = PersonProfile.objects
    profiles = {str(profile.pk): list(profile.sentences) for profile in profiles_obj}

    for bot in Bot.objects:
        bot_id = str(bot.id)

        q_participant1 = Q(participant1__peer=bot)
        q_participant2 = Q(participant2__peer=bot)
        bot_convs = Conversation.objects(q_participant1 | q_participant2)

        for bot_conv in bot_convs:
            bot_conv_id = str(bot_conv.id)

            if isinstance(bot_conv.participant1.peer, Bot):
                peer_bot = bot_conv.participant1
                peer_user = bot_conv.participant2
            else:
                peer_bot = bot_conv.participant2
                peer_user = bot_conv.participant1

            bot_profile = peer_bot.assigned_profile
            user_eval_score = peer_user.dialog_evaluation_score
            user_profile_selected = peer_user.other_peer_profile_selected
            user_profile_selected_parts = peer_user.other_peer_profile_selected_parts

            convs

            #try:
            #    print(part_user.other_peer_profile_selected.sentences)
            #except errors.DoesNotExist:
            #    print('errors.DoesNotExist:')
            #except AttributeError:
            #    print('AttributeError:')

            #convs[bot_id] = type(bot_conv.participant2.peer)


    #bot_ids = [bot.id for bot in Bot.objects]
    #participant1 = Conversation.participant1.peer.id
    #participant2 = Conversation.participant2.peer.id

    #for bot_id in bot_ids:

        #q1 = Q("{'participant1.peer.id': bot_id}")
        #q2 = {'participant1.peer.id': bot_id}
        #args = {'$or': [{'participant1.peer': bot_id}, {'participant2.peer': bot_id}]}
        #args = ()
        #convs = Conversation.objects(Q(participant1=bot_id) | Q(participant2=bot_id))
    #bot_id = bot_ids[1]
    #args = {'participant1__peer__exists': True, 'participant1__peer': bot_id}
    #convs = Conversation.objects(**args)

    #argzz = {'pk': 'stube91cfb90-8f4d-4d1f-9991-1b57a7823d14'}
    #big_bots = Bot.objects(**argzz)
    #print(big_bots)
    #big_bot = big_bots[0]

    #export_date = '2018-06-27'

    #datetime_begin = datetime.strptime(f'{export_date}_00:00:00.000000', "%Y-%m-%d_%H:%M:%S.%f")
    #datetime_end = datetime.strptime(f'{export_date}_23:59:59.999999', "%Y-%m-%d_%H:%M:%S.%f")
    #q_dt_args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end}
    #q_p1_args = {'participant1__peer': big_bot}
    #q_p2_args = {'participant2__peer': big_bot}
    #args = {'start_time__gte': datetime_begin, 'start_time__lte': datetime_end, 'participant2__peer': big_bot}

    #q_dt = Q(**q_dt_args)
    #q_p1 = Q(**q_p1_args)
    #q_p2 = Q(**q_p2_args)

    #convs = Conversation.objects(**args)
    #convs = Conversation.objects(q_dt & (q_p1 | q_p2))

    #conv = convs[0]
    #part = type(conv.participant2.peer)

    #profiles = PersonProfile.objects()
    #prof1 = profiles[0]
    #print(str(prof1.pk))
    #result = convs

    result = convs
    return result
