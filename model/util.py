import os
from datetime import datetime, timedelta
from io import StringIO
from typing import TextIO, Union
from uuid import uuid4

from mongoengine import errors

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
