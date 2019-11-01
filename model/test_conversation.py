from datetime import datetime, timedelta
from unittest import TestCase

import mongoengine

from model.bot import Bot
from model.conversation import Conversation
from model.conversation_peer import ConversationPeer
from model.message import Message
from model.person_profile import PersonProfile
from model.test_common import MockedMongoTestCase
from model.user import User, UserPK


class TestConversation(MockedMongoTestCase):

    def test_clean(self):
        peer1 = ConversationPeer(peer=User(user_key=UserPK(user_id='stub',
                                                           platform=UserPK.PLATFORM_TELEGRAM),
                                           username='Dummy'),
                                 assigned_profile=PersonProfile(persona=['stub profile']))
        peer2 = ConversationPeer(peer=Bot(token='stub',
                                          bot_name='Dummy'),
                                 assigned_profile=PersonProfile(persona=['stub profile 2']))
        peers = [peer1, peer2]

        for p in peers:
            p.peer.save(cascade=True)
            p.assigned_profile.save(cascade=True)

        n_msg = 10
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=n_msg - 1)

        msgs = list(map(lambda x: Message(msg_id=x,
                                          text=str(x),
                                          sender=peers[x % 2].peer,
                                          time=start_time + timedelta(hours=x)),
                        range(10)))

        test_conv = Conversation(conversation_id=1)

        test_conv.participant1 = peers[0]
        test_conv.participant2 = peers[1]
        with self.assertRaises(mongoengine.ValidationError):
            test_conv.save()

        test_conv.participant1 = None
        test_conv.participant2 = None
        test_conv.messages = msgs

        with self.assertRaises(mongoengine.ValidationError):
            test_conv.save()

        test_conv.participant1 = peers[0]
        test_conv.participant2 = peers[1]

        raised = False
        error_msg = ''

        try:
            test_conv.save()
        except Exception as e:
            raised = True
            error_msg = 'Unexpected exception: {}'.format(e)

        self.assertFalse(raised, error_msg)

        self.assertEqual(test_conv.start_time, start_time)
        self.assertEqual(test_conv.end_time, end_time)
