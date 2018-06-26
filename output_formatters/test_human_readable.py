from datetime import datetime
from unittest import TestCase

from model import Bot, User, UserPK, BannedPair, Message, PersonProfile, ConversationPeer, Conversation, Complaint
from output_formatters.human_readable import HumanReadable


class TestHumanReadable(TestCase):
    def setUp(self):
        self.f = HumanReadable()

    @property
    def bot(self):
        return Bot(token="bot_token", bot_name="bot_name")

    @property
    def user(self):
        return User(user_key=UserPK(platform='Facebook', user_id='user_id'), username='Jon Snow', banned=True)

    @property
    def banned_pair(self):
        return BannedPair(user=self.user, bot=self.bot)

    @property
    def messages(self):
        return [Message(sender=self.user, msg_id=0, text='msg text', time=datetime(2018, 5, 8, 12, 34, 56, 789)),
                Message(sender=self.bot, msg_id=1, text='msg text', time=datetime(2018, 5, 8, 12, 36, 54, 321),
                        evaluation_score=1),
                Message(sender=self.bot, msg_id=2, text='msg text', time=datetime(2018, 5, 8, 12, 37, 55, 432),
                        evaluation_score=0)]

    @property
    def profile(self):
        return PersonProfile(sentences=['profile description'])

    @property
    def conversation_peers(self):
        return [ConversationPeer(peer=self.user, assigned_profile=self.profile, dialog_evaluation_score=4,
                                 other_peer_profile_options=[self.profile, self.profile],
                                 other_peer_profile_selected=self.profile),
                ConversationPeer(peer=self.bot, assigned_profile=self.profile)]

    @property
    def conversation(self):
        return Conversation(conversation_id=3, participant1=self.conversation_peers[0],
                            participant2=self.conversation_peers[1], messages=self.messages,
                            start_time=self.messages[0].time, end_time=self.messages[-1].time)

    @property
    def complaint(self):
        return Complaint(complainer=self.user, complain_to=self.bot, conversation=self.conversation, processed=True)

    @property
    def iterable(self):
        return [self.bot, self.user, self.banned_pair, self.messages, self.profile, self.conversation_peers,
                self.conversation, self.complaint]

    def test_format_banned_pair(self):
        out = 'User:\n  Platform: Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nBot:\n  Token: ' \
              'bot_token\n  Bot name: bot_name'
        self.assertEqual(self.f.format_banned_pair(self.banned_pair), out)
        self.assertEqual(self.f.format_entity(self.banned_pair), out)

    def test_format_bot(self):
        out = 'Token: bot_token\nBot name: bot_name'
        self.assertEqual(self.f.format_bot(self.bot), out)
        self.assertEqual(self.f.format_entity(self.bot), out)

    def test_format_complaint(self):
        out = 'Complainer:\n  Platform: Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nComplain to:\n  ' \
              'Token: bot_token\n  Bot name: bot_name\nProcessed: True\nConversation:\n  Conversation ID: 3\n  ' \
              'Participants:\n    Peer:\n      Platform: Facebook\n      ID: user_id\n      Username: Jon Snow\n      '\
              'Banned!\n    Assigned profile:\n      Profile None\n      profile description\n    Given dialog score: '\
              '4\n    Other peer profile options:\n      Profile None\n      profile description\n\n      Profile ' \
              'None\n      profile description\n    Selected other peer profile:\n      Profile None\n      profile ' \
              'description\n\n    Peer:\n      Token: bot_token\n      Bot name: bot_name\n    Assigned profile:\n    '\
              '  Profile None\n      profile description\n    Other peer profile options:\n\n\n  Start time: ' \
              '2018-05-08 12:34:56.000789\n  End time: 2018-05-08 12:37:55.000432\n  Messages:\n    ID: 0\n    From: ' \
              'Jon Snow\n    Time: 2018-05-08 12:34:56.000789\n    Text:\n      msg text\n\n    ID: 1\n    From: ' \
              'bot_name\n    Time: 2018-05-08 12:36:54.000321\n    Evaluation: 1\n    Text:\n      msg text\n\n    ' \
              'ID: 2\n    From: bot_name\n    Time: 2018-05-08 12:37:55.000432\n    Evaluation: 0\n    Text:\n      ' \
              'msg text'
        self.assertEqual(self.f.format_complaint(self.complaint), out)
        self.assertEqual(self.f.format_entity(self.complaint), out)

    def test_format_conversation(self):
        out = 'Conversation ID: 3\nParticipants:\n  Peer:\n    Platform: Facebook\n    ID: user_id\n    Username: Jon ' \
              'Snow\n    Banned!\n  Assigned profile:\n    Profile None\n    profile description\n  Given dialog ' \
              'score: 4\n  Other peer profile options:\n    Profile None\n    profile description\n\n    Profile ' \
              'None\n    profile description\n  Selected other peer profile:\n    Profile None\n    profile ' \
              'description\n\n  Peer:\n    Token: bot_token\n    Bot name: bot_name\n  Assigned profile:\n    Profile ' \
              'None\n    profile description\n  Other peer profile options:\n\n\nStart time: 2018-05-08 ' \
              '12:34:56.000789\nEnd time: 2018-05-08 12:37:55.000432\nMessages:\n  ID: 0\n  From: Jon Snow\n  Time: ' \
              '2018-05-08 12:34:56.000789\n  Text:\n    msg text\n\n  ID: 1\n  From: bot_name\n  Time: 2018-05-08 ' \
              '12:36:54.000321\n  Evaluation: 1\n  Text:\n    msg text\n\n  ID: 2\n  From: bot_name\n  Time: ' \
              '2018-05-08 12:37:55.000432\n  Evaluation: 0\n  Text:\n    msg text'
        self.assertEqual(self.f.format_conversation(self.conversation), out)
        self.assertEqual(self.f.format_entity(self.conversation), out)

    def test_format_conversation_peer(self):
        out = 'Peer:\n  Platform: Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nAssigned profile:\n  ' \
              'Profile None\n  profile description\nGiven dialog score: 4\nOther peer profile options:\n  Profile ' \
              'None\n  profile description\n\n  Profile None\n  profile description\nSelected other peer profile:\n  ' \
              'Profile None\n  profile description'
        self.assertEqual(self.f.format_conversation_peer(self.conversation_peers[0]), out)
        self.assertEqual(self.f.format_entity(self.conversation_peers[0]), out)

    def test_format_message(self):
        out = 'ID: 0\nFrom: Jon Snow\nTime: 2018-05-08 12:34:56.000789\nText:\n  msg text'
        self.assertEqual(self.f.format_message(self.messages[0]), out)
        self.assertEqual(self.f.format_entity(self.messages[0]), out)

    def test_format_profile(self):
        out = 'Profile None\nprofile description'
        self.assertEqual(self.f.format_profile(self.profile), out)
        self.assertEqual(self.f.format_entity(self.profile), out)

    def test_format_user(self):
        out = 'Platform: Facebook\nID: user_id\nUsername: Jon Snow\nBanned!'
        self.assertEqual(self.f.format_user(self.user), out)
        self.assertEqual(self.f.format_entity(self.user), out)

    def test_format_iterable(self):
        out = 'Token: bot_token\nBot name: bot_name\n\nPlatform: Facebook\nID: user_id\nUsername: Jon ' \
              'Snow\nBanned!\n\nUser:\n  Platform: Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nBot:\n  ' \
              'Token: bot_token\n  Bot name: bot_name\n\nID: 0\nFrom: Jon Snow\nTime: 2018-05-08 ' \
              '12:34:56.000789\nText:\n  msg text\n\nID: 1\nFrom: bot_name\nTime: 2018-05-08 ' \
              '12:36:54.000321\nEvaluation: 1\nText:\n  msg text\n\nID: 2\nFrom: bot_name\nTime: 2018-05-08 ' \
              '12:37:55.000432\nEvaluation: 0\nText:\n  msg text\n\nProfile None\nprofile description\n\nPeer:\n  ' \
              'Platform: Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nAssigned profile:\n  Profile ' \
              'None\n  profile description\nGiven dialog score: 4\nOther peer profile options:\n  Profile None\n  ' \
              'profile description\n\n  Profile None\n  profile description\nSelected other peer profile:\n  Profile ' \
              'None\n  profile description\n\nPeer:\n  Token: bot_token\n  Bot name: bot_name\nAssigned profile:\n  ' \
              'Profile None\n  profile description\nOther peer profile options:\n\n\n\nConversation ID: ' \
              '3\nParticipants:\n  Peer:\n    Platform: Facebook\n    ID: user_id\n    Username: Jon Snow\n    ' \
              'Banned!\n  Assigned profile:\n    Profile None\n    profile description\n  Given dialog score: 4\n  ' \
              'Other peer profile options:\n    Profile None\n    profile description\n\n    Profile None\n    ' \
              'profile description\n  Selected other peer profile:\n    Profile None\n    profile description\n\n  ' \
              'Peer:\n    Token: bot_token\n    Bot name: bot_name\n  Assigned profile:\n    Profile None\n    ' \
              'profile description\n  Other peer profile options:\n\n\nStart time: 2018-05-08 12:34:56.000789\nEnd ' \
              'time: 2018-05-08 12:37:55.000432\nMessages:\n  ID: 0\n  From: Jon Snow\n  Time: 2018-05-08 ' \
              '12:34:56.000789\n  Text:\n    msg text\n\n  ID: 1\n  From: bot_name\n  Time: 2018-05-08 ' \
              '12:36:54.000321\n  Evaluation: 1\n  Text:\n    msg text\n\n  ID: 2\n  From: bot_name\n  Time: ' \
              '2018-05-08 12:37:55.000432\n  Evaluation: 0\n  Text:\n    msg text\n\nComplainer:\n  Platform: ' \
              'Facebook\n  ID: user_id\n  Username: Jon Snow\n  Banned!\nComplain to:\n  Token: bot_token\n  Bot ' \
              'name: bot_name\nProcessed: True\nConversation:\n  Conversation ID: 3\n  Participants:\n    Peer:\n     '\
              ' Platform: Facebook\n      ID: user_id\n      Username: Jon Snow\n      Banned!\n    Assigned ' \
              'profile:\n      Profile None\n      profile description\n    Given dialog score: 4\n    Other peer ' \
              'profile options:\n      Profile None\n      profile description\n\n      Profile None\n      profile ' \
              'description\n    Selected other peer profile:\n      Profile None\n      profile description\n\n    ' \
              'Peer:\n      Token: bot_token\n      Bot name: bot_name\n    Assigned profile:\n      Profile None\n   '\
              '   profile description\n    Other peer profile options:\n\n\n  Start time: 2018-05-08 ' \
              '12:34:56.000789\n  End time: 2018-05-08 12:37:55.000432\n  Messages:\n    ID: 0\n    From: Jon Snow\n  '\
              '  Time: 2018-05-08 12:34:56.000789\n    Text:\n      msg text\n\n    ID: 1\n    From: bot_name\n    ' \
              'Time: 2018-05-08 12:36:54.000321\n    Evaluation: 1\n    Text:\n      msg text\n\n    ID: 2\n    From: '\
              'bot_name\n    Time: 2018-05-08 12:37:55.000432\n    Evaluation: 0\n    Text:\n      msg text'

        self.assertEqual(self.f.format_iterable(self.iterable), out)
        self.assertEqual(self.f.format_entity(self.iterable), out)

    def test__format_lines(self):
        l = list('abc')
        out0 = 'a\nb\nc'
        out1 = '  a\n  b\n  c'
        out3 = '      a\n      b\n      c'
        self.assertEqual(self.f._format_lines(l, 0), out0)
        self.assertEqual(self.f._format_lines(l, 1), out1)
        self.assertEqual(self.f._format_lines(l, 3), out3)
