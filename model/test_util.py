from io import StringIO
from random import choice
from unittest import skip

from model import *
from model.test_common import MockedMongoTestCase

stub_data_kwargs = {"n_bots": 5,
                    "n_bots_banned": 2,
                    "n_humans": 10,
                    "n_humans_banned": 2,
                    "n_banned_pairs": 3,
                    "n_profiles": 20,
                    "n_conversations": 20,
                    "n_msg_per_conv": 15,
                    "n_complaints_new": 5,
                    "n_complaints_processed": 2}


class TestUtil(MockedMongoTestCase):
    def test_fill_db_with_stub(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        self.assertEqual(BannedPair.objects.count(), stub_data_kwargs["n_banned_pairs"])
        self.assertEqual(Bot.objects.count(), stub_data_kwargs["n_bots"] + stub_data_kwargs["n_bots_banned"])
        self.assertEqual(Bot.objects(banned=True).count(), stub_data_kwargs["n_bots_banned"])
        self.assertEqual(Complaint.objects.count(),
                         stub_data_kwargs["n_complaints_new"] + stub_data_kwargs["n_complaints_processed"])
        self.assertEqual(Complaint.objects(processed=True).count(), stub_data_kwargs["n_complaints_processed"])
        self.assertEqual(Conversation.objects.count(), stub_data_kwargs["n_conversations"])
        self.assertEqual(PersonProfile.objects.count(), stub_data_kwargs["n_profiles"])
        self.assertEqual(User.objects.count(), stub_data_kwargs["n_humans"] + stub_data_kwargs["n_humans_banned"])
        self.assertEqual(User.objects(banned=True).count(), stub_data_kwargs["n_humans_banned"])

    @skip('Requires real mongo instance as mongomock fails to work with aggregate queries properly')
    def test_get_inactive_bots(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        by_threshold = list(util.get_inactive_bots(0, threshold=2))
        by_count = list(util.get_inactive_bots(n_bots=2))

        self.assertEqual(len(by_count), 2)

        by_threshold_counts = [x[1] for x in by_threshold]
        by_count_counts = [x[1] for x in by_count]

        self.assertSequenceEqual(sorted(by_threshold_counts), by_threshold_counts)
        self.assertSequenceEqual(sorted(by_count_counts), by_count_counts)

    def test_register_bot(self):
        util.register_bot('token', 'name')

        self.assertIsNotNone(Bot.objects.with_id('token'))
        self.assertEqual(Bot.objects.with_id('token').bot_name, 'name')

    def test_get_complaints(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        all_complaints = util.get_complaints(True)
        unprocessed_complaints = util.get_complaints(False)

        self.assertEqual(all_complaints.count(),
                         stub_data_kwargs['n_complaints_processed'] + stub_data_kwargs['n_complaints_new'])
        self.assertEqual(unprocessed_complaints.count(),
                         stub_data_kwargs['n_complaints_new'])

    def test_mark_complaints_processed(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        new_complaints = Complaint.objects(processed=False)[:2]

        util.mark_complaints_processed(False, *[nc.id for nc in new_complaints])

        self.assertEqual(util.get_complaints(False).count(), stub_data_kwargs['n_complaints_new'] - 2)

        self.assertEqual(util.mark_complaints_processed(True),
                         stub_data_kwargs['n_complaints_new'] + stub_data_kwargs['n_complaints_processed'])
        self.assertEqual(util.get_complaints(False).count(), 0)

    def test_ban_human(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        some_human = User.objects(banned=False).first()

        self.assertEqual(util.ban_human(some_human.user_key.platform, some_human.user_key.user_id), 1)
        self.assertEqual(util.ban_human('nonExisting', 'fake'), 0)
        self.assertEqual(User.objects(banned=False).count(), stub_data_kwargs['n_humans'] - 1)

    def test_ban_bot(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        some_bot = Bot.objects(banned=False).first()

        self.assertEqual(util.ban_bot(some_bot.token), 1)
        self.assertEqual(util.ban_bot('nonExisting'), 0)
        self.assertEqual(Bot.objects(banned=False).count(), stub_data_kwargs['n_bots'] - 1)

    def test_ban_human_bot(self):
        util.fill_db_with_stub(**stub_data_kwargs)

        self.assertEqual(BannedPair.objects.count(), stub_data_kwargs['n_banned_pairs'])

        while True:
            some_human = choice(User.objects)
            some_bot = choice(Bot.objects)

            if BannedPair.objects(user=some_human, bot=some_bot).count() == 0:
                break

        new_pair = util.ban_human_bot(some_human.user_key.platform, some_human.user_key.user_id, some_bot.token)

        self.assertEqual(new_pair.bot, some_bot)
        self.assertEqual(new_pair.user, some_human)
        self.assertEqual(BannedPair.objects.count(), stub_data_kwargs['n_banned_pairs'] + 1)

    def test_import_profiles(self):
        single_profile_txt = 'a\nb\nc'
        multi_profile_txt = 'd\ne\nf\n\nx\ny\nz'

        single_profile = util.import_profiles(StringIO(single_profile_txt))
        self.assertEqual(len(single_profile), 1)
        self.assertEqual(PersonProfile.objects.count(), 1)

        multi_profile = util.import_profiles(StringIO(multi_profile_txt))
        self.assertEqual(PersonProfile.objects.count(), 3)
        self.assertEqual(len(multi_profile), 2)

        self.assertEqual(single_profile[0].description, single_profile_txt)
        self.assertEqual(multi_profile[0].description, 'd\ne\nf')
        self.assertEqual(multi_profile[1].description, 'x\ny\nz')
