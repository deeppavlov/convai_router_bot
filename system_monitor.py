#!/usr/bin/env python3.6
import argparse
import os
import sys
import json
import csv
from pathlib import Path

import mongoengine

import output_formatters
from model import util, Bot, User, BannedPair, UserPK


def setup_db_connection(uri=None):
    if uri is None:
        uri = os.environ.get('MONGO_URI')
    if uri is None:
        uri = "mongodb://localhost/convai"
        print('Warning: MONGO_URI is not provided. Using the default one: {}'.format(uri), file=sys.stderr)

    mongoengine.connect(host=uri)


def handle_fill_db_with_stub(args):
    util.fill_db_with_stub()
    print("Done!")


def handle_inactive_bots(args):
    for bot, count in util.get_inactive_bots(args.bots_number, args.conversations_threshold):
        print(args.formatter.format_entity(bot))
        print("Conversations: {}".format(count))


def handle_register_bot(args):
    util.register_bot(token=args.token, name=args.name)
    print("Done!")


def handle_complaints(args):
    complaints = util.get_complaints(args.include_processed)
    print(args.formatter.format_entity(complaints))


def handle_mark_complaint_processed(args):
    util.mark_complaints_processed(args.all, *args.complaints)
    print("Done!")


def handle_ban_human(args):
    banned = util.ban_human(args.platform, args.id)
    print("{} users banned".format(banned))


def handle_ban_bot(args):
    banned = util.ban_bot(args.token)
    print("{} bots banned".format(banned))


def handle_ban_human_bot(args):
    banned = util.ban_human_bot(args.platform, args.id, args.token)
    print("Banned pair:")
    print(args.formatter.format_entity(banned))


def handle_set_default_bot(args):
    updated = util.set_default_bot(args.platform, args.id, args.token)
    print("Default bot was set:")
    #print(args.formatter.format_entity(updated))


def handle_banlist_bot(args):
    print(args.formatter.format_entity(Bot.objects(banned=True)))


def handle_banlist_human(args):
    print(args.formatter.format_entity(User.objects(banned=True)))


def handle_banlist_human_bot(args):
    print(args.formatter(BannedPair.objects))


def handle_import_profiles(args):
    profiles = util.import_profiles(args.profiles_file)
    print(f'{len(profiles)} profiles imported')


def handle_training_conversations(args):
    save_dir = Path(args.target).expanduser().resolve()
    save_dir = save_dir.joinpath('train_valid')

    if not save_dir.is_dir():
        save_dir.mkdir(parents=True)

    begin_name_part = '' if args.begin is None else f'_{args.begin}'
    end_name_part = '' if args.end is None else f'_{args.end}'

    save_path_train = save_dir.joinpath(f'export{begin_name_part}{end_name_part}_train.json')
    save_path_valid = save_dir.joinpath(f'export{begin_name_part}{end_name_part}_valid.json')

    convs = util.export_training_conversations(args.begin, args.end)
    convs_num_train = round(len(convs) * args.rate)
    convs_train = convs[:convs_num_train]
    convs_valid = convs[convs_num_train:]

    with open(save_path_train, 'w') as f_train:
        json.dump(convs_train, f_train)

    with open(save_path_valid, 'w') as f_valid:
        json.dump(convs_valid, f_valid)

    print(f'Training and validation datasets for {begin_name_part[1:]} {end_name_part[1:]} saved in {save_dir}')


def handle_export_conversations(args):
    save_dir = Path(args.target).expanduser().resolve()
    save_dir = save_dir.joinpath('export_dialogs')

    if not save_dir.is_dir():
        save_dir.mkdir(parents=True)

    begin_name_part = '' if args.begin is None else f'_{args.begin}'
    end_name_part = '' if args.end is None else f'_{args.end}'
    save_path = save_dir.joinpath(f'export{begin_name_part}{end_name_part}.tsv')

    convs = util.export_training_conversations(args.begin, args.end, reveal_sender=True)

    with open(save_path, 'w', newline='') as f_tsv:
        fieldnames = ['INPUT:text', 'GOLDEN:result', 'HINT:text', 'TASK:id', 'TASK:overlap', 'TASK:remaining_overlap']
        writer = csv.DictWriter(f_tsv,
                                fieldnames=fieldnames,
                                dialect='excel-tab',
                                #delimiter='\t',
                                #quotechar='"'
                                quotechar=chr(1)
                                )
        writer.writeheader()

        for dialog in convs:
            replicas = []

            for replica in dialog['dialog']:
                sender = replica['sender_class']
                text = replica['text']
                replicas.append(f'<span class={sender.lower()}>{sender}: {text}</span><br>')

            #replicas_delimiter = chr(10)
            replicas_delimiter = ''
            replicas_string = replicas_delimiter.join(replicas)
            replicas_string = f'{chr(34)}{replicas_string}{chr(34)}'

            writer.writerow({'INPUT:text': replicas_string,
                             'TASK:id': dialog['dialog_id'],
                             'TASK:overlap': 'infinite',
                             'TASK:remaining_overlap': 'infinite'})

        print(f'Export conversations for {begin_name_part[1:]} {end_name_part[1:]} saved in {save_dir}')

def handle_bot_scores(args):
    save_dir = Path(args.target).expanduser().resolve()
    save_dir = save_dir.joinpath('bot_scores')

    if not save_dir.is_dir():
        save_dir.mkdir(parents=True)

    begin_name_part = '' if args.begin is None else f'_{args.begin}'
    end_name_part = '' if args.end is None else f'_{args.end}'
    save_path = save_dir.joinpath(f'bot_scores{begin_name_part}{end_name_part}.json')

    scores = util.export_bot_scores(args.begin, args.end)

    with open(save_path, 'w') as f_scores:
        json.dump(scores, f_scores)

    print(f'Bot scores for {begin_name_part[1:]} {end_name_part[1:]} saved in {save_dir}')


def setup_argparser():
    parser = argparse.ArgumentParser(description='ConvAI system management tool')
    parser.add_argument('--mongo-uri',
                        help='URI for connecting to MongoDB. Optional. If not specified then the value of MONGO_URI '
                             'system variable is used. If MONGO_URI is not defined either, '
                             'then "mongodb://localhost/convai" is used')
    parser.set_defaults(formatter=output_formatters.HumanReadable())
    subparsers = parser.add_subparsers(title='Available commands',
                                       description='For additional help use "<command> -h"')
    parser_fill_stub = subparsers.add_parser('fill-db-with-stub',
                                             help='Fill database with stub data for testing purposes',
                                             description='Fill database with stub data for testing purposes')
    parser_fill_stub.set_defaults(func=handle_fill_db_with_stub)

    parser_inactive_bots = subparsers.add_parser('inactive-bots',
                                                 help='Get bots with the fewest number of conversations',
                                                 description='Get bots with the fewest number of conversations')
    inactive_bots_group = parser_inactive_bots.add_mutually_exclusive_group()
    inactive_bots_group.add_argument('-n',
                                     '--bots-number',
                                     type=int,
                                     default=10,
                                     help='Number of bots to output. Default is %(default)s')
    inactive_bots_group.add_argument('-c',
                                     '--conversations-threshold',
                                     type=int,
                                     help='Output bots with <conversations-threshold> number of conversations or less')
    parser_inactive_bots.set_defaults(func=handle_inactive_bots)

    parser_register_bot = subparsers.add_parser('register-bot',
                                                help='Register new bot in the system',
                                                description='Register new bot in the system')
    parser_register_bot.add_argument('token',
                                     help='String which will serve as both Bot API access token and unique identifier '
                                          'of the bot')
    parser_register_bot.add_argument('name',
                                     help='Common human-readable name of the bot. Have no special meaning')
    parser_register_bot.set_defaults(func=handle_register_bot)

    parser_complaints = subparsers.add_parser('complaints',
                                              help='Get list of insult complaints',
                                              description='Get list of insult complaints')
    parser_complaints.add_argument('-a',
                                   '--include-processed',
                                   action='store_true',
                                   help='Show all complaints, including ones marked as processed')
    parser_complaints.set_defaults(func=handle_complaints)

    parser_mark_complaints = subparsers.add_parser('mark-complaints',
                                                   help='Mark insult complaints as processed',
                                                   description='Mark insult complaints as processed')
    parser_mark_complaints.add_argument('-a',
                                        '--all',
                                        action='store_true',
                                        help='Mark all complaints as processed')
    parser_mark_complaints.add_argument('complaints',
                                        nargs='*',
                                        metavar='ComplaintID',
                                        help='ID of the complaint to mark processed')
    parser_mark_complaints.set_defaults(func=handle_mark_complaint_processed)

    parser_ban_user = subparsers.add_parser('ban-human',
                                            help='Ban human user',
                                            description='Ban human user')
    parser_ban_user.add_argument('platform',
                                 choices=UserPK.PLATFORM_CHOICES,
                                 help='User platform')
    parser_ban_user.add_argument('id',
                                 help='User ID within the specified platform')
    parser_ban_user.set_defaults(func=handle_ban_human)

    parser_ban_bot = subparsers.add_parser('ban-bot',
                                           help='Ban bot',
                                           description='Ban bot')
    parser_ban_bot.add_argument('token',
                                help='Bot access token')
    parser_ban_bot.set_defaults(func=handle_ban_bot)

    parser_ban_user_bot = subparsers.add_parser('ban-human-bot-pair',
                                                help='Ban human-bot pair to prevent developers from evaluating their '
                                                     'own bots',
                                                description='Ban human-bot pair to prevent developers from evaluating '
                                                            'their own bots')
    parser_ban_user_bot.add_argument('platform',
                                     choices=UserPK.PLATFORM_CHOICES,
                                     help='User platform')
    parser_ban_user_bot.add_argument('id',
                                     help='User ID within the specified platform')
    parser_ban_user_bot.add_argument('token',
                                     help='Bot access token')
    parser_ban_user_bot.set_defaults(func=handle_ban_human_bot)

    parser_set_default_bot = subparsers.add_parser('set-default-bot',
                                                   help='Set default bot for user to interact with',
                                                   description='Set default bot for user to interact with')
    parser_set_default_bot.add_argument('platform',
                                        choices=UserPK.PLATFORM_CHOICES,
                                        help='User platform')
    parser_set_default_bot.add_argument('id',
                                        help='User ID within the specified platform')
    parser_set_default_bot.add_argument('-t',
                                        '--token',
                                        help='Bot access token. Default is %(default)s',
                                        default=None)
    parser_set_default_bot.set_defaults(func=handle_set_default_bot)

    parser_banlist = subparsers.add_parser('banlist',
                                           help='List banned humans, bots or human-bot pairs',
                                           description='List banned humans, bots or human-bot pairs')
    banlist_group = parser_banlist.add_mutually_exclusive_group(required=True)
    banlist_group.add_argument('-b',
                               '--bot',
                               action='store_const',
                               const=handle_banlist_bot,
                               dest='func',
                               help='Banned bots')
    banlist_group.add_argument('-u',
                               '--human',
                               action='store_const',
                               const=handle_banlist_human,
                               dest='func',
                               help='Banned humans')
    banlist_group.add_argument('-hb',
                               '--human-bot',
                               action='store_const',
                               const=handle_banlist_human_bot,
                               dest='func',
                               help='Banned human-bot pairs')

    parser_import_profiles = subparsers.add_parser('import-profiles',
                                                   help='Import profiles into database',
                                                   description='Import profiles into database')
    parser_import_profiles.add_argument('profiles_file',
                                        type=argparse.FileType('r'),
                                        nargs='?',
                                        help='Profiles file name. stdin by default',
                                        default=sys.stdin)
    parser_import_profiles.set_defaults(func=handle_import_profiles)

    training_conversations = subparsers.add_parser('training-dialogs',
                                                   help='Export training conversations for given date or interval',
                                                   description='Export training conversations for given date or interval')
    training_conversations.add_argument('-b',
                                        '--begin',
                                        type=str,
                                        default=None,
                                        help='Begin or exact date of export interval in YYYY-MM-DD format. Default is %(default)s')
    training_conversations.add_argument('-e',
                                        '--end',
                                        type=str,
                                        default=None,
                                        help='End date of export interval in YYYY-MM-DD format Default is %(default)s')
    training_conversations.add_argument('-t',
                                        '--target',
                                        type=str,
                                        default='~/router_bot_export',
                                        help='Target dir for export. Default is %(default)s')
    training_conversations.add_argument('-r',
                                        '--rate',
                                        type=int,
                                        default=0.8,
                                        help='Dialogs in training/validation datasets rate. Default is %(default)s')
    training_conversations.set_defaults(func=handle_training_conversations)

    export_conversations = subparsers.add_parser('export-dialogs',
                                                   help='Export conversations for given date or interval',
                                                   description='Export conversations for given date or interval')
    export_conversations.add_argument('-b',
                                      '--begin',
                                      type=str,
                                      default=None,
                                      help='Begin or exact date of export interval in YYYY-MM-DD format. Default is %(default)s')
    export_conversations.add_argument('-e',
                                      '--end',
                                      type=str,
                                      default=None,
                                      help='End date of export interval in YYYY-MM-DD format Default is %(default)s')
    export_conversations.add_argument('-t',
                                      '--target',
                                      type=str,
                                      default='~/router_bot_export',
                                      help='Target dir for export. Default is %(default)s')
    export_conversations.set_defaults(func=handle_export_conversations)

    bot_scores = subparsers.add_parser('bot-scores',
                                       help='Export dayly and total bot scores',
                                       description='Export dayly and total bot scores')

    bot_scores.add_argument('-b',
                            '--begin',
                            type=str,
                            default=None,
                            help='Begin or exact date of report interval in YYYY-MM-DD format. Default is %(default)s')
    bot_scores.add_argument('-e',
                            '--end',
                            type=str,
                            default=None,
                            help='End date of report interval in YYYY-MM-DD format Default is %(default)s')
    bot_scores.add_argument('-t',
                            '--target',
                            type=str,
                            default='~/router_bot_export',
                            help='Target dir for export. Default is %(default)s')
    bot_scores.set_defaults(func=handle_bot_scores)

    return parser


def main():
    parser = setup_argparser()
    args = parser.parse_args()
    setup_db_connection(args.mongo_uri)
    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
