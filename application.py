import argparse
import asyncio
import logging
import os
import re
from functools import wraps
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from urllib.parse import urlparse
from pathlib import Path

import aiofiles
import mongoengine
import yaml
from aiohttp import web
from telepot.aio import Bot

from convai.conversation_gateways import HumansGateway, BotsGateway
from convai.dialog_manager import DialogManager
from convai.exceptions import BotNotRegisteredError
from convai.messenger_interfaces import FacebookMessenger, TelegramMessenger
from convai.messages_wrapper import MessagesWrapper


async def init():
    global config
    global fb_messenger
    global tg_messenger
    global bots_gateway

    def sub_env_vars(d: dict):
        regex = re.compile(r'\${(\w+?)(?::(.*))?}')
        for k, v in d.items():
            if isinstance(v, str):
                m = regex.match(v)
                if m:
                    d[k] = os.environ.get(*m.groups())
            elif isinstance(v, dict):
                sub_env_vars(v)

    def validate_config(c: dict, required_keys: Dict[str, Any], prefix: str = ''):
        for key, value in required_keys.items():
            if key not in c or c[key] is None:
                raise ValueError(f'Invalid configuration. Value for "{prefix}.{key}" is missing.')
            if isinstance(value, Dict):
                validate_config(c[key], value, f'{prefix}.{key}')

    def recursive_update(d1: dict, d2: dict):
        for k, v in d2.items():
            if k not in d1 or not isinstance(v, dict):
                d1[k] = v
            else:
                recursive_update(d1[k], v)

    config = {}
    config_files = [os.path.join(os.path.dirname(__file__), 'settings/config.yml'),
                    '/etc/convai-router/config.yml']

    for filename in config_files:
        if not os.path.exists(filename):
            continue
        async with aiofiles.open(filename, 'r') as f:
            new_config = yaml.safe_load(await f.read())
            recursive_update(config, new_config)

    sub_env_vars(config)
    validate_config(config, {'mongo_uri': True,
                             'facebook': True,
                             'telegram': True})
    fb_config = config['facebook']
    tg_config = config['telegram']

    if fb_config:
        validate_config(fb_config, {'page_access_token': True,
                                    'webhook': True,
                                    'webhook_secret': True})
    if tg_config:
        validate_config(tg_config, {'webhook': True,
                                    'token': True})

    logging_config = config['logging'] if 'logging' in config else {}

    setup_logging(logging_config)

    mongoengine.connect(host=config['mongo_uri'])

    messages_file = os.path.join(os.path.dirname(__file__), 'settings/messages.tsv')
    messages = MessagesWrapper(Path(messages_file).resolve())

    humans_gateway = HumansGateway(config['dialog']['guess_profile_sentence_by_sentence'],
                                   config['dialog']['allow_set_bot'],
                                   config['dialog']['reveal_dialog_id'],
                                   config['evaluation_options'],
                                   messages)

    bots_gateway = BotsGateway(config['dialog']['n_bad_messages_in_a_row_threshold'])

    init_tasks = []

    if fb_config:
        fb_messenger = FacebookMessenger(humans_gateway, config['facebook']['page_access_token'])
        init_tasks.append(fb_messenger.perform_initial_setup(True))
        humans_gateway.add_messengers(fb_messenger)

    if tg_config:
        tg_bot = Bot(config['telegram']['token'], loop=loop)
        tg_messenger = TelegramMessenger(humans_gateway, tg_bot, config['telegram']['webhook'])
        init_tasks.append(tg_messenger.perform_initial_setup())
        humans_gateway.add_messengers(tg_messenger)

    dialog_manager = DialogManager(config['dialog']['max_time_in_lobby'],
                                   config['dialog']['human_bot_ratio'],
                                   config['dialog']['inactivity_timeout'],
                                   config['dialog']['max_length'],
                                   bots_gateway,
                                   humans_gateway,
                                   config['dialog']['evaluation_score_from'],
                                   config['dialog']['evaluation_score_to'],
                                   config['evaluation_options'])

    humans_gateway.dialog_handler = dialog_manager
    bots_gateway.dialog_handler = dialog_manager

    await asyncio.gather(*init_tasks)


def setup_logging(logging_config):
    path = logging_config['root_path'] if 'root_path' in logging_config else ''
    level = logging_config['base_level'] if 'base_level' in logging_config else 'INFO'

    full_formatter = logging.Formatter('[%(asctime)s %(name)-50s %(levelname)-8s] %(message)s')

    logging.basicConfig(level=level,
                        format='[%(name)-50s: %(levelname)-8s] %(message)s')

    # noinspection PyBroadException
    try:
        if not path:
            raise ValueError('logging root_path not set')

        logs_folder = Path(path).expanduser()
        # noinspection PyTypeChecker
        os.makedirs(logs_folder, exist_ok=True)

        file_log_handler = logging.FileHandler(logs_folder / 'info.log')
        file_log_handler.setLevel(logging.INFO)
        file_log_handler.setFormatter(full_formatter)
        logging.getLogger('').addHandler(file_log_handler)

        err_log_handler = logging.FileHandler(logs_folder / 'error.log')
        err_log_handler.setLevel(logging.ERROR)
        err_log_handler.setFormatter(full_formatter)
        logging.getLogger('').addHandler(err_log_handler)

        logging.info(f'storing file logs in {logs_folder.absolute()}')
    except Exception as e:
        logging.exception(f'not using file logging. Error: {e}')


def setup_routes():
    app.add_routes([web.get('/bot{token}/getUpdates', handle_bot_get_updates),
                    web.post('/bot{token}/getUpdates', handle_bot_get_updates),
                    web.get('/bot{token}/sendMessage', handle_bot_send_message),
                    web.post('/bot{token}/sendMessage', handle_bot_send_message)])
    fb_config = config['facebook']
    tg_config = config['telegram']
    if fb_config:
        app.add_routes([web.get(urlparse(fb_config['webhook']).path, handle_fb_verification),
                        web.post(urlparse(fb_config['webhook']).path, handle_fb_message)])
    if tg_config:
        app.add_routes([web.post(urlparse(tg_config['webhook']).path, handle_tg_message)])


async def handle_fb_verification(request: web.Request):
    log.debug('FB  verification request received')
    mode = request.query.getone('hub.mode', '')
    token = request.query.getone('hub.verify_token', '')
    challenge = request.query.getone('hub.challenge', '')
    if mode == 'subscribe' and token == config['facebook']['webhook_secret']:
        return web.Response(text=challenge)
    else:
        log.error(f'invalid fb verification data. Mode: {mode} Challenge: {challenge}')
        return web.Response(text='Error', status=403)


async def handle_fb_message(request: web.Request):
    await fb_messenger.feed(await request.read())
    return web.Response()


async def handle_tg_message(request: web.Request):
    tg_messenger.feed(await request.read())
    return web.Response()


def bot_endpoint_handler(f):
    @wraps(f)
    async def internal(request: web.Request):
        token = request.match_info['token']
        post_dict = await request.post()
        query_dict = request.query
        try:
            json_dict = await request.json()
        except JSONDecodeError:
            json_dict = {}

        def get_param(param: str) -> Optional[str]:
            if param in query_dict:
                return query_dict[param]
            if param in json_dict:
                return json_dict[param]
            if param in post_dict:
                return post_dict[param]
            return None

        # noinspection PyBroadException
        try:
            result = await f(token, get_param)
        except BotNotRegisteredError:
            log.warning(f'non registered bot token: {token}')
            return web.json_response({"ok": False, "error_code": 401, "description": f"BotNotRegistered"}, status=401)
        except Exception as e:
            log.exception(f'Exception: {e}')
            return web.json_response({"ok": False, "error_code": 400, "description": f"Error: {e.__class__}: {e}"},
                                     status=400)
        return web.json_response({"ok": True, "result": result})

    return internal


@bot_endpoint_handler
async def handle_bot_get_updates(token: str, get_param: Callable[[str], Optional[str]]):
    timeout = get_param('timeout')
    limit = get_param('limit')
    return await bots_gateway.get_updates(token, int(timeout) if timeout else None, int(limit) if limit else None)


@bot_endpoint_handler
async def handle_bot_send_message(token: str, get_param: Callable[[str], Optional[str]]):
    return await bots_gateway.on_message_received(token, int(get_param('chat_id')), get_param('text'))


loop = asyncio.get_event_loop()
loop.run_until_complete(init())

log = logging.getLogger(__name__)
log.info('Starting')

app = web.Application(loop=loop)
setup_routes()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ConvAI router application")
    parser.add_argument('--path', help='Path to the unix socket file')
    parser.add_argument('--port')
    args = parser.parse_args()

    web.run_app(app, path=args.path, port=args.port)
