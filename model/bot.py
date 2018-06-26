from mongoengine import *


class Bot(Document):
    """Bot chats participant"""

    token: str = StringField(required=True, primary_key=True)
    bot_name: str = StringField(required=True)
    banned: bool = BooleanField(default=False)
    last_update_id: int = IntField(default=1)
