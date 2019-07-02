from mongoengine import *

from .bot import Bot
from .user import User


class Message(EmbeddedDocument):
    """A single message in a conversation"""

    msg_id = IntField(required=True, unique=True)
    text = StringField(required=True)
    sender = GenericReferenceField(choices=[User, Bot], required=True)
    time = DateTimeField(required=True)
    evaluation_score = IntField(max_value=1, min_value=0)
    system = BooleanField(required=True, default=False)
