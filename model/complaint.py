from mongoengine import *

from model.conversation import Conversation
from .bot import Bot
from .user import User


class Complaint(Document):
    """A single insult complaint"""

    complainer = ReferenceField(User, required=True)
    complain_to = GenericReferenceField(choices=[User, Bot], required=True)
    conversation = ReferenceField(Conversation)

    processed = BooleanField(default=False)

    meta = {'indexes': ['processed', 'complainer', 'complain_to']}
