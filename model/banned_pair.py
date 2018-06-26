from mongoengine import *

from .bot import Bot
from .user import User


class BannedPair(Document):
    """A User-Bot pair that is prohibited from having conversations between each other"""

    user = ReferenceField(User, required=True, unique_with=['bot'])
    bot = ReferenceField(Bot, required=True)
