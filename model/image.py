from typing import Optional

from mongoengine import *


class Image(Document):
    """This is general purpose image storage, not only for profile and topic description"""
    binary: Optional[bytes] = BinaryField()
    telegram_id: Optional[str] = StringField()
