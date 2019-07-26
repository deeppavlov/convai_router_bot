from typing import Optional

from mongoengine import *


class Image(Document):
    """Image for profile and topic description"""
    binary: Optional[bytes] = BinaryField()
    tg_id: Optional[str] = StringField()
