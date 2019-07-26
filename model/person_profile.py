from typing import List, Dict, Optional

from mongoengine import *

from .image import Image


class PersonProfile(Document):
    """Profile assigned to the conversation participant"""
    sentences: List[str] = ListField(StringField(), required=True)
    link_uuid: Optional[str] = StringField(required=True)
    topics: Optional[List[str]] = ListField(StringField(), required=False)
    sentences_image: Optional[Image] = ObjectIdField(Image)
    topics_images: Optional[Dict[int, Image]] = DictField()

    @property
    def description(self) -> str:
        return '\n'.join(self.sentences)
