from typing import List, Optional

from mongoengine import *


class PersonProfile(Document):
    """Profile assigned to the conversation participant"""
    sentences: List[str] = ListField(StringField(), required=True)
    link_uuid: Optional[str] = StringField(required=False)

    @property
    def description(self) -> str:
        return '\n'.join(self.sentences)
