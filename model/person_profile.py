from typing import List

from mongoengine import *


class PersonProfile(Document):
    """Profile assigned to the conversation participant"""
    sentences: List[str] = ListField(StringField(), required=True)

    @property
    def description(self) -> str:
        return '\n'.join(self.sentences)
