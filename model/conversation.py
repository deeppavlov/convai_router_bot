from datetime import datetime
from typing import List

from mongoengine import EmbeddedDocumentField, EmbeddedDocumentListField, DateTimeField, Document, ValidationError, \
    IntField, UUIDField

from .conversation_peer import ConversationPeer
from .message import Message


class Conversation(Document):
    """A single conversation between 2 participants"""

    conversation_id: int = IntField(required=True, unique=True)
    conversation_uuid = UUIDField()
    participant1: ConversationPeer = EmbeddedDocumentField(ConversationPeer, required=True)
    participant2: ConversationPeer = EmbeddedDocumentField(ConversationPeer, required=True)
    messages: List[Message] = EmbeddedDocumentListField(Message, required=True)
    start_time: datetime = DateTimeField(required=True)
    end_time: datetime = DateTimeField(required=True)

    @property
    def participants(self) -> List[ConversationPeer]:
        return [self.participant1, self.participant2]

    def clean(self):
        """Ensures that the conversation is not empty. Also sets start_time and end_time fields """

        if len(self.messages) == 0:
            raise ValidationError('Conversation can not be empty')

        self.start_time = min(map(lambda x: x.time, self.messages))
        self.end_time = max(map(lambda x: x.time, self.messages))
