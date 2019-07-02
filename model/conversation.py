from datetime import datetime
from typing import List

from mongoengine import EmbeddedDocumentField, EmbeddedDocumentListField, DateTimeField, Document, ValidationError, \
    IntField

from .conversation_peer import ConversationPeer
from .message import Message


class Conversation(Document):
    """A single conversation between 2 participants"""

    conversation_id: int = IntField(required=True, unique=True)
    participant1: ConversationPeer = EmbeddedDocumentField(ConversationPeer, required=True)
    participant2: ConversationPeer = EmbeddedDocumentField(ConversationPeer, required=True)
    messages: List[Message] = EmbeddedDocumentListField(Message, required=True)
    start_time: datetime = DateTimeField(required=True)
    end_time: datetime = DateTimeField(required=True)
    active_topic_index: int = IntField(required=True, default=0)

    @property
    def participants(self) -> List[ConversationPeer]:
        return [self.participant1, self.participant2]

    def clean(self):
        """Ensures that the conversation is not empty. Also sets start_time and end_time fields """

        if len(self.messages) == 0:
            raise ValidationError('Conversation can not be empty')

        self.start_time = min(map(lambda x: x.time, self.messages))
        self.end_time = max(map(lambda x: x.time, self.messages))

    def next_topic(self) -> bool:
        p1_topics_n = len(self.participant1.assigned_profile.topics)
        p2_topics_n = len(self.participant2.assigned_profile.topics)

        if self.active_topic_index + 1 < p1_topics_n and self.active_topic_index + 1 < p2_topics_n:
            self.active_topic_index += 1
            return True
        else:
            return False
