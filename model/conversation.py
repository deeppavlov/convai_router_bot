from datetime import datetime
from typing import List, Union, Optional

from mongoengine import EmbeddedDocumentField, EmbeddedDocumentListField, DateTimeField, Document, ValidationError, \
    IntField

from .conversation_peer import ConversationPeer
from .user import User
from .bot import Bot
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
    messages_to_switch_topic = IntField(required=True, default=0)
    messages_to_switch_topic_left = IntField(required=True, default=0)

    @property
    def participants(self) -> List[ConversationPeer]:
        return [self.participant1, self.participant2]

    def clean(self):
        """Ensures that the conversation is not empty. Also sets start_time and end_time fields """

        if len(self.messages) == 0:
            raise ValidationError('Conversation can not be empty')

        self.start_time = min(map(lambda x: x.time, self.messages))
        self.end_time = max(map(lambda x: x.time, self.messages))

    def add_message(self, text: str, sender: Union[Bot, User], time: Optional[datetime] = None,
                    system: Optional[bool] = False) -> Message:
        time = time or datetime.utcnow()
        message = Message(msg_id=len(self.messages),
                          text=text,
                          sender=sender,
                          time=time,
                          system=system)

        self.messages.append(message)

        if not system:
            self.messages_to_switch_topic_left -= 1 if self.messages_to_switch_topic_left > 0 else 0

        return message

    def reset_topic_switch_counter(self) -> None:
        self.messages_to_switch_topic_left = self.messages_to_switch_topic

    def next_topic(self) -> int:
        p1_topics = self.participant1.assigned_profile.topics or []
        p2_topics = self.participant2.assigned_profile.topics or []

        p1_topics_n = len(p1_topics)
        p2_topics_n = len(p2_topics)

        if self.active_topic_index + 1 < min(p1_topics_n, p2_topics_n):
            if self.messages_to_switch_topic_left <= 0:
                self.active_topic_index += 1
                self.reset_topic_switch_counter()
                return 0
            else:
                return self.messages_to_switch_topic_left
        else:
            return -1
