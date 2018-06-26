from typing import Union, List, Optional

from mongoengine import *

from .person_profile import PersonProfile
from .bot import Bot
from .user import User


class ConversationPeer(EmbeddedDocument):
    peer: Union[Bot, User] = GenericReferenceField(choices=[Bot, User], required=True)
    assigned_profile: PersonProfile = ReferenceField(PersonProfile, required=True)
    dialog_evaluation_score: int = IntField(max_value=5, min_value=1)
    other_peer_profile_options: List[PersonProfile] = ListField(ReferenceField(PersonProfile))
    other_peer_profile_selected: Optional[PersonProfile] = ReferenceField(PersonProfile)
    other_peer_profile_selected_parts: List[str] = ListField(StringField())

    @property
    def other_peer_selected_profile_assembled(self) -> Optional[PersonProfile]:
        if self.other_peer_profile_selected is not None:
            return self.other_peer_profile_selected
        if self.other_peer_profile_selected_parts:
            return PersonProfile(sentences=self.other_peer_profile_selected_parts, id='<assembled>')
        return None
