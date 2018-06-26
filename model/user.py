from mongoengine import EmbeddedDocument, StringField, DynamicDocument, BooleanField, EmbeddedDocumentField


class UserPK(EmbeddedDocument):
    """Pair of platform and user id within this platform, that is used to uniquely identify the user within our
    system"""

    PLATFORM_FACEBOOK = 'Facebook'
    PLATFORM_TELEGRAM = 'Telegram'
    PLATFORM_CHOICES = [PLATFORM_FACEBOOK, PLATFORM_TELEGRAM]

    user_id: str = StringField(required=True)
    platform: str = StringField(required=True, choices=PLATFORM_CHOICES)

    def __hash__(self):
        return self.user_id.__hash__() ^ self.platform.__hash__()

    def __repr__(self):
        return f'UserPK[{self.platform}; {self.user_id}]'

    def __str__(self):
        return f'\{{{self.platform}; {self.user_id}}}]'


class User(DynamicDocument):
    """Human chats participant"""

    user_key: UserPK = EmbeddedDocumentField(UserPK, unique=True, required=True)
    username: str = StringField()
    banned: bool = BooleanField(default=False)

    def __repr__(self):
        return f'User[{repr(self.user_key)}]'

    def __str__(self):
        return repr(self)
