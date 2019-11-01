from typing import Any

from mongoengine import Document, DynamicField, StringField


class Settings(Document):
    name: str = StringField(required=True)
    value: Any = DynamicField(required=True)
