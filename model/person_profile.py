from typing import List, Optional

from mongoengine import *

from .image import Image
from .tools import get_image_from_text

class PersonProfile(Document):
    """Profile assigned to the conversation participant"""
    sentences: List[str] = ListField(StringField(), required=True)
    link_uuid: Optional[str] = StringField(required=True)
    topics: Optional[List[str]] = ListField(StringField(), required=False)
    sentences_image: str = ObjectIdField()
    topics_images: Optional[List[str]] = ListField()

    @property
    def description(self) -> str:
        return '\n'.join(self.sentences)

    @property
    def description_image(self) -> Image:
        if not self.sentences_image:
            img = Image()
            img.binary = get_image_from_text(self.description)
            img.save()
            self.sentences_image = img.id
            self.save()
        return Image.objects(id=self.sentences_image)[0]

    def get_topic_image(self, topic_num) -> Image:
        if len(self.topics_images) <= topic_num:
            img = Image()
            img.binary = get_image_from_text(self.topics[topic_num], True)
            img.save()
            self.topics_images.append(img.id)
            self.save()
        return Image.objects(id=self.topics_images[topic_num])[0]
