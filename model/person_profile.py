import re
from io import BytesIO
from typing import List, Dict, Optional
from textwrap import wrap
from PIL import Image as Im, ImageDraw, ImageFont

from mongoengine import *

from .image import Image


class PersonProfile(Document):
    """Profile assigned to the conversation participant"""
    sentences: List[str] = ListField(StringField(), required=True)
    link_uuid: Optional[str] = StringField(required=True)
    topics: Optional[List[str]] = ListField(StringField(), required=False)
    sentences_image: str = ObjectIdField()
    topics_images: Optional[Dict[int, Image]] = DictField()

    @property
    def description(self) -> str:
        return '\n'.join(self.sentences)

    @property
    def description_image(self) -> Image:
        if not self.sentences_image:
            img = Image()
            img.binary = self.get_image_from_text(self.description)
            img.save()
            self.sentences_image = img.id
        return Image.objects(id=self.sentences_image)[0]

    @staticmethod
    def get_image_from_text(text: str) -> bytes:
        """
        Generates jpeg image as bytes array for profile description and profile topic.
        :param text:
        :return:
        """
        font_size = 50
        bg_color = (255, 255, 255)
        fnt_color = (0, 0, 0)
        if text.find('Тема:') > -1:
            lines = re.match('(.*?)(\(.*)', text).groups()
            lines = [lines[0]] + wrap(lines[1], width=25)
        else:
            lines = re.findall(r'(.+?)\.', text)
        fnt = ImageFont.truetype('/home/ubuntu/remote_development/model/arial.ttf', font_size)
        width = max([fnt.getsize(line)[0] for line in lines]) + 20
        height = max(width // 2, len(lines) * font_size + 20)
        img = Im.new('RGB', (width, height), color=bg_color)
        d = ImageDraw.Draw(img)
        for i, line in enumerate(lines):
            x_begin = (width - fnt.getsize(line)[0]) // 2
            d.text((x_begin, 10 + i * font_size), line, font=fnt, fill=fnt_color)
        output = BytesIO()
        img.save(output, format='JPEG')
        return output.getvalue()
