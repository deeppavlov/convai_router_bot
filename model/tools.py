import re
from io import BytesIO
from pathlib import Path

from textwrap import wrap
from PIL import Image as Im, ImageDraw, ImageFont

path_to_font = Path(__file__).resolve().parents[0] / 'arial.ttf'

def get_image_from_text(text: str, topic: bool = False) -> bytes:
    """
    Generates jpeg image as bytes array for profile description and profile topic.
    :param text:
    :return:
    """
    font_size = 50
    bg_color = (255, 255, 255)
    fnt_color = (0, 0, 0)
    if topic:
        lines = wrap(text, width=25)
    else:
        lines = re.findall(r'(.+?)\.', text)
    fnt = ImageFont.truetype(str(path_to_font), font_size)
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
