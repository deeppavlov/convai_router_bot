import re
import csv
from pathlib import Path


class MessagesWrapper:
    def __init__(self, messages_tsv_file_path: Path) -> None:
        self.messages = {}

        with messages_tsv_file_path.open('r') as f_messages:
            tsv_reader = csv.reader(f_messages, delimiter='\t')

            messages = [line for line in tsv_reader]
            for message in messages[1:]:
                self.messages[message[0]] = message[1]

    def __call__(self, message_type: str, *args) -> str:
        message = self.messages.get(message_type, '')
        format_values = [str(arg) for arg in args]
        format_placeholders = re.findall(r'\{\}', message)
        delta = len(format_placeholders) - len(format_values)

        if delta > 0:
            format_values.extend([''] * delta)

        result = message if len(format_values) == 0 else message.format(*format_values)

        return result
