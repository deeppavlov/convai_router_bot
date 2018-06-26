import collections

from model import BannedPair, Bot, Complaint, Conversation, ConversationPeer, Message, PersonProfile, User


class HumanReadable:
    level_fill: str

    def __init__(self, level_fill='  ', *args, **kwargs):
        super(HumanReadable, self).__init__(*args, **kwargs)
        self.level_fill = level_fill

    def format_entity(self, e, level=0):
        if isinstance(e, BannedPair):
            return self.format_banned_pair(e, level)
        elif isinstance(e, Bot):
            return self.format_bot(e, level)
        elif isinstance(e, Complaint):
            return self.format_complaint(e, level)
        elif isinstance(e, Conversation):
            return self.format_conversation(e, level)
        elif isinstance(e, ConversationPeer):
            return self.format_conversation_peer(e, level)
        elif isinstance(e, Message):
            return self.format_message(e, level)
        elif isinstance(e, PersonProfile):
            return self.format_profile(e, level)
        elif isinstance(e, User):
            return self.format_user(e, level)
        elif isinstance(e, collections.Iterable):
            return self.format_iterable(e, level)
        else:
            raise ValueError(f"Could not format {e}. Check it's class")

    def format_banned_pair(self, bp, level=0):
        res = self._format_lines(['User:'], level) + '\n'
        res += self.format_user(bp.user, level + 1) + '\n'
        res += self._format_lines(['Bot:'], level) + '\n'
        res += self.format_bot(bp.bot, level + 1)
        return res

    def format_bot(self, b, level=0):
        lines = [f'Token: {b.token}',
                 f'Bot name: {b.bot_name}']
        if b.banned:
            lines.append("Banned!")
        return self._format_lines(lines, level)

    def format_complaint(self, c, level=0):
        res = self._format_lines(['Complainer:'], level) + '\n'
        res += self.format_user(c.complainer, level + 1) + '\n'
        res += self._format_lines(['Complain to:'], level) + '\n'
        res += self.format_entity(c.complain_to, level + 1) + '\n'
        res += self._format_lines([f'Processed: {c.processed}'], level) + '\n'
        res += self._format_lines(['Conversation:'], level) + '\n'
        res += self.format_conversation(c.conversation, level + 1)
        return res

    def format_conversation(self, c, level=0):
        res = self._format_lines([f'Conversation ID: {c.conversation_id}'], level) + '\n'
        res += self._format_lines(['Participants:'], level) + '\n'
        res += self.format_iterable(c.participants, level + 1) + '\n'
        res += self._format_lines([f'Start time: {c.start_time}',
                                   f'End time: {c.end_time}'], level) + '\n'
        res += self._format_lines(['Messages:'], level) + '\n'
        res += self.format_iterable(c.messages, level + 1)
        return res

    def format_conversation_peer(self, cp, level=0):
        res = self._format_lines(['Peer:'], level) + '\n'
        res += self.format_entity(cp.peer, level + 1) + '\n'
        res += self._format_lines(['Assigned profile:'], level) + '\n'
        res += self.format_profile(cp.assigned_profile, level + 1) + '\n'
        if cp.dialog_evaluation_score is not None:
            res += self._format_lines([f'Given dialog score: {cp.dialog_evaluation_score}'], level) + '\n'
        if cp.other_peer_profile_options is not None:
            res += self._format_lines(['Other peer profile options:'], level) + '\n'
            res += self.format_iterable(cp.other_peer_profile_options, level + 1) + '\n'
        if cp.other_peer_selected_profile_assembled is not None:
            res += self._format_lines([f'Selected other peer profile:'], level) + '\n'
            res += self.format_profile(cp.other_peer_selected_profile_assembled, level + 1)

        return res

    def format_message(self, m, level=0):
        lines = [f'ID: {m.msg_id}']
        sender = m.sender.username if isinstance(m.sender, User) else m.sender.bot_name
        lines += [f'From: {sender}',
                  f'Time: {m.time}']
        if m.evaluation_score is not None:
            lines.append(f'Evaluation: {m.evaluation_score}')
        lines.append('Text:')
        return self._format_lines(lines, level) + '\n' + self._format_lines(m.text.split('\n'), level + 1)

    def format_profile(self, p, level=0):
        lines = [f'Profile {p.id}'] + p.description.split('\n')
        return self._format_lines(lines, level)

    def format_user(self, u, level=0):
        lines = [f'Platform: {u.user_key.platform}',
                 f'ID: {u.user_key.user_id}',
                 f'Username: {u.username}']
        if u.banned:
            lines.append("Banned!")
        return self._format_lines(lines, level)

    def format_iterable(self, iterable, level=0):
        return '\n\n'.join([self.format_entity(e, level) for e in iterable])

    def _format_lines(self, lines, level):
        return '\n'.join(map(lambda x: self.level_fill * level + x, lines))
