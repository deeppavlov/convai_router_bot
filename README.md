# ConvAI Router Bot

The ConvAI Router Bot is an implementaton of proxy bot connecting people to the chatbots. The people are communicating 
with proxy bot through instant messengers. Right now the system supports Telegram & Facebook Messenger API for proxy-bot
exposion. 

The bots are connected to proxy bot using simplified version of Telergam API. It supports only `/getUpdates` and `/sendMessage`
methods. 

## Importing profiles

Person profiles are imported via:
```shell script
python system_monitor.py import-profiles <profiles_file_path>
```

`system_monitor.py` supports `yaml` and `json` profiles files. Profiles file should contain list of linked profiles lists.
Each profile is a dict with following keys:
- `persona (List[str])` - Profile description.
- `tags (Optional[List[str]])` - Profile tags.
- `topics (Optional[List[str]])` - Profile topics to discuss.

For example, the following list will link `Profile A` with `Profile B`, `Profile C` with `Profile D`:
```python
[[{'persona': ['Profile A']}, {'persona': ['Profile B', 'Second sentence']}], [{'persona': ['Profile C']}, {'persona': ['Profile D']}]]
```

## Managing active profile tags

Active profile tags list is modified via:
```shell script
python system_monitor.py manage-tags <command> [<tag>]
```
- `<command>` - `add` to add `<tag>` to list, `remove` to remove `<tag>` from list and `list` to get active tags list.
- `<tag>` - tag name, mandatory parameter for `add` and `remove` commands.