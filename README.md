# ConvAI Router Bot

The ConvAI Router Bot is an implementaton of proxy bot connecting people to the chabots. The people are communicating 
with proxy bot through instant messengers. Right now the system supports Telegram & Facebook Messneger API for proxy-bot
exposion. 

The bots are connected to proxy bot using simplified version of Telergam API. It supports only `/getUpdates` and `/sendMessage`
methods. 

## Importing profiles

Person profiles are imported via:
```
python system_monitor.py import-profiles <profiles_file_path>
```

Profile files contains person profiles in raw string format. Sentences in each profiles are delimited by newline (`\n`) symbol and profiles are delimited by empty line (`\n\n` symbols sequence).

Each profile can have several topics for discussion. Topics' sentences are delimited from profile sentences by `[:topic:]` string without any empty lines.

Linked profiles are delimited by `[:linked:]` string without any empty lines. Groups of linked profiles delimited by empty strings from each other.