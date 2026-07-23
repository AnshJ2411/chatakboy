# Ansh Instagram DM Bot

An Instagram DM auto-responder using:

- Meta's Instagram API with Instagram Login
- Anthropic Claude Haiku 4.5 (`claude-haiku-4-5-20251001`)
- A Render web service

The webhook acknowledges Meta immediately, deduplicates events, silently rejects
spam before it reaches Anthropic, and handles accepted messages in background
workers.

## Paid-model protection

Claude is a paid API. The app therefore applies a free local credit firewall
before every model request:

- duplicate Meta deliveries are ignored;
- a third equivalent message inside ten minutes starts a silent cooldown;
- more than five messages in thirty seconds starts a silent cooldown;
- oversized text, link floods, repeated-character floods, word floods, and
  symbol floods are silently ignored;
- each sender gets at most 20 replies per active session and 60 replies per
  rolling 24 hours;
- the whole bot gets at most 20 Claude calls per minute and 300 per rolling
  24 hours;
- Anthropic SDK retries are disabled, preventing a hidden second paid request
  after an ambiguous timeout;
- output is capped at 120 tokens.

The sender is never told that spam or a spending limit was detected. The bot
simply stops responding. All limits are configurable through Render environment
variables.

## Required secrets

Configure these only in Render's Environment page. Never commit their real
values:

```text
VERIFY_TOKEN
IG_ACCESS_TOKEN
IG_ACCOUNT_ID
META_APP_SECRET
ANTHROPIC_API_KEY
```

The default Claude model is:

```text
claude-haiku-4-5-20251001
```

If the Anthropic key is absent or Claude fails, the app can send a limited
rule-based fallback response rather than crashing. A global or sender credit
limit does not use the fallback; it stays silent.

## Render configuration

The checked-in `render.yaml` declares all non-secret defaults and marks the
secrets with `sync: false`.

Before deploying the provider migration:

1. Add `ANTHROPIC_API_KEY` in Render.
2. Keep the existing Meta secrets unchanged.
3. Deploy the repository.
4. Open `/health` and confirm that `claude` is `configured`.
5. Send one test DM and confirm one `Generating Claude reply` entry followed by
   an Instagram send success.
6. Remove the obsolete `GEMINI_API_KEY` from Render after the Claude test passes.

## Credit-guard settings

```text
CLAUDE_MAX_TOKENS=120
MAX_USER_TEXT_CHARS=1200
SPAM_BURST_WINDOW_SECONDS=30
SPAM_BURST_MAX_MESSAGES=5
SPAM_REPEAT_WINDOW_SECONDS=600
SPAM_REPEAT_MAX_MESSAGES=2
SPAM_COOLDOWN_SECONDS=21600
SESSION_IDLE_SECONDS=3600
SESSION_COOLDOWN_SECONDS=21600
MAX_REPLIES_PER_SESSION=20
MAX_REPLIES_PER_24H=60
MAX_GLOBAL_CLAUDE_CALLS_PER_MINUTE=20
MAX_GLOBAL_CLAUDE_CALLS_PER_24H=300
```

`SPAM_COOLDOWN_SECONDS` and `SESSION_COOLDOWN_SECONDS` default to six hours.
An idle hour starts a fresh conversation session, unless the sender is still in
a cooldown.

## Meta webhook

Use:

```text
Callback URL: https://YOUR-RENDER-SERVICE.onrender.com/webhook
Verify token: the exact VERIFY_TOKEN saved in Render
Subscribed field: messages
```

The Instagram account must be a Creator or Business account with
`instagram_business_manage_messages`.

## Local test

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python app.py
```

Meta cannot send production webhooks to localhost without a secure public
tunnel.

## Privacy and operational limits

- Conversation history, spam state, and spend counters are kept only in process
  memory and reset when Render restarts or sleeps.
- Queued messages are lost if the process stops before processing them.
- Text DMs are supported; media, reactions, and other non-text events are
  ignored.
- Protected diagnostics expose aggregate Claude call and token counters, never
  keys or full Instagram IDs.
