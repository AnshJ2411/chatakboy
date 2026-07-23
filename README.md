# Ansh Instagram DM Bot

An Instagram DM auto-responder using:

- Meta's Instagram API with Instagram Login
- Anthropic Claude Haiku 4.5 (`claude-haiku-4-5-20251001`)
- A Render web service

The webhook acknowledges Meta immediately, deduplicates events, silently rejects
spam before it reaches Anthropic, briefly combines rapid follow-up DMs from the
same sender, and handles accepted conversations in per-sender background queues.

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
- an originality check can make one additional paid Claude request only when
  the first draft repeats a recent or blocked reply; if that replacement is
  still repetitive, the bot stays silent;
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

If Claude fails, times out, returns an empty response, or cannot produce an
original replacement, the production bot stays silent instead of sending a
canned fallback. A global or sender credit limit also stays silent.
`ALLOW_LOCAL_FALLBACK` defaults to `false`; its small fallback is for offline
development only and must not be enabled on Render.

## Conversation quality and pacing

- `MAX_TURNS=20` supplies up to ten recent user/assistant exchanges to Claude.
- Messages from the same sender are processed in order. DMs arriving within
  `0.8` seconds are combined into one turn, which avoids unnecessary paid calls
  for natural double texts.
- The first reply is timed to arrive about `2.5` to `8.5` seconds after the
  latest accepted DM was received. Claude processing time counts toward this
  total, so the delay does not stack on top of a slow model response.
- When Claude intentionally returns a two-bubble reply, the second bubble waits
  `1.0` to `3.2` seconds.
- A private aggressive mood mode has a `0.13` chance on an eligible turn and a
  minimum gap of five persona turns. It remains contextual and is never
  announced to the sender.
- A 250-entry, 24-hour recent-reply cache detects repeated bot phrasing across
  conversations. The cache stores bot output fingerprints, not incoming DMs.

These timings are for perceived human pacing; the webhook still acknowledges
Meta immediately.

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
MAX_TURNS=20
WORKER_THREADS=6
MESSAGE_COALESCE_SECONDS=0.8
MIN_REPLY_DELAY_SECONDS=2.5
MAX_REPLY_DELAY_SECONDS=8.5
DOUBLE_TEXT_DELAY_MIN_SECONDS=1.0
DOUBLE_TEXT_DELAY_MAX_SECONDS=3.2
OFFENSIVE_FLIP_CHANCE=0.13
OFFENSIVE_FLIP_MIN_GAP=5
RECENT_REPLY_CACHE_SIZE=250
RECENT_REPLY_TTL_SECONDS=86400
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
MAX_SEEN_EVENTS=10000
```

`SPAM_COOLDOWN_SECONDS` and `SESSION_COOLDOWN_SECONDS` default to six hours.
An idle hour starts a fresh conversation session, unless the sender is still in
a cooldown. The optional originality retry is a real Claude call and therefore
counts toward the same per-minute and rolling 24-hour global call limits.

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

- The bot keeps at most ten recent user/assistant exchanges per sender. This
  conversation history, the recent-reply cache, spam state, and spend counters
  live only in process memory and reset when Render restarts or sleeps.
- Because in-process spend counters reset with the service, set a hard monthly
  workspace/key spend limit in Anthropic as the final billing backstop. The
  application limits reduce abuse but are not a substitute for a provider cap.
- Render's free plan sleeps and is not strict 24/7 hosting. Use an always-on
  instance if immediate replies and durable in-memory limits matter.
- Queued messages are lost if the process stops before processing them.
- Text DMs are supported; media, reactions, and other non-text events are
  ignored.
- Protected diagnostics expose aggregate Claude call and token counters, never
  keys or full Instagram IDs.
