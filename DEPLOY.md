# Instagram DM bot — deploy and verify

## Before committing the provider migration

Add this secret in Render without pasting it into GitHub, logs, screenshots, or
chat:

```text
ANTHROPIC_API_KEY
```

Keep the existing Meta secrets:

```text
VERIFY_TOKEN
IG_ACCESS_TOKEN
IG_ACCOUNT_ID
META_APP_SECRET
DIAGNOSTIC_TOKEN
```

The non-secret provider defaults are:

```text
CLAUDE_MODEL=claude-haiku-4-5-20251001
CLAUDE_MAX_TOKENS=120
```

The non-secret conversation/runtime defaults are:

```text
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
```

Render should continue using:

```text
gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -
```

Use `/ready` as Render's health-check path. It returns `503` when any production
secret, including `ANTHROPIC_API_KEY`, is missing.

## Verify

After Render deploys, open:

```text
https://ansh-instagram-dm-bot.onrender.com/health
```

It must show:

```json
{"status":"ok","missing":[],"claude":"configured"}
```

Send one fresh DM. A successful log sequence contains:

```text
Webhook signature valid=True
DM queued ...
Generating Claude reply model=claude-haiku-4-5-20251001
Sending Instagram reply ...
Instagram reply sent status=200 message_id_present=True
```

Spam is intentionally quiet. When a local guard blocks a message, logs contain
`Silently ignored DM before queue` or `Silently stopped DM`; the sender receives
nothing and no Claude request is made.

Normal DMs from one sender are handled in order. Messages received within `0.8`
seconds are combined into one Claude turn. The first reply arrives about `2.5`
to `8.5` seconds after the latest accepted DM, including Claude's own response
time; a deliberate second bubble waits another `1.0` to `3.2` seconds.

The originality guard may make exactly one additional paid Claude request when
the first draft repeats recent wording. Both calls count toward the global
credit limits. If the replacement is still repetitive, or if Claude fails,
times out, or returns empty output, the bot stays silent rather than sending a
canned fallback.

The protected `/diagnostics` route reports:

```text
stats.claude_calls
stats.claude_input_tokens
stats.claude_output_tokens
stats.spam_silenced
stats.replies_sent
stats.novelty_retries
stats.repeated_drafts_rejected
stats.messages_coalesced
stats.silent_failures
```

`MAX_TURNS=20` means at most ten recent user/assistant exchanges are supplied to
Claude. That history and the 250-entry, 24-hour recent bot-reply cache are
in-memory only; both reset whenever Render restarts or sleeps.

The in-process call counters also reset on restart, so configure a hard
workspace/key spend limit in Anthropic. Render's free plan sleeps; an always-on
instance is required for strict 24/7 response availability.

Once the Claude test succeeds, remove the obsolete `GEMINI_API_KEY` from Render.
