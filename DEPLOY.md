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

Render should continue using:

```text
gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -
```

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

The protected `/diagnostics` route reports:

```text
stats.claude_calls
stats.claude_input_tokens
stats.claude_output_tokens
stats.spam_silenced
stats.replies_sent
```

Once the Claude test succeeds, remove the obsolete `GEMINI_API_KEY` from Render.
