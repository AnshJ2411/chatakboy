# Ansh Instagram DM Bot — free-tier setup

An Instagram DM auto-responder using:

- Meta's Instagram API with Instagram Login
- Google Gemini free tier (`gemini-3.5-flash-lite`)
- A Render free web service

The webhook acknowledges Meta immediately, queues the message, and generates the reply in a background thread. Duplicate webhook deliveries are ignored.

## What is and is not free

- Meta API calls do not require a paid API subscription.
- Gemini Flash-Lite currently has a free tier, but Google applies usage limits and can change them.
- Render has a free web-service plan, but it sleeps after inactivity and has monthly limits.
- You still need free access credentials: an Instagram access token and a Gemini API key. Never commit either one to GitHub.
- This is suitable for testing and light personal use, not guaranteed always-on production traffic.

## 1. Prepare the Instagram account

The Instagram account must be a **Creator** or **Business** account.

In your Meta developer app, use **Instagram API with Instagram Login**, generate the Instagram User access token, and note the account's numeric Instagram ID.

Required permission for messaging:

```text
instagram_business_manage_messages
```

## 2. Create the free Gemini API key

1. Open Google AI Studio.
2. Create an API key in a project that is shown as Free Tier.
3. Do not enable billing if you want to prevent paid usage.
4. Save the key as `GEMINI_API_KEY` in Render.

The default model is:

```text
gemini-3.5-flash-lite
```

If Gemini is unavailable or its quota is exhausted, the app sends a limited rule-based fallback response rather than crashing.

## 3. Put these files in a GitHub repository

Your repository should contain:

```text
app.py
requirements.txt
render.yaml
.env.example
README.md
```

Do not upload a real `.env` file or any actual token.

## 4. Deploy on Render for free

1. Sign in to Render and create a **Blueprint** from the GitHub repository containing `render.yaml`.
2. Select the Free instance type if Render asks.
3. Enter the secret environment variables when prompted:

```text
VERIFY_TOKEN
IG_ACCESS_TOKEN
IG_ACCOUNT_ID
META_APP_SECRET
GEMINI_API_KEY
```

`VERIFY_TOKEN` is any long random value you invent. The same exact value must be entered in Meta's webhook settings.

`META_APP_SECRET` is found in the Meta app dashboard under **App settings → Basic**. It allows the code to reject forged webhook requests.

Render starts the app with one Gunicorn process intentionally. The queue and conversation history are stored in process memory, so using multiple processes would split that state.

After deployment, open:

```text
https://YOUR-RENDER-SERVICE.onrender.com/health
```

A correct response should show no missing configuration and `gemini` as `configured`.

## 5. Configure the Meta webhook

In the Meta app dashboard, open the Instagram webhook configuration.

Use:

```text
Callback URL: https://YOUR-RENDER-SERVICE.onrender.com/webhook
Verify token: the exact VERIFY_TOKEN saved in Render
```

Click **Verify and save**.

Subscribe to the Instagram webhook field:

```text
messages
```

Then enable the **Webhook subscription** checkbox beside the Instagram account for which you generated the access token.

## 6. Test it

1. Keep the Meta app in development mode for initial testing.
2. DM the professional account from an allowed tester/test Instagram account.
3. Open the Render logs.
4. A successful flow logs that the event was accepted and later that a reply was sent.

Because Render's free service can sleep after inactivity, the first webhook after a long idle period may be delayed while the service starts. Meta may retry the event; the code deduplicates those retries.

## 7. Important testing restriction

While the Meta app is in development mode, only app-role/tester accounts can normally exercise the integration. To reply to ordinary public users, request the required permission through Meta App Review and move the app live when approved.

## 8. Token lifetime

The dashboard token may be short-lived. Exchange it for a long-lived Instagram User token before relying on the deployment:

```bash
curl -i -X GET "https://graph.instagram.com/access_token?grant_type=ig_exchange_token&client_secret=YOUR_META_APP_SECRET&access_token=YOUR_SHORT_LIVED_TOKEN"
```

Save the returned `access_token` as `IG_ACCESS_TOKEN` in Render, then redeploy/restart the service.

Do not print or paste the token into public logs, screenshots, GitHub, or chat messages.

## Local test

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
# Load the environment variables using your shell or an env manager.
python app.py
```

Meta cannot send real webhooks to `localhost`; local execution is only for health-route and code testing unless you use a secure public tunnel.

## Current limitations

- Conversation history resets whenever Render restarts or sleeps.
- Queued messages are lost if the process stops before they are handled.
- Text DMs are supported; media, reactions and other non-text events are ignored.
- Free tiers have quotas and are not an always-on service guarantee.
