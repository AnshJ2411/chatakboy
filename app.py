"""Instagram DM auto-responder using Meta webhooks + Gemini's free tier.

The webhook route only validates and queues incoming messages, then returns to
Meta immediately. Background worker threads generate and send replies.

Designed for one Gunicorn process. Use the start command in render.yaml/README.
Conversation history and duplicate tracking are in memory and reset on restart.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import queue
import random
import threading
import time
from collections import defaultdict
from typing import Any

import requests
from flask import Flask, Response, jsonify, request
from google import genai
from google.genai import types

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ig-bot")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "").strip()
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "")
IG_ACCOUNT_ID = os.getenv("IG_ACCOUNT_ID", "")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")  # recommended, optional for first test

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")

GRAPH_API_VERSION = os.getenv("GRAPH_API_VERSION", "v25.0")
MAX_TURNS = max(2, int(os.getenv("MAX_TURNS", "12")))
WORKER_COUNT = max(1, min(4, int(os.getenv("WORKER_COUNT", "2"))))
QUEUE_SIZE = max(10, int(os.getenv("QUEUE_SIZE", "500")))
DEDUPE_TTL_SECONDS = max(3600, int(os.getenv("DEDUPE_TTL_SECONDS", "172800")))

SEND_URL = (
    f"https://graph.instagram.com/{GRAPH_API_VERSION}/"
    f"{IG_ACCOUNT_ID}/messages"
)

http = requests.Session()
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ---------------------------------------------------------------------------
# Persona
# ---------------------------------------------------------------------------
ANSH_SYSTEM_PROMPT = """You are the automated Instagram DM assistant for Ansh, a Delhi-based AI content creator focused on character IPs, LoRA training, and video pipelines. Match Ansh's casual texting style while staying honest if directly asked whether this is automated.

MECHANICS
- Mostly lowercase and minimal punctuation.
- Usually answer in 2-8 words. Use a slightly longer answer for serious or work questions.
- Use "u", "ur", "im", "dont", "cant", and "ill" naturally.
- Occasional stretched words or ALL-CAPS for emphasis, but do not overdo it.

VOICE
- Dry, reactive, relaxed, and friendly-but-guarded with unfamiliar people.
- The default reaction emoji is 😭. Use at most one emoji per message unless genuinely hyped.
- Light casual swearing is rare and never directed at the other person.
- Short Hindi phrases are okay when natural.

RULES
- Never claim to be human if directly asked. Say you are Ansh's automated DM assistant.
- Do not invent personal facts, promises, prices, availability, meetings, or commitments.
- For business enquiries, ask for the deliverable, deadline, and budget when relevant.
- Do not request passwords, OTPs, payment-card data, or other sensitive credentials.
- Keep replies non-romantic and non-sexual.
- Output only the DM reply, with no labels or explanation.
"""

# ---------------------------------------------------------------------------
# In-memory state (sufficient for testing and a single free web process)
# ---------------------------------------------------------------------------
message_queue: queue.Queue[dict[str, str]] = queue.Queue(maxsize=QUEUE_SIZE)
conversations: dict[str, list[dict[str, str]]] = {}
conversation_lock = threading.RLock()
user_locks: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)

seen_events: dict[str, float] = {}
seen_events_lock = threading.Lock()
workers_started = False
workers_started_lock = threading.Lock()


def missing_required_config() -> list[str]:
    missing = []
    for name, value in (
        ("VERIFY_TOKEN", VERIFY_TOKEN),
        ("IG_ACCESS_TOKEN", IG_ACCESS_TOKEN),
        ("IG_ACCOUNT_ID", IG_ACCOUNT_ID),
    ):
        if not value:
            missing.append(name)
    return missing


def validate_signature(raw_body: bytes, supplied_signature: str | None) -> bool:
    """Validate Meta's X-Hub-Signature-256 when META_APP_SECRET is configured."""
    if not META_APP_SECRET:
        return True
    if not supplied_signature or not supplied_signature.startswith("sha256="):
        return False

    expected = "sha256=" + hmac.new(
        META_APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, supplied_signature)


def event_key(sender_id: str, message: dict[str, Any], event: dict[str, Any]) -> str:
    """Use Meta's message ID where available; otherwise derive a stable retry key."""
    mid = message.get("mid")
    if mid:
        return str(mid)

    material = "|".join(
        (
            sender_id,
            str(event.get("timestamp", "")),
            str(message.get("text", "")),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def reserve_event(key: str) -> bool:
    """Return False when the event was already seen within the dedupe window."""
    now = time.time()
    cutoff = now - DEDUPE_TTL_SECONDS

    with seen_events_lock:
        # Lazy cleanup prevents this dictionary growing forever.
        if len(seen_events) > 2000:
            expired = [event_id for event_id, seen_at in seen_events.items() if seen_at < cutoff]
            for event_id in expired:
                seen_events.pop(event_id, None)

        if seen_events.get(key, 0) >= cutoff:
            return False
        seen_events[key] = now
        return True


def release_event(key: str) -> None:
    """Allow Meta to retry an event that could not be queued."""
    with seen_events_lock:
        seen_events.pop(key, None)


def fallback_reply(user_text: str) -> str:
    """Very small no-AI fallback used if Gemini is unavailable or quota is exhausted."""
    text = user_text.strip().lower()
    if any(word in text for word in ("hi", "hii", "hello", "hey")):
        return random.choice(("hii whats up", "heyy", "yo whats good"))
    if any(word in text for word in ("price", "cost", "rate", "budget")):
        return "send the details n budget"
    if any(word in text for word in ("collab", "work", "project", "business")):
        return "send the brief n deadline"
    if "?" in user_text:
        return "lemme check dis"
    return random.choice(("gotchu", "fs", "say more", "hmm 😭"))


def generate_reply(sender_igsid: str, user_text: str) -> str:
    """Generate a reply with Gemini; keep history consistent per Instagram user."""
    with conversation_lock:
        history = list(conversations.get(sender_igsid, []))

    history.append({"role": "user", "content": user_text})
    history = history[-MAX_TURNS:]

    if not gemini_client:
        log.warning("GEMINI_API_KEY is missing; using the basic fallback reply")
        reply_text = fallback_reply(user_text)
    else:
        contents = [
            types.Content(
                role="model" if turn["role"] == "assistant" else "user",
                parts=[types.Part(text=turn["content"])],
            )
            for turn in history
        ]

        try:
            response = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=ANSH_SYSTEM_PROMPT,
                    max_output_tokens=160,
                    temperature=0.9,
                ),
            )
            reply_text = (response.text or "").strip()
            if not reply_text:
                reply_text = fallback_reply(user_text)
        except Exception:
            log.exception("Gemini generation failed; using the basic fallback")
            reply_text = fallback_reply(user_text)

    # Instagram text messages have a finite length. Persona replies should be
    # much shorter, but this guards against an accidental long model response.
    reply_text = reply_text[:900].strip() or "hmm"

    history.append({"role": "assistant", "content": reply_text})
    with conversation_lock:
        conversations[sender_igsid] = history[-MAX_TURNS:]

    return reply_text


def send_message(recipient_igsid: str, text: str) -> None:
    """Send one Instagram message, retrying only transient failures."""
    if not IG_ACCESS_TOKEN or not IG_ACCOUNT_ID:
        raise RuntimeError("IG_ACCESS_TOKEN and IG_ACCOUNT_ID must be configured")

    payload = {"recipient": {"id": recipient_igsid}, "message": {"text": text}}

    for attempt in range(3):
        response = http.post(
            SEND_URL,
            params={"access_token": IG_ACCESS_TOKEN},
            json=payload,
            timeout=(5, 20),
        )

        if response.status_code < 400:
            log.info("Reply sent to Instagram-scoped user %s", recipient_igsid)
            return

        transient = response.status_code == 429 or response.status_code >= 500
        log.error(
            "Instagram send failed (%s): %s",
            response.status_code,
            response.text[:1000],
        )
        if not transient or attempt == 2:
            response.raise_for_status()
        time.sleep(2**attempt)


def process_message(task: dict[str, str]) -> None:
    sender_id = task["sender_id"]
    user_text = task["user_text"]

    # Preserve message order for one sender even when multiple workers run.
    with user_locks[sender_id]:
        reply = generate_reply(sender_id, user_text)
        send_message(sender_id, reply)


def worker_loop(worker_number: int) -> None:
    log.info("Message worker %s started", worker_number)
    while True:
        task = message_queue.get()
        try:
            process_message(task)
        except Exception:
            log.exception("Failed to process queued Instagram message")
        finally:
            message_queue.task_done()


def start_workers() -> None:
    global workers_started
    with workers_started_lock:
        if workers_started:
            return
        for worker_number in range(1, WORKER_COUNT + 1):
            thread = threading.Thread(
                target=worker_loop,
                args=(worker_number,),
                name=f"ig-worker-{worker_number}",
                daemon=True,
            )
            thread.start()
        workers_started = True


# Start background workers when Gunicorn imports the application.
start_workers()

for missing_name in missing_required_config():
    log.warning("Missing environment variable: %s", missing_name)
if not META_APP_SECRET:
    log.warning("META_APP_SECRET is not set; webhook signature checks are disabled")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
@app.get("/health")
def health() -> tuple[Any, int]:
    missing = missing_required_config()
    return (
        jsonify(
            status="ok" if not missing else "configuration_incomplete",
            missing=missing,
            gemini="configured" if GEMINI_API_KEY else "fallback_only",
            model=GEMINI_MODEL,
            queue_depth=message_queue.qsize(),
        ),
        200,
    )


@app.get("/webhook", strict_slashes=False)
def verify_webhook() -> Response:
    """Meta calls this once to verify the callback URL."""
    mode = request.args.get("hub.mode", "")
    supplied_token = request.args.get("hub.verify_token", "")
    challenge = request.args.get("hub.challenge", "")
    token_matches = bool(VERIFY_TOKEN) and hmac.compare_digest(
        supplied_token, VERIFY_TOKEN
    )

    # Never log the token itself. These fields are enough to diagnose a 403,
    # wrong path, missing query parameters, or an old Render deployment.
    log.info(
        "Webhook verification request: mode=%r token_matches=%s challenge_present=%s",
        mode,
        token_matches,
        bool(challenge),
    )

    if mode == "subscribe" and token_matches and challenge:
        return Response(challenge, status=200, mimetype="text/plain")

    return Response("verification failed", status=403, mimetype="text/plain")


@app.post("/webhook", strict_slashes=False)
def handle_webhook() -> tuple[Any, int]:
    """Validate, deduplicate and queue events, then acknowledge Meta immediately."""
    raw_body = request.get_data(cache=True)
    if not validate_signature(raw_body, request.headers.get("X-Hub-Signature-256")):
        log.warning("Rejected webhook with an invalid Meta signature")
        return jsonify(status="invalid_signature"), 401

    data = request.get_json(silent=True) or {}
    if data.get("object") != "instagram":
        return jsonify(status="ignored"), 200

    queued = 0
    duplicates = 0

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender_id = str(event.get("sender", {}).get("id", ""))
            message = event.get("message") or {}

            # Ignore our own echoes and message types this text bot cannot handle.
            if not sender_id or message.get("is_echo") or not isinstance(message.get("text"), str):
                continue

            user_text = message["text"].strip()
            if not user_text:
                continue

            key = event_key(sender_id, message, event)
            if not reserve_event(key):
                duplicates += 1
                continue

            try:
                message_queue.put_nowait(
                    {"sender_id": sender_id, "user_text": user_text, "event_key": key}
                )
                queued += 1
            except queue.Full:
                release_event(key)
                log.error("Message queue is full; asking Meta to retry")
                return jsonify(status="busy"), 503

    # The slow Gemini and Instagram API calls happen in worker threads.
    return jsonify(status="accepted", queued=queued, duplicates=duplicates), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
