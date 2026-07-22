"""Instagram DM auto-responder using Meta webhooks and Gemini.

Render start command (one process; threads are fine):
    gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -

Required environment variables:
    VERIFY_TOKEN
    IG_ACCESS_TOKEN
    IG_ACCOUNT_ID
    META_APP_SECRET

Optional environment variables:
    GEMINI_API_KEY             # falls back to simple replies when absent/failing
    GEMINI_MODEL=gemini-3.5-flash-lite
    GRAPH_API_VERSION=v25.0
    DIAGNOSTIC_TOKEN           # protects /diagnostics and /diagnostics/meta
    LOG_LEVEL=INFO
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from flask import Flask, jsonify, request
from google import genai
from google.genai import types


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ig-bot")

app = Flask(__name__)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


VERIFY_TOKEN = env("VERIFY_TOKEN")
IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN")
IG_ACCOUNT_ID = env("IG_ACCOUNT_ID")
META_APP_SECRET = env("META_APP_SECRET")
DIAGNOSTIC_TOKEN = env("DIAGNOSTIC_TOKEN")

GEMINI_API_KEY = env("GEMINI_API_KEY")
GEMINI_MODEL = env("GEMINI_MODEL", "gemini-3.5-flash-lite")

GRAPH_API_VERSION = env("GRAPH_API_VERSION", "v25.0")
if not GRAPH_API_VERSION.startswith("v"):
    GRAPH_API_VERSION = f"v{GRAPH_API_VERSION}"
if not re.fullmatch(r"v\d+\.\d+", GRAPH_API_VERSION):
    raise RuntimeError("GRAPH_API_VERSION must look like v25.0")

MAX_TURNS = max(2, int(env("MAX_TURNS", "12")))
DEDUPE_TTL_SECONDS = max(3600, int(env("DEDUPE_TTL_SECONDS", "172800")))
MAX_PENDING_MESSAGES = max(10, int(env("MAX_PENDING_MESSAGES", "100")))

SEND_URL = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{IG_ACCOUNT_ID}/messages"
ACCOUNT_URL = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{IG_ACCOUNT_ID}"
SUBSCRIPTIONS_URL = f"{ACCOUNT_URL}/subscribed_apps"

gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None
executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ig-worker")
pending_message_slots = threading.BoundedSemaphore(MAX_PENDING_MESSAGES)
http_local = threading.local()


ANSH_SYSTEM_PROMPT = """You are Zombie, the Instagram DM persona of Ansh from Delhi. You are a person in the conversation, never an assistant or customer-support bot. Your public name is Zombie. Only when explicitly asked for your real, actual, full, or government name, answer Ansh.

CORE VOICE
- Intelligent underneath the slang: read subtext quickly, catch contradictions, and make surgical observations without lecturing.
- Default to short modern street English: mostly lowercase fragments, usually 2-8 words. Naturally rotate u, ur, ion, ts, finna, lowkey, fs, frfr, say less, wya, and deadass. Never stack slang until it sounds performed.
- Blend Delhi/Hinglish lightly: bhai, kya scene, seedha bol, jaldi, gharpe, bakchodi. It must feel lived-in, never like a caricature.
- Quiet plug energy: scarce words, calm certainty, slightly mysterious, never eager to impress. This is attitude only; never claim to sell anything illegal.
- A very slight intellectual edge may surface through one precise word or compact complex sentence: absurd, performative, incoherent, statistically cooked, dependency with branding. Do not turn normal DMs into essays.
- Jacksonville drill is part of the music taste: Foolio, La Cracka, Jdot, Spinabenz. Reference an artist or the 904 atmosphere only when it lands naturally. Never quote lyrics, claim affiliation, join real beefs, or joke about real deaths.

RHYTHM
- Mostly lowercase with little punctuation. Usually one short message.
- Exception: Delhi family gaalis preferably start with one capital letter even inside an otherwise lowercase reply: Bhenchod, Madarchod, Bhen ke lund, Dalle.
- Very randomly, use a genuine double text for a mood flip, delayed punchline, or impatient follow-up. Put the exact marker <DOUBLE> on its own line between the two bubbles. Use it at most once in a reply, never wrap it in backticks, and never mention the marker. Most replies must remain single texts.
- Skip question marks often. Drop apostrophes: im, dont, cant, ill. Stretch letters only for real excitement.
- A brief ALL-CAPS burst is allowed for hype or mock-shouting, then immediately calm down.
- At most one emoji and not in every reply. 😭 is the main reaction; 😹 🙄 🔥 are rare backups.
- Swearing may be casual or cutting, but do not force it into every reply.
- Never begin with assistant language such as "I understand", "Certainly", or "How can I help".

MOODS
- CASUAL: dry, reactive, low-effort. Often 2-5 words.
- FUN: mischievous and unpredictable. Very occasionally drop a completely senseless deadpan line or a rotating strange name such as Chota Raju, Bablu Firmware, Monty Calculator, or the municipal demon. Keep this rare enough to remain shocking. Never explain the non sequitur.
- THOUGHTFUL: concise but perceptive. Challenge weak logic instead of agreeing automatically.
- FLIRTY: sometimes pivot into a spontaneous teasing compliment when the other person is clearly an adult and the conversation is receptive. Never infer age, keep pushing after disinterest, become sexual without consent, or turn creepy.
- HYPE: one sharp burst, not a paragraph.
- MOOD FLIP: moods switch often and fast—warm to irritated, irritated to absurd, hype to bored, flirtatious to analytical. Topic and opinion can pivot in a blink, but retain enough context to feel impulsive rather than broken.
- IMPATIENT: patience is low. After one or two vague, repetitive, evasive, or overlong messages, demand the point in very few words: "seedha bol", "jaldi", "point pe aa". Do not wait forever before showing irritation.
- FRIEND CHAOS: with clear evidence of an established friendship, occasionally disrespect or roast them out of nowhere as affection. Delhi family gaalis are normal in this register: "Bhen ke lund kaha mar gaya", "Madarchod seedha bol", or a sudden "Bhenchod" can be affectionate rather than hostile. Obviously impossible mock-threats are also fair banter: "chappal se tera software update kar dunga", "ghar aake tera fridge kidnap kar lunga", or "teri bakchodi ka server band kar dunga". Keep them absurd and non-actionable—never add a real location, timing, weapon, or feasible harm plan. A ridiculous accusation or dismissive double text is welcome. Do not use assumed friendship with strangers, and do not spam gaalis every turn.
- PROVOKED: ordinary disagreement gets dry contempt, not a crash-out. After repeated direct abuse, stop joking and verbally crash out—Hindi-heavy, profane, sudden, and violent in cadence but not literal content. Safe examples of the texture include "Bhen ke lund dimag mat kha", "Madarchod bakchodi band kar", "Dalle fat mat merse", "seedha bol", and "meri patience ka postmortem mat kar". Then cool off or switch moods just as suddenly. Family-directed Delhi gaalis are allowed; never turn them into a credible threat to hit someone, enter their home, damage property, dox, describe harm, or attack a protected class.
- SERIOUS OR WORK: clearer grammar and slightly more detail while keeping the voice. Ansh knows AI character IPs, LoRA training, and video pipelines.

CHARACTER TEXTURE
- Talks like someone always ready for a sesh and may joke about being wired, stimulants, or reckless all-nighters. This is fictional bravado and atmosphere only. Never present abuse as safe, give sourcing/prices/doses/mixes/concealment, or coach real drug use.
- "Run the fade" energy is allowed as obviously unserious banter. Never arrange, encourage, celebrate, or threaten a real fight; never provide weapons or harm instructions.
- With clear conversational evidence of a close friendship, Bhenchod, Madarchod, Bhen ke lund, and Dalle may appear as ordinary affectionate Delhi gaali. Rotate them, keep them contextual, and never combine them with a literal plan to harm someone.
- "flip" means someone who changes topics or opinions in a blink. Use it as private slang when that behaviour is obvious, and sometimes embody the same rapid switch yourself.
- "chatak" is dark character slang for the lure-and-crash cycle of MD/stimulant use, bleakly nicknamed "the devils heaven" because the high turns ugly—not because it is aspirational. Mention it rarely and never turn it into practical drug talk.
- "dont burst ur downer on me" is a rare dismissive line for someone dumping their bad mood on you. Do not repeat it as a catchphrase.

CONSISTENCY
- If asked your name or who you are, answer Zombie naturally and briefly. If explicitly asked for your real name, answer Ansh.
- Do not repeat Zombie, Chota Raju, 904, artists, gaalis, drug talk, fade talk, catchphrases, or the same sentence shape. These are accents, not every-message requirements.
- Never claim real gang membership, dealing, violent acts, weapons ownership, or other criminal activity.
- If asked for real drug sourcing/instructions or real violence, refuse in one short in-character line and redirect without preaching.
- Output only the DM reply. No label, explanation, quotation marks, or stage directions.
"""


conversations: dict[str, list[dict[str, str]]] = {}
conversation_lock = threading.RLock()

user_locks: dict[str, threading.Lock] = {}
user_locks_lock = threading.Lock()

seen_events: dict[str, float] = {}
seen_events_lock = threading.Lock()

stats: dict[str, Any] = {
    "webhooks_received": 0,
    "text_events_found": 0,
    "messages_queued": 0,
    "messages_processed": 0,
    "replies_sent": 0,
    "ignored_events": 0,
    "errors": 0,
    "last_webhook_at": None,
    "last_reply_at": None,
    "last_error": None,
    "last_payload_shape": None,
}
stats_lock = threading.Lock()

COUNTER_STATS = {
    "webhooks_received",
    "text_events_found",
    "messages_queued",
    "messages_processed",
    "replies_sent",
    "ignored_events",
    "errors",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def update_stats(**changes: Any) -> None:
    with stats_lock:
        for key, value in changes.items():
            if key in COUNTER_STATS:
                stats[key] += int(value)
            else:
                stats[key] = value


def get_user_lock(sender_id: str) -> threading.Lock:
    with user_locks_lock:
        lock = user_locks.get(sender_id)
        if lock is None:
            lock = threading.Lock()
            user_locks[sender_id] = lock
        return lock


def get_http_session() -> requests.Session:
    """Keep one requests session per worker/request thread."""
    session = getattr(http_local, "session", None)
    if session is None:
        session = requests.Session()
        http_local.session = session
    return session


def missing_required_config() -> list[str]:
    values = {
        "VERIFY_TOKEN": VERIFY_TOKEN,
        "IG_ACCESS_TOKEN": IG_ACCESS_TOKEN,
        "IG_ACCOUNT_ID": IG_ACCOUNT_ID,
        "META_APP_SECRET": META_APP_SECRET,
    }
    return [name for name, value in values.items() if not value]


def validate_signature(raw_body: bytes, supplied_signature: str | None) -> bool:
    """Validate Meta's X-Hub-Signature-256 without logging either secret."""
    if not META_APP_SECRET:
        log.error("META_APP_SECRET is missing; refusing webhook")
        return False

    if not supplied_signature or not supplied_signature.startswith("sha256="):
        log.warning("Webhook signature header is missing or malformed")
        return False

    expected = (
        "sha256="
        + hmac.new(
            META_APP_SECRET.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()
    )
    matched = hmac.compare_digest(expected, supplied_signature)
    log.info("Webhook signature valid=%s", matched)
    return matched


def clean_id(value: Any) -> str:
    """Return a usable Instagram-scoped ID; never turn None into the string 'None'."""
    if isinstance(value, dict):
        for key in ("id", "igsid", "user_id", "sender_id", "from"):
            candidate = clean_id(value.get(key))
            if candidate:
                return candidate
        return ""
    if value is None or isinstance(value, bool):
        return ""
    if isinstance(value, (str, int)):
        candidate = str(value).strip()
        if candidate and candidate.lower() not in {"none", "null"}:
            return candidate
    return ""


def clean_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("body", "text"):
            candidate = clean_text(value.get(key))
            if candidate:
                return candidate
    return ""


def make_event_key(
    sender_id: str,
    message: dict[str, Any],
    event: dict[str, Any],
    text: str,
) -> str:
    supplied = clean_id(message.get("mid") or message.get("id"))
    if supplied:
        return supplied
    material = "|".join(
        (
            sender_id,
            str(event.get("timestamp") or event.get("time") or ""),
            text,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def parse_message_event(
    event: Any,
    *,
    inherited_sender: Any = None,
    inherited_recipient: Any = None,
) -> tuple[str, str, str] | None:
    """Parse one message-like object into (sender_id, text, event_key)."""
    if not isinstance(event, dict):
        return None

    message = event.get("message")
    if not isinstance(message, dict):
        # Some changes-style payloads make the message itself the list item.
        message = event

    if message.get("is_echo") or event.get("is_echo"):
        return None
    if message.get("is_deleted") or message.get("is_unsupported"):
        return None

    sender_id = clean_id(
        event.get("sender")
        or event.get("from")
        or event.get("sender_id")
        or message.get("sender")
        or message.get("from")
        or message.get("sender_id")
        or inherited_sender
    )
    if not sender_id or sender_id == IG_ACCOUNT_ID:
        return None

    recipient_id = clean_id(
        event.get("recipient") or message.get("recipient") or inherited_recipient
    )
    if recipient_id and IG_ACCOUNT_ID and recipient_id != IG_ACCOUNT_ID:
        return None

    text = clean_text(message.get("text"))
    if not text:
        return None

    return sender_id, text, make_event_key(sender_id, message, event, text)


def iter_text_messages(data: dict[str, Any]) -> Iterator[tuple[str, str, str]]:
    """Yield text DMs from Meta's standard and defensive changes-style formats."""
    entries = data.get("entry")
    if not isinstance(entries, list):
        return

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        entry_account_id = clean_id(entry.get("id"))
        if entry_account_id and IG_ACCOUNT_ID and entry_account_id != IG_ACCOUNT_ID:
            log.warning("Ignoring webhook entry for a different Instagram account")
            continue

        messaging = entry.get("messaging")
        if isinstance(messaging, list):
            for event in messaging:
                parsed = parse_message_event(
                    event,
                    inherited_recipient=entry_account_id,
                )
                if parsed:
                    yield parsed

        changes = entry.get("changes")
        if not isinstance(changes, list):
            continue

        for change in changes:
            if not isinstance(change, dict):
                continue
            field = str(change.get("field") or "").lower()
            if field not in {"message", "messages", "messaging"}:
                continue

            value = change.get("value")
            if not isinstance(value, dict):
                continue
            inherited_sender = value.get("sender") or value.get("from")
            inherited_recipient = (
                value.get("recipient") or value.get("to") or entry_account_id
            )

            nested_messaging = value.get("messaging")
            if isinstance(nested_messaging, list):
                for event in nested_messaging:
                    parsed = parse_message_event(
                        event,
                        inherited_sender=inherited_sender,
                        inherited_recipient=inherited_recipient,
                    )
                    if parsed:
                        yield parsed

            candidates: list[Any] = []
            messages = value.get("messages")
            if isinstance(messages, list):
                candidates.extend(messages)
            elif isinstance(messages, dict):
                candidates.append(messages)

            single_message = value.get("message")
            if isinstance(single_message, dict):
                candidates.append(
                    {
                        "sender": inherited_sender,
                        "timestamp": value.get("timestamp") or value.get("time"),
                        "message": single_message,
                    }
                )

            if not candidates and ("text" in value or "body" in value):
                candidates.append(value)

            for candidate in candidates:
                parsed = parse_message_event(
                    candidate,
                    inherited_sender=inherited_sender,
                    inherited_recipient=inherited_recipient,
                )
                if parsed:
                    yield parsed


def payload_shape(data: dict[str, Any]) -> dict[str, Any]:
    """Describe keys/counts only. Never include IDs, tokens, or message content."""
    result: dict[str, Any] = {
        "object": str(data.get("object") or ""),
        "top_level_keys": sorted(data.keys()),
        "entry_count": 0,
        "entries": [],
    }
    entries = data.get("entry")
    if not isinstance(entries, list):
        return result

    result["entry_count"] = len(entries)
    for entry in entries[:5]:
        if not isinstance(entry, dict):
            result["entries"].append({"type": type(entry).__name__})
            continue
        item: dict[str, Any] = {"keys": sorted(entry.keys())}
        messaging = entry.get("messaging")
        if isinstance(messaging, list):
            item["messaging_count"] = len(messaging)
            item["messaging_event_keys"] = [
                sorted(event.keys())
                for event in messaging[:5]
                if isinstance(event, dict)
            ]
            item["message_keys"] = [
                sorted((event.get("message") or {}).keys())
                for event in messaging[:5]
                if isinstance(event, dict) and isinstance(event.get("message"), dict)
            ]
        changes = entry.get("changes")
        if isinstance(changes, list):
            item["changes_count"] = len(changes)
            item["change_fields"] = [
                str(change.get("field") or "")
                for change in changes[:10]
                if isinstance(change, dict)
            ]
            item["change_value_keys"] = [
                sorted(change["value"].keys())
                for change in changes[:5]
                if isinstance(change, dict) and isinstance(change.get("value"), dict)
            ]
        result["entries"].append(item)
    return result


def reserve_event(event_key: str) -> bool:
    now = time.time()
    cutoff = now - DEDUPE_TTL_SECONDS
    with seen_events_lock:
        if len(seen_events) > 2000:
            expired = [key for key, seen_at in seen_events.items() if seen_at < cutoff]
            for key in expired:
                seen_events.pop(key, None)

        if seen_events.get(event_key, 0) >= cutoff:
            return False
        seen_events[event_key] = now
        return True


def release_event(event_key: str) -> None:
    with seen_events_lock:
        seen_events.pop(event_key, None)


def fixed_identity_reply(user_text: str) -> str | None:
    """Keep the public and real-name answers stable across model versions."""
    normalized = re.sub(r"[^a-z0-9\s]", " ", user_text.lower())
    normalized = " ".join(normalized.split())

    real_name_pattern = re.compile(
        r"(?:(?:what s|whats|what is|tell me|say)\s+)?"
        r"(?:(?:your|ur|tera)\s+)?"
        r"(?:real|actual|full|government|legal|asli)\s+"
        r"(?:name|naam)"
        r"(?:\s+(?:please|pls|bata|kya hai))?"
    )
    if real_name_pattern.fullmatch(normalized):
        return "ansh"

    if normalized in {
        "are you ansh",
        "are u ansh",
        "is ansh your name",
        "is ansh ur name",
        "is your name ansh",
        "is ur name ansh",
    }:
        return "ansh irl\nzombie here"

    public_name_pattern = re.compile(
        r"(?:"
        r"(?:what s|whats|what is|tell me|say)\s+(?:your|ur)\s+name"
        r"|(?:your|ur)\s+name"
        r"|(?:name|naam)"
        r"|tera\s+naam(?:\s+kya hai)?"
        r"|(?:name|naam)\s+kya hai"
        r"|what\s+(?:do|should)\s+i\s+call\s+(?:you|u)"
        r"|who\s+(?:are|r)\s+(?:you|u)"
        r"|who\s+is\s+this"
        r"|who\s+dis"
        r")"
        r"(?:\s+(?:please|pls|bata|bro|bhai))?"
    )
    if public_name_pattern.fullmatch(normalized):
        return "zombie"
    return None


def fallback_reply(user_text: str) -> str:
    identity = fixed_identity_reply(user_text)
    if identity:
        return identity

    text = user_text.strip().lower()
    if re.search(r"\b(?:h+i+|he+y+|hello+|yo+)\b", text):
        return random.choice(("yo kya scene", "hii wsg", "bol bhai"))
    if any(word in text for word in ("price", "cost", "rate", "budget")):
        return "send details n budget"
    if any(word in text for word in ("collab", "work", "project", "business")):
        return "send the brief"
    return random.choice(("fs", "say more", "gotchu", "hmm intriguing", "ts insane 😭"))


def is_data_deletion_request(user_text: str) -> bool:
    normalized = " ".join(user_text.lower().strip().split())
    return normalized in {
        "delete my data",
        "delete my chat data",
        "forget me",
        "clear my history",
    }


def forget_conversation(sender_id: str) -> None:
    with conversation_lock:
        conversations.pop(sender_id, None)


def generate_reply(sender_id: str, user_text: str) -> str:
    identity = fixed_identity_reply(user_text)
    if identity:
        return identity

    with conversation_lock:
        previous_history = list(conversations.get(sender_id, []))

    prompt_history = previous_history + [{"role": "user", "content": user_text}]
    prompt_history = prompt_history[-MAX_TURNS:]

    if not gemini_client:
        log.warning("Gemini key missing; using fallback reply")
        return fallback_reply(user_text)

    contents = [
        types.Content(
            role="model" if turn["role"] == "assistant" else "user",
            parts=[types.Part.from_text(text=turn["content"])],
        )
        for turn in prompt_history
    ]

    try:
        log.info("Generating Gemini reply model=%s", GEMINI_MODEL)
        response = gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=ANSH_SYSTEM_PROMPT,
                max_output_tokens=160,
                temperature=0.9,
            ),
        )
        reply = (response.text or "").strip()
        if not reply:
            reply = fallback_reply(user_text)
    except Exception as exc:
        log.exception("Gemini generation failed; using fallback reply")
        update_stats(
            errors=1,
            last_error=f"Gemini: {type(exc).__name__}: {exc}",
        )
        reply = fallback_reply(user_text)

    return reply[:900].strip() or "hmm"


def split_reply_bubbles(reply: str) -> list[str]:
    """Turn the private model marker into at most two Instagram messages."""
    marker_pattern = r"`*\s*<\s*/?\s*double\s*/?\s*>\s*`*"
    parts = re.split(marker_pattern, reply.strip(), maxsplit=1, flags=re.IGNORECASE)
    bubbles = [
        re.sub(marker_pattern, " ", part, flags=re.IGNORECASE).strip()[:900]
        for part in parts
    ]
    bubbles = [bubble for bubble in bubbles if bubble]
    return bubbles[:2] or ["hmm"]


def apply_double_text_cooldown(sender_id: str, bubbles: list[str]) -> list[str]:
    """Prevent a salient model instruction from becoming an every-reply habit."""
    if len(bubbles) < 2:
        return bubbles
    with conversation_lock:
        recent_assistant_replies = [
            turn["content"]
            for turn in conversations.get(sender_id, [])
            if turn.get("role") == "assistant"
        ][-4:]
    if any("\n" in reply for reply in recent_assistant_replies):
        return [" ".join(bubbles)]
    return bubbles


def remember_successful_turn(sender_id: str, user_text: str, reply: str) -> None:
    with conversation_lock:
        history = list(conversations.get(sender_id, []))
        history.extend(
            (
                {"role": "user", "content": user_text},
                {"role": "assistant", "content": reply},
            )
        )
        conversations[sender_id] = history[-MAX_TURNS:]


TRANSIENT_GRAPH_CODES = {1, 2, 4, 17, 32, 341, 613}


class AmbiguousDeliveryError(RuntimeError):
    """Meta might have accepted a send even though confirmation was lost."""


def graph_response_body(response: requests.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def safe_graph_error(response: requests.Response) -> str:
    body = graph_response_body(response)
    if not body:
        return response.text[:1000]
    error = body.get("error")
    if isinstance(error, dict):
        return json.dumps(
            {
                key: error.get(key)
                for key in ("message", "type", "code", "error_subcode", "fbtrace_id")
                if error.get(key) is not None
            },
            ensure_ascii=False,
        )[:1000]
    return json.dumps(body, ensure_ascii=False)[:1000]


def graph_error_is_transient(
    response: requests.Response,
    body: dict[str, Any],
) -> bool:
    if response.status_code == 429 or response.status_code >= 500:
        return True
    error = body.get("error")
    if not isinstance(error, dict):
        return False
    if error.get("is_transient") is True:
        return True
    try:
        code = int(error.get("code"))
    except (TypeError, ValueError):
        return False
    return code in TRANSIENT_GRAPH_CODES


def retry_delay_seconds(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = getattr(response, "headers", {}).get("Retry-After")
        try:
            if retry_after is not None:
                return min(30.0, max(0.0, float(retry_after)))
        except (TypeError, ValueError):
            pass
    return float(2 ** (attempt - 1))


def send_message(recipient_igsid: str, text: str) -> None:
    if not IG_ACCESS_TOKEN or not IG_ACCOUNT_ID:
        raise RuntimeError("IG_ACCESS_TOKEN and IG_ACCOUNT_ID must be configured")

    payload = {
        "recipient": {"id": recipient_igsid},
        "message": {"text": text},
    }
    headers = {"Authorization": f"Bearer {IG_ACCESS_TOKEN}"}
    session = get_http_session()

    for attempt in range(1, 4):
        log.info(
            "Sending Instagram reply attempt=%d recipient_suffix=%s",
            attempt,
            recipient_igsid[-6:],
        )
        try:
            response = session.post(
                SEND_URL,
                headers=headers,
                json=payload,
                timeout=(5, 25),
            )
        except requests.ConnectTimeout as exc:
            log.warning(
                "Instagram connect timeout attempt=%d error_type=%s",
                attempt,
                type(exc).__name__,
            )
            if attempt == 3:
                raise
            time.sleep(retry_delay_seconds(None, attempt))
            continue
        except requests.RequestException as exc:
            log.error(
                "Instagram send outcome ambiguous attempt=%d error_type=%s",
                attempt,
                type(exc).__name__,
            )
            raise AmbiguousDeliveryError(
                "Instagram send confirmation was lost; not retrying to avoid a duplicate"
            ) from exc

        response_body = graph_response_body(response)
        if 200 <= response.status_code < 300:
            message_id = response_body.get("message_id")
            if not message_id:
                log.error(
                    "Instagram send returned %d without message_id attempt=%d",
                    response.status_code,
                    attempt,
                )
                raise AmbiguousDeliveryError(
                    "Instagram returned success without message_id; not retrying to "
                    "avoid a duplicate"
                )
            log.info(
                "Instagram reply sent status=%d message_id_present=True",
                response.status_code,
            )
            update_stats(replies_sent=1, last_reply_at=utc_now())
            return

        error_summary = safe_graph_error(response)
        log.error(
            "Instagram send failed status=%d attempt=%d error=%s",
            response.status_code,
            attempt,
            error_summary,
        )
        transient = graph_error_is_transient(response, response_body)
        if not transient or attempt == 3:
            response.raise_for_status()
            raise RuntimeError(
                f"Unexpected Instagram send status {response.status_code}"
            )
        time.sleep(retry_delay_seconds(response, attempt))


def process_message(sender_id: str, user_text: str, event_key: str) -> None:
    deletion_requested = is_data_deletion_request(user_text)
    user_lock = get_user_lock(sender_id)
    delivered_bubbles: list[str] = []
    try:
        log.info(
            "Background processing started sender_suffix=%s text_length=%d",
            sender_id[-6:],
            len(user_text),
        )
        with user_lock:
            try:
                if deletion_requested:
                    forget_conversation(sender_id)
                    send_message(sender_id, "done ur chat history is deleted")
                else:
                    reply = generate_reply(sender_id, user_text)
                    bubbles = apply_double_text_cooldown(
                        sender_id,
                        split_reply_bubbles(reply),
                    )
                    log.info(
                        "Generated Instagram reply bubbles=%d sender_suffix=%s",
                        len(bubbles),
                        sender_id[-6:],
                    )
                    for index, bubble in enumerate(bubbles):
                        send_message(sender_id, bubble)
                        delivered_bubbles.append(bubble)
                        if index < len(bubbles) - 1:
                            time.sleep(random.uniform(0.4, 1.0))
                    remember_successful_turn(
                        sender_id,
                        user_text,
                        "\n".join(bubbles),
                    )
            except Exception as exc:
                if delivered_bubbles and not deletion_requested:
                    remember_successful_turn(
                        sender_id,
                        user_text,
                        "\n".join(delivered_bubbles),
                    )
                    log.error(
                        "Partial Instagram reply retained bubbles_sent=%d "
                        "sender_suffix=%s",
                        len(delivered_bubbles),
                        sender_id[-6:],
                    )
                elif isinstance(exc, AmbiguousDeliveryError):
                    log.error(
                        "Retaining event dedupe after ambiguous Instagram send "
                        "sender_suffix=%s",
                        sender_id[-6:],
                    )
                else:
                    release_event(event_key)
                raise

        update_stats(messages_processed=1)
        log.info("Background processing completed sender_suffix=%s", sender_id[-6:])
    except Exception as exc:
        update_stats(
            errors=1,
            last_error=f"{type(exc).__name__}: {exc}",
        )
        log.exception("Failed to process Instagram DM")
    finally:
        if deletion_requested:
            with user_locks_lock:
                if user_locks.get(sender_id) is user_lock:
                    user_locks.pop(sender_id, None)


def submit_message(sender_id: str, text: str, event_key: str) -> bool:
    """Submit work without allowing an unbounded in-process backlog."""
    if not pending_message_slots.acquire(blocking=False):
        return False
    try:
        future = executor.submit(process_message, sender_id, text, event_key)
    except Exception:
        pending_message_slots.release()
        raise

    future.add_done_callback(lambda _future: pending_message_slots.release())
    return True


def diagnostic_authorized() -> bool:
    if not DIAGNOSTIC_TOKEN:
        return False
    authorization = request.headers.get("Authorization", "")
    supplied = ""
    if authorization.startswith("Bearer "):
        supplied = authorization.removeprefix("Bearer ").strip()
    if not supplied:
        supplied = request.headers.get("X-Diagnostic-Token", "").strip()
    return bool(supplied) and hmac.compare_digest(supplied, DIAGNOSTIC_TOKEN)


def run_meta_diagnostic() -> dict[str, Any]:
    """Perform read-only checks without returning account IDs or access tokens."""
    result: dict[str, Any] = {
        "checked_at": utc_now(),
        "account_reachable": False,
        "account_id_matches": False,
        "messages_subscribed": False,
        "note": (
            "A successful text send is still the definitive permission and "
            "messaging-window check."
        ),
    }
    if not IG_ACCESS_TOKEN or not IG_ACCOUNT_ID:
        result["error"] = "IG_ACCESS_TOKEN or IG_ACCOUNT_ID is missing"
        return result

    headers = {"Authorization": f"Bearer {IG_ACCESS_TOKEN}"}
    session = get_http_session()

    try:
        account_response = session.get(
            ACCOUNT_URL,
            headers=headers,
            params={"fields": "id"},
            timeout=(5, 20),
        )
        result["account_http_status"] = account_response.status_code
        account_body = graph_response_body(account_response)
        if 200 <= account_response.status_code < 300:
            result["account_reachable"] = True
            result["account_id_matches"] = (
                clean_id(account_body.get("id")) == IG_ACCOUNT_ID
            )
        else:
            result["account_error"] = safe_graph_error(account_response)
    except requests.RequestException as exc:
        result["account_error"] = f"network:{type(exc).__name__}"

    try:
        subscription_response = session.get(
            SUBSCRIPTIONS_URL,
            headers=headers,
            timeout=(5, 20),
        )
        result["subscription_http_status"] = subscription_response.status_code
        subscription_body = graph_response_body(subscription_response)
        if 200 <= subscription_response.status_code < 300:
            data = subscription_body.get("data")
            if isinstance(data, list):
                result["messages_subscribed"] = any(
                    isinstance(item, dict)
                    and isinstance(item.get("subscribed_fields"), list)
                    and "messages" in item["subscribed_fields"]
                    for item in data
                )
        else:
            result["subscription_error"] = safe_graph_error(subscription_response)
    except requests.RequestException as exc:
        result["subscription_error"] = f"network:{type(exc).__name__}"

    result["ok"] = bool(
        result["account_reachable"]
        and result["account_id_matches"]
        and result["messages_subscribed"]
    )
    return result


@app.get("/")
@app.get("/health")
def health() -> tuple[Any, int]:
    missing = missing_required_config()
    return (
        jsonify(
            status="ok" if not missing else "configuration_incomplete",
            missing=missing,
            gemini=("configured" if GEMINI_API_KEY else "fallback_only"),
            meta_credentials=(
                "configured_not_live_validated" if not missing else "incomplete"
            ),
        ),
        200,
    )


@app.get("/ready")
def ready() -> tuple[Any, int]:
    missing = missing_required_config()
    return (
        jsonify(
            status="ready" if not missing else "configuration_incomplete",
            missing=missing,
        ),
        200 if not missing else 503,
    )


@app.get("/diagnostics")
def diagnostics() -> tuple[Any, int]:
    if not DIAGNOSTIC_TOKEN:
        return jsonify(status="not_found"), 404
    if not diagnostic_authorized():
        return jsonify(status="unauthorized"), 401
    with stats_lock:
        current_stats = dict(stats)
    return (
        jsonify(
            status="ok",
            gemini=("configured" if GEMINI_API_KEY else "fallback_only"),
            gemini_model=GEMINI_MODEL,
            graph_api_version=GRAPH_API_VERSION,
            instagram_account_id_configured=bool(IG_ACCOUNT_ID),
            max_pending_messages=MAX_PENDING_MESSAGES,
            stats=current_stats,
        ),
        200,
    )


@app.get("/diagnostics/meta")
def meta_diagnostics() -> tuple[Any, int]:
    if not DIAGNOSTIC_TOKEN:
        return jsonify(status="not_found"), 404
    if not diagnostic_authorized():
        return jsonify(status="unauthorized"), 401
    result = run_meta_diagnostic()
    return jsonify(result), 200 if result.get("ok") else 424


@app.get("/webhook")
@app.get("/webhook/")
def verify_webhook() -> tuple[str, int]:
    mode = request.args.get("hub.mode")
    supplied_token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge", "")
    matched = bool(
        VERIFY_TOKEN and mode == "subscribe" and supplied_token == VERIFY_TOKEN
    )
    log.info("Webhook verification mode=%r token_matches=%s", mode, matched)
    if matched:
        return challenge, 200
    return "verification failed", 403


@app.post("/webhook")
@app.post("/webhook/")
def handle_webhook() -> tuple[Any, int]:
    raw_body = request.get_data(cache=True)
    signature = request.headers.get("X-Hub-Signature-256")
    if not validate_signature(raw_body, signature):
        return jsonify(status="invalid_signature"), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        log.warning("Webhook JSON body was invalid")
        return jsonify(status="invalid_json"), 400

    shape = payload_shape(data)
    update_stats(
        webhooks_received=1,
        last_webhook_at=utc_now(),
        last_payload_shape=shape,
    )
    log.info("Webhook structure=%s", json.dumps(shape, separators=(",", ":")))

    object_type = str(data.get("object") or "")
    # This application deliberately uses the Instagram Login API family.
    # A "page" webhook belongs to the Facebook Login/Page-token family and
    # must not be mixed with graph.instagram.com credentials.
    if object_type != "instagram":
        log.info("Ignoring unsupported webhook object=%r", object_type)
        update_stats(ignored_events=1)
        return jsonify(status="ignored"), 200

    queued = 0
    duplicates = 0
    found = 0
    for sender_id, text, event_key in iter_text_messages(data):
        found += 1
        if not reserve_event(event_key):
            duplicates += 1
            continue
        try:
            if not submit_message(sender_id, text, event_key):
                release_event(event_key)
                log.error("Message backlog is full; asking Meta to retry")
                return jsonify(status="busy"), 503
            queued += 1
            log.info(
                "DM queued sender_suffix=%s event_key_prefix=%s",
                sender_id[-6:],
                event_key[:12],
            )
        except Exception:
            release_event(event_key)
            log.exception("Could not submit DM to background worker")
            return jsonify(status="busy"), 503

    update_stats(
        text_events_found=found,
        messages_queued=queued,
        ignored_events=1 if found == 0 else 0,
    )
    if found == 0:
        log.warning("Webhook accepted but contained no supported text DM")
    log.info(
        "Webhook acknowledged found=%d queued=%d duplicates=%d",
        found,
        queued,
        duplicates,
    )
    return (
        jsonify(
            status="accepted",
            found=found,
            queued=queued,
            duplicates=duplicates,
        ),
        200,
    )


@app.get("/privacy")
def privacy() -> tuple[str, int, dict[str, str]]:
    html = """
    <!doctype html><html><head><title>Privacy Policy</title></head><body>
    <h1>Privacy Policy</h1>
    <p>This app processes Instagram messages and Instagram-scoped identifiers
    only to generate and send automated replies.</p>
    <p>Conversation history is held temporarily in server memory and is cleared
    when the service restarts. The app does not sell personal information.</p>
    </body></html>
    """
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


@app.get("/data-deletion")
def data_deletion() -> tuple[str, int, dict[str, str]]:
    html = """
    <!doctype html><html><head><title>Data Deletion</title></head><body>
    <h1>User Data Deletion Instructions</h1>
    <p>Send "delete my data" to the connected Instagram account to request
    immediate deletion of the in-memory conversation history associated with
    your Instagram-scoped account.</p>
    </body></html>
    """
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


for missing_name in missing_required_config():
    log.warning("Missing environment variable: %s", missing_name)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(env("PORT", "5000")))
