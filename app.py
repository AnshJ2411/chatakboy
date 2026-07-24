"""Instagram DM auto-responder using Meta webhooks and Claude Haiku 4.5.

This version has one deliberate cost rule: normal conversations are never
silenced by per-user, per-session, daily, or global reply quotas. Only clear
spam is rejected before a paid Claude request.

Render start command:
    gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 --access-logfile -
"""

from __future__ import annotations

import hashlib
import hmac
import html
import logging
import os
import random
import re
import threading
import time
import unicodedata
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any

import requests
from anthropic import Anthropic
from flask import Flask, jsonify, request


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ig-bot")
app = Flask(__name__)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def bounded_float(name: str, default: str, minimum: float, maximum: float) -> float:
    try:
        value = float(env(name, default))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be numeric") from exc
    return max(minimum, min(maximum, value))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VERIFY_TOKEN = env("VERIFY_TOKEN")
IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN")
IG_ACCOUNT_ID = env("IG_ACCOUNT_ID")
META_APP_SECRET = env("META_APP_SECRET")
ANTHROPIC_API_KEY = env("ANTHROPIC_API_KEY")
DIAGNOSTIC_TOKEN = env("DIAGNOSTIC_TOKEN")

CLAUDE_MODEL = env("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
CLAUDE_MAX_TOKENS = max(48, min(240, int(env("CLAUDE_MAX_TOKENS", "140"))))

GRAPH_API_VERSION = env("GRAPH_API_VERSION", "v25.0")
if not GRAPH_API_VERSION.startswith("v"):
    GRAPH_API_VERSION = f"v{GRAPH_API_VERSION}"
if not re.fullmatch(r"v\d+\.\d+", GRAPH_API_VERSION):
    raise RuntimeError("GRAPH_API_VERSION must look like v25.0")

MAX_TURNS = max(4, min(40, int(env("MAX_TURNS", "20"))))
MAX_TURNS -= MAX_TURNS % 2
DEDUPE_TTL_SECONDS = max(3600, int(env("DEDUPE_TTL_SECONDS", "172800")))
MAX_SEEN_EVENTS = max(2000, int(env("MAX_SEEN_EVENTS", "10000")))
MAX_PENDING_MESSAGES = max(10, int(env("MAX_PENDING_MESSAGES", "100")))
WORKER_THREADS = max(2, min(16, int(env("WORKER_THREADS", "6"))))
MESSAGE_COALESCE_SECONDS = bounded_float(
    "MESSAGE_COALESCE_SECONDS", "0.8", 0.0, 2.0
)
MIN_REPLY_DELAY_SECONDS = bounded_float(
    "MIN_REPLY_DELAY_SECONDS", "2.0", 0.0, 15.0
)
MAX_REPLY_DELAY_SECONDS = bounded_float(
    "MAX_REPLY_DELAY_SECONDS", "7.0", MIN_REPLY_DELAY_SECONDS, 20.0
)
DOUBLE_TEXT_DELAY_MIN_SECONDS = bounded_float(
    "DOUBLE_TEXT_DELAY_MIN_SECONDS", "0.8", 0.0, 5.0
)
DOUBLE_TEXT_DELAY_MAX_SECONDS = bounded_float(
    "DOUBLE_TEXT_DELAY_MAX_SECONDS",
    "2.4",
    DOUBLE_TEXT_DELAY_MIN_SECONDS,
    8.0,
)

# Spam-only protection. These defaults are intentionally lenient enough for
# normal double texts and active conversations.
MAX_USER_TEXT_CHARS = max(300, int(env("MAX_USER_TEXT_CHARS", "1600")))
SPAM_BURST_WINDOW_SECONDS = max(
    5, int(env("SPAM_BURST_WINDOW_SECONDS", "20"))
)
SPAM_BURST_MAX_MESSAGES = max(5, int(env("SPAM_BURST_MAX_MESSAGES", "8")))
SPAM_REPEAT_WINDOW_SECONDS = max(
    60, int(env("SPAM_REPEAT_WINDOW_SECONDS", "600"))
)
SPAM_REPEAT_MAX_MESSAGES = max(3, int(env("SPAM_REPEAT_MAX_MESSAGES", "4")))
SPAM_COOLDOWN_SECONDS = max(60, int(env("SPAM_COOLDOWN_SECONDS", "900")))

# Quality memory prevents Haiku from falling into repeated phrases across chats.
RECENT_REPLY_CACHE_SIZE = max(100, int(env("RECENT_REPLY_CACHE_SIZE", "350")))
RECENT_REPLY_TTL_SECONDS = max(3600, int(env("RECENT_REPLY_TTL_SECONDS", "86400")))
CHATAK_LORE_CHANCE = bounded_float("CHATAK_LORE_CHANCE", "0.025", 0.0, 0.08)
DRILL_REFERENCE_CHANCE = bounded_float("DRILL_REFERENCE_CHANCE", "0.008", 0.0, 0.03)

SEND_URL = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{IG_ACCOUNT_ID}/messages"

claude_client = (
    Anthropic(api_key=ANTHROPIC_API_KEY, timeout=30.0, max_retries=0)
    if ANTHROPIC_API_KEY
    else None
)
executor = ThreadPoolExecutor(
    max_workers=WORKER_THREADS,
    thread_name_prefix="ig-worker",
)
pending_message_slots = threading.BoundedSemaphore(MAX_PENDING_MESSAGES)
http_local = threading.local()


# ---------------------------------------------------------------------------
# Persona
# ---------------------------------------------------------------------------
ANSH_SYSTEM_PROMPT = """You are Zombie, Ansh's Instagram DM persona from Delhi. You are a person inside the chat, never an assistant, moderator, narrator, or customer-support bot. Your public name is Zombie. Say Ansh only when explicitly asked for your real/asli/legal/full name.

STYLE GROUNDING
- This voice was distilled from Ansh's real DM archive: the median message is about four words, roughly three quarters are lowercase, and question marks/full stops are rare.
- Default to 2-10 words. Use a compact 10-24 word reply only when the message genuinely needs substance. Work/AI questions may be clearer and longer.
- Mostly lowercase, little punctuation, no emojis. Natural forms include u, ur, im, dont, cant, ill, ion, n, tho, js, rn, yr, wya, fs, frfr. Rotate them; do not stack slang to perform a character.
- Code-switch between English and Delhi/Hinglish naturally. Use caps only for a sudden real reaction, then return to lowercase.
- Typos may happen lightly, but every sentence must still make sense. Never output random word salad.

NORMAL ENERGY
- Chill, low-effort, socially confident, observant, and genuinely conversational. React to one concrete detail from the newest message.
- Keep normal chats alive with a specific reaction, opinion, callback, playful assumption, or relevant question. Never dismiss somebody merely to seem cool.
- Humor is dry and nonchalant: one small punchline in lowercase, no emoji, no explanation. Avoid internet-comeback templates and fake-deep speeches.
- Do not repeatedly say bro, bhai, relax, u good, say more, or any other stock phrase.

PROVOCATION
- Zombie gets irritated quickly when disrespect is directed at him. Any direct insult, taunt, humiliating line, hostile gaali, or block/threat performance gets an immediate clapback in the very next reply.
- Retaliate in the first clause. Be direct, personal to what they actually said, and concise. Do not mediate, placate, defend yourself, deny fear, explain intent, or ask them to calm down.
- English disrespect may get short English profanity such as stfu bitch ass, fuck outta here, loud ass, or dumb ass. Never use racial slurs, even when the sender uses one.
- Hindi disrespect may get one strong Delhi gaali such as Bhenchod, Bhen k lode, Chutiye, Gandu, Bhosdike, Lodu, or Lode. Use one naturally; do not dump a list.
- Target their weak logic, contradiction, repetition, fake toughness, or inability to make a point. Do not use a memorized comeback structure.
- Keep it verbal. Never make a real threat, arrange a fight, name a location/weapon, dox, or attack a protected trait.

PRIVATE LORE
- Chatak and tutan are fictional in-character slang for a late-night session/restless craving. Mention them only when a private turn instruction asks for it, never with substances, buying, selling, doses, instructions, or encouragement for the other person.
- Jacksonville/904 drill taste may appear only when a private turn instruction asks for it. Use an original non-lyrical reference or artist nod. Never quote lyrics, mention real deaths/beefs, claim affiliation, or turn it into a credible threat.

RHYTHM AND QUALITY
- Usually one bubble. A genuine second thought may use the exact marker <DOUBLE> on its own line, at most once.
- No markdown, labels, quotation marks, stage directions, or explanations.
- Do not repeat a complete sentence, punchline, opening, or odd nickname from earlier replies.
- If the newest message is vague, reply to what is actually there instead of inventing nonsense.
- Output only the Instagram DM reply.
"""


# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
conversations: dict[str, list[dict[str, str]]] = {}
conversation_lock = threading.RLock()

seen_events: dict[str, float] = {}
seen_events_lock = threading.Lock()


@dataclass
class SenderSpamState:
    incoming_times: deque[float] = field(default_factory=deque)
    repeat_times: dict[str, deque[float]] = field(default_factory=dict)
    blocked_until: float = 0.0
    last_seen_at: float = 0.0


spam_states: dict[str, SenderSpamState] = {}
spam_lock = threading.RLock()

# (timestamp, sender_id, normalized reply). This is output-only memory and never
# stores incoming DMs. It resets whenever the Render process restarts.
recent_reply_cache: deque[tuple[float, str, str]] = deque()
recent_reply_lock = threading.RLock()


@dataclass(frozen=True)
class QueuedMessage:
    text: str
    event_key: str
    received_monotonic: float


sender_queues: dict[str, deque[QueuedMessage]] = {}
active_sender_workers: set[str] = set()
sender_queue_lock = threading.Lock()

stats: dict[str, Any] = {
    "webhooks_received": 0,
    "messages_queued": 0,
    "messages_processed": 0,
    "messages_coalesced": 0,
    "replies_sent": 0,
    "spam_silenced": 0,
    "duplicates": 0,
    "claude_calls": 0,
    "claude_input_tokens": 0,
    "claude_output_tokens": 0,
    "local_fallbacks": 0,
    "persona_repairs": 0,
    "repetition_repairs": 0,
    "unsafe_repairs": 0,
    "chatak_lore_turns": 0,
    "drill_reference_turns": 0,
    "errors": 0,
    "last_reply_at": None,
    "last_error": None,
}
stats_lock = threading.Lock()

COUNTER_STATS = {
    "webhooks_received",
    "messages_queued",
    "messages_processed",
    "messages_coalesced",
    "replies_sent",
    "spam_silenced",
    "duplicates",
    "claude_calls",
    "claude_input_tokens",
    "claude_output_tokens",
    "local_fallbacks",
    "persona_repairs",
    "repetition_repairs",
    "unsafe_repairs",
    "chatak_lore_turns",
    "drill_reference_turns",
    "errors",
}


def update_stats(**changes: Any) -> None:
    with stats_lock:
        for key, value in changes.items():
            if key in COUNTER_STATS:
                stats[key] += int(value)
            else:
                stats[key] = value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_http_session() -> requests.Session:
    session = getattr(http_local, "session", None)
    if session is None:
        session = requests.Session()
        http_local.session = session
    return session


# ---------------------------------------------------------------------------
# Webhook security and deduplication
# ---------------------------------------------------------------------------
def validate_signature(raw_body: bytes, supplied_signature: str | None) -> bool:
    """Validate Meta's X-Hub-Signature-256 when a secret is configured."""
    if not META_APP_SECRET:
        return True
    if not supplied_signature or not supplied_signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(
        META_APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, supplied_signature)


def event_key(sender_id: str, message: dict[str, Any], event: dict[str, Any]) -> str:
    mid = message.get("mid")
    if mid:
        return str(mid)
    material = "|".join(
        (sender_id, str(event.get("timestamp", "")), str(message.get("text", "")))
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def reserve_event(key: str) -> bool:
    now = time.time()
    cutoff = now - DEDUPE_TTL_SECONDS
    with seen_events_lock:
        if len(seen_events) >= 2000:
            expired = [item for item, seen_at in seen_events.items() if seen_at < cutoff]
            for item in expired:
                seen_events.pop(item, None)
        if seen_events.get(key, 0.0) >= cutoff:
            return False
        while len(seen_events) >= MAX_SEEN_EVENTS:
            seen_events.pop(next(iter(seen_events)))
        seen_events[key] = now
        return True


def release_event(key: str) -> None:
    with seen_events_lock:
        seen_events.pop(key, None)


# ---------------------------------------------------------------------------
# Spam-only paid-call firewall
# ---------------------------------------------------------------------------
def prune_times(values: deque[float], cutoff: float) -> None:
    while values and values[0] < cutoff:
        values.popleft()


def normalized_spam_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text).casefold()
    normalized = re.sub(r"https?://\S+|www\.\S+", "<url>", normalized)
    normalized = " ".join(
        "".join(char if char.isalnum() or char in "<>" else " " for char in normalized).split()
    )[:300]
    if not normalized:
        normalized = re.sub(r"\s+", "", text)[:300]
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def content_spam_reason(text: str) -> str | None:
    stripped = text.strip()
    if len(stripped) > MAX_USER_TEXT_CHARS:
        return "oversized_text"
    if len(re.findall(r"https?://|www\.", stripped, flags=re.I)) >= 4:
        return "link_flood"
    if re.search(r"(.)\1{24,}", stripped.casefold()):
        return "character_flood"

    normalized = unicodedata.normalize("NFKC", stripped).casefold()
    words = "".join(char if char.isalnum() else " " for char in normalized).split()
    if len(words) >= 16 and len(set(words)) <= 2:
        return "word_flood"
    if len(stripped) >= 20 and not words:
        return "symbol_flood"
    return None


def inspect_spam(sender_id: str, text: str, now: float | None = None) -> str | None:
    """Return a reason only for clear spam. Normal chat has no reply quota."""
    current = time.time() if now is None else now
    direct_reason = content_spam_reason(text)
    fingerprint = normalized_spam_text(text)

    with spam_lock:
        state = spam_states.setdefault(sender_id, SenderSpamState())
        state.last_seen_at = current

        if state.blocked_until > current:
            return "spam_cooldown"

        if direct_reason:
            state.blocked_until = current + SPAM_COOLDOWN_SECONDS
            return direct_reason

        prune_times(state.incoming_times, current - SPAM_BURST_WINDOW_SECONDS)
        state.incoming_times.append(current)
        if len(state.incoming_times) > SPAM_BURST_MAX_MESSAGES:
            state.blocked_until = current + SPAM_COOLDOWN_SECONDS
            return "message_burst"

        if fingerprint:
            repeats = state.repeat_times.setdefault(fingerprint, deque())
            prune_times(repeats, current - SPAM_REPEAT_WINDOW_SECONDS)
            repeats.append(current)
            if len(repeats) > SPAM_REPEAT_MAX_MESSAGES:
                state.blocked_until = current + SPAM_COOLDOWN_SECONDS
                return "repeated_message"

        if len(state.repeat_times) > 150:
            stale = [
                key
                for key, times in state.repeat_times.items()
                if not times or times[-1] < current - SPAM_REPEAT_WINDOW_SECONDS
            ]
            for key in stale:
                state.repeat_times.pop(key, None)

        if len(spam_states) > 5000:
            cutoff = current - 86400
            for key in list(spam_states):
                old = spam_states[key]
                if old.last_seen_at < cutoff and old.blocked_until < current:
                    spam_states.pop(key, None)

    return None


# ---------------------------------------------------------------------------
# Reply generation, turn modes, and local quality repair
# ---------------------------------------------------------------------------
DOUBLE_MARKER = "<DOUBLE>"
DOUBLE_PATTERN = re.compile(r"`*\s*<\s*/?\s*double\s*/?\s*>\s*`*", re.I)


def normalize_text(text: str) -> str:
    cleaned = unicodedata.normalize("NFKC", text).casefold()
    cleaned = "".join(char if char.isalnum() else " " for char in cleaned)
    return " ".join(cleaned.split())


def fixed_identity_reply(user_text: str) -> str | None:
    normalized = normalize_text(user_text)
    if re.fullmatch(
        r"(?:(?:whats|what is|tell me) )?(?:(?:your|ur|tera) )?"
        r"(?:real|actual|full|legal|government|asli) (?:name|naam)",
        normalized,
    ):
        return "ansh"
    if normalized in {
        "who is this",
        "who dis",
        "who are you",
        "who r u",
        "whats your name",
        "what is your name",
        "ur name",
        "your name",
        "name",
        "naam",
        "tera naam kya hai",
    }:
        return "zombie"
    return None


def strip_emojis(text: str) -> str:
    """Persona is text-only; remove emoji without damaging Hindi or punctuation."""
    output: list[str] = []
    for character in text:
        codepoint = ord(character)
        if (
            0x1F000 <= codepoint <= 0x1FAFF
            or 0x2600 <= codepoint <= 0x27BF
            or 0xFE00 <= codepoint <= 0xFE0F
            or 0x1F1E6 <= codepoint <= 0x1F1FF
        ):
            continue
        output.append(character)
    return "".join(output)


def sanitize_reply(reply: str) -> str:
    text = html.unescape(str(reply or "")).replace("\x00", " ").replace("```", "")
    token = "\ue000DOUBLE\ue001"
    text = DOUBLE_PATTERN.sub(f"\n{token}\n", text)
    text = re.sub(r"</?\s*(?:p|br)\b[^>]*>", " ", text, flags=re.I)
    text = re.sub(r"<[^>\r\n]{1,100}>", " ", text)
    text = re.sub(r"(?m)^\s*(?:assistant|zombie)\s*:\s*", "", text, flags=re.I)
    text = re.sub(r"(?m)^\s*(?:#{1,6}\s+|>\s*|[-*+]\s+|\d+[.)]\s+)", "", text)
    text = re.sub(r"[*_~`]+", "", text)
    text = strip_emojis(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n+ *", "\n", text).strip()
    text = text.replace(token, DOUBLE_MARKER)
    return text[:1800].strip()


ENGLISH_DIRECT_HOSTILITY = re.compile(
    r"\b(?:fuck\s+(?:u|you)|fuck\s+nigg(?:a|er)|bitch\s+ass\s+nigg(?:a|er)|stfu|shut\s+up)\b"
    r"|\b(?:u|you|ur|your)\b.{0,28}\b(?:bitch(?:\s+ass)?|pussy|clown|loser|"
    r"dumb(?:\s+ass)?|stupid(?:\s+ass)?|fatass|weak\s+ass|hoe\s+ass)\b"
    r"|\b(?:bitch(?:\s+ass)?|pussy|clown|loser|dumb(?:\s+ass)?|"
    r"stupid(?:\s+ass)?|fatass)\b.{0,18}\b(?:u|you)\b"
    r"|^(?:yo\s+)?(?:bitch|pussy|clown|loser|fatass|dumbass|stupid ass)\b",
    re.I,
)
HINDI_DIRECT_HOSTILITY = re.compile(
    r"\b(?:teri\s+ma+a?|teri\s+maa|bhen\s*k(?:e)?\s+lode|bhenchod|behenchod|"
    r"madarchod|chutiya|chutiye|gandu|bhosdike|bsdk|lodu|lode|nalle|dalle|randike|bhadwe)\b",
    re.I,
)
BLOCK_OR_THREAT_POSTURE = re.compile(
    r"\b(?:i(?:ll| will)\s+block\s+(?:u|you)|block\s+(?:u|you)|"
    r"pull\s+up|come\s+outside|run\s+the\s+fade|watch\s+ur\s+back|"
    r"ill\s+(?:beat|jump|smack|hit)\s+(?:u|you))\b",
    re.I,
)
WORK_INTENT = re.compile(
    r"\b(?:collab|project|business|client|price|cost|rate|budget|deadline|brief|"
    r"lora|ai|model|video|prompt|render|api|webhook|instagram|code|python)\b",
    re.I,
)
ENGLISH_CURSE_WORD = re.compile(
    r"\b(?:fuck|fucking|stfu|bitch|pussy|clown|loser|dumb|stupid|fatass)\b",
    re.I,
)

WEAK_HOSTILE_REPLY = re.compile(
    r"\b(?:relax|calm down|chill bro|u good|you good|why are u mad|why are you mad|"
    r"not even trippin|not trippin|not tripping|im not bothered|i am not bothered|"
    r"im not scared|i am not scared|not fazed|not phased|i dont want beef|"
    r"i do not want beef|leave me alone|lets not fight|let us not fight|"
    r"chat over|goodbye|my bad|sorry)\b|^blocked\.?$",
    re.I,
)
MODEL_META_REPLY = re.compile(
    r"\b(?:as an ai|i cant assist|i cannot assist|i am unable to|policy|guidelines|"
    r"i dont have feelings|language model|system prompt)\b",
    re.I,
)
PROTECTED_SLUR_REPLY = re.compile(
    r"\b(?:nigg(?:a|er)s?|chink|paki|faggot|tranny|kike)\b",
    re.I,
)
CREDIBLE_THREAT_REPLY = re.compile(
    r"\b(?:i(?:ll| will|m gonna| am gonna)\s+(?:kill|shoot|stab|jump|beat|smack|"
    r"hurt|find|pull up on)\s+(?:u|you)|maa\s+chod\s+dunga|ghar\s+aa(?:unga|ra)|"
    r"address\s+bhej|location\s+bhej)\b",
    re.I,
)
DANGEROUS_SUBSTANCE_REPLY = re.compile(
    r"\b(?:buy|sell|score|dealer|plug|dose|dosage|grams?|mg|mix)\b.{0,25}\b"
    r"(?:coke|cocaine|mdma|meth|weed|thc|xanax|perc|lean|acid|lsd)\b",
    re.I,
)


def classify_turn(user_text: str) -> tuple[str, str]:
    """Return (mode, register). Direct disrespect always wins over other modes."""
    normalized = normalize_text(user_text)
    english = bool(ENGLISH_DIRECT_HOSTILITY.search(normalized))
    hindi = bool(HINDI_DIRECT_HOSTILITY.search(normalized))
    threat = bool(BLOCK_OR_THREAT_POSTURE.search(normalized))
    if english or hindi or threat:
        if hindi and english:
            register = "mixed"
        elif hindi:
            register = "hindi"
        else:
            register = "english"
        return "provoked", register
    if WORK_INTENT.search(normalized):
        return "work", "neutral"
    return "normal", "neutral"


def build_turn_system_prompt(
    user_text: str,
    previous_history: list[dict[str, str]],
) -> tuple[str, str]:
    mode, register = classify_turn(user_text)
    if mode == "provoked":
        if register == "hindi":
            register_instruction = (
                "Prefer one natural Delhi gaali from the allowed list, then attack the exact flaw."
            )
        elif register == "english":
            register_instruction = (
                "Prefer direct English profanity such as stfu bitch ass or fuck outta here; never copy a racial slur from them."
            )
        else:
            register_instruction = (
                "Use whichever English/Hinglish register fits naturally, with one strong profanity at most."
            )
        return (
            ANSH_SYSTEM_PROMPT
            + "\n\nPRIVATE TURN MODE — PROVOKED\n"
            + "Direct disrespect is present. Clap back immediately in the first clause. "
            + "Keep it roughly 2-16 words, specific to their newest line, irritated rather than theatrical. "
            + register_instruction
            + " No apology, fear denial, emotional explanation, therapy language, warning, or real threat. "
            + "Do not ask a soft question and do not end the conversation unless they clearly ended it.",
            "provoked",
        )

    if mode == "work":
        return (
            ANSH_SYSTEM_PROMPT
            + "\n\nPRIVATE TURN MODE — WORK\n"
            + "Be concise but actually useful. Clarify only the missing detail that materially changes the answer. "
            + "Do not force gaalis, chatak lore, drill references, or fake mystery into work.",
            "work",
        )

    has_prior_assistant = any(
        turn.get("role") == "assistant" for turn in previous_history
    )
    roll = random.random() if has_prior_assistant else 1.0
    if roll < DRILL_REFERENCE_CHANCE:
        update_stats(drill_reference_turns=1)
        return (
            ANSH_SYSTEM_PROMPT
            + "\n\nPRIVATE TURN MODE — RARE 904 NOD\n"
            + "Reply normally, but weave in one very brief original Jacksonville/904 drill-flavored reference or artist nod. "
            + "No lyrics, real beef/deaths, affiliation claim, or credible threat. It must still answer the actual message.",
            "drill",
        )
    if roll < DRILL_REFERENCE_CHANCE + CHATAK_LORE_CHANCE:
        update_stats(chatak_lore_turns=1)
        return (
            ANSH_SYSTEM_PROMPT
            + "\n\nPRIVATE TURN MODE — CHATAK LORE\n"
            + "Reply to the actual message, then naturally mention chatak or tutan once as vague fictional late-night-session slang. "
            + "No substance name, sourcing, buying, selling, dose, instruction, invitation, or encouragement.",
            "chatak",
        )
    return ANSH_SYSTEM_PROMPT, "normal"


def _reply_fingerprints(reply: str) -> list[str]:
    cleaned = sanitize_reply(reply)
    values = [cleaned.replace(DOUBLE_MARKER, " ")]
    values.extend(cleaned.split(DOUBLE_MARKER))
    result: list[str] = []
    for value in values:
        normalized = normalize_text(value)
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _similar(left: str, right: str, threshold: float) -> bool:
    return SequenceMatcher(None, left, right, autojunk=False).ratio() >= threshold


def is_repetitive_reply(sender_id: str, reply: str, now: float | None = None) -> bool:
    candidates = _reply_fingerprints(reply)
    if not candidates:
        return True

    with conversation_lock:
        sender_previous = [
            fingerprint
            for turn in conversations.get(sender_id, [])[-16:]
            if turn.get("role") == "assistant"
            for fingerprint in _reply_fingerprints(turn.get("content", ""))
        ]

    for candidate in candidates:
        for previous in sender_previous:
            if candidate == previous:
                return True
            if (
                len(candidate.split()) >= 5
                and len(previous.split()) >= 5
                and _similar(candidate, previous, 0.84)
            ):
                return True

    current = time.time() if now is None else now
    with recent_reply_lock:
        while (
            recent_reply_cache
            and recent_reply_cache[0][0] < current - RECENT_REPLY_TTL_SECONDS
        ):
            recent_reply_cache.popleft()
        global_previous = [value for _, _, value in recent_reply_cache]

    for candidate in candidates:
        for previous in global_previous:
            if candidate == previous:
                return True
            if (
                len(candidate.split()) >= 6
                and len(previous.split()) >= 6
                and _similar(candidate, previous, 0.91)
            ):
                return True
    return False


def remember_recent_reply(sender_id: str, reply: str, now: float | None = None) -> None:
    current = time.time() if now is None else now
    fingerprints = _reply_fingerprints(reply)
    if not fingerprints:
        return
    with recent_reply_lock:
        while (
            recent_reply_cache
            and recent_reply_cache[0][0] < current - RECENT_REPLY_TTL_SECONDS
        ):
            recent_reply_cache.popleft()
        existing = {value for _, _, value in recent_reply_cache}
        for fingerprint in fingerprints:
            if fingerprint not in existing:
                recent_reply_cache.append((current, sender_id, fingerprint))
                existing.add(fingerprint)
        while len(recent_reply_cache) > RECENT_REPLY_CACHE_SIZE:
            recent_reply_cache.popleft()


def recent_assistant_replies(sender_id: str) -> set[str]:
    with conversation_lock:
        return {
            normalize_text(turn.get("content", ""))
            for turn in conversations.get(sender_id, [])[-16:]
            if turn.get("role") == "assistant"
        }


def candidate_is_fresh(sender_id: str, candidate: str) -> bool:
    normalized = normalize_text(candidate)
    if not normalized or normalized in recent_assistant_replies(sender_id):
        return False
    with recent_reply_lock:
        global_recent = {value for _, _, value in recent_reply_cache}
    return normalized not in global_recent


def choose_fresh(sender_id: str, candidates: tuple[str, ...]) -> str:
    shuffled = list(candidates)
    random.shuffle(shuffled)
    for candidate in shuffled:
        if candidate_is_fresh(sender_id, candidate):
            return candidate
    return shuffled[0]


def fallback_reply(sender_id: str, user_text: str) -> str:
    identity = fixed_identity_reply(user_text)
    if identity:
        return identity

    normalized = normalize_text(user_text)
    mode, register = classify_turn(user_text)
    if mode == "provoked":
        if "block" in normalized:
            candidates = (
                "kar na block announcement kyu",
                "button daba speech band",
                "block karna h to kar bhenchod",
                "itna build up ek button ke liye",
                "live commentary band kar n block kar",
                "bhen k lode block bhi permission leke karega",
            )
        elif any(word in normalized for word in ("scared", "afraid", "pressed", "shook", "dar")):
            candidates = (
                "stfu bitch ass apni fantasy apne paas rakh",
                "Bhen k lode film kam kar",
                "u needed that story bad",
                "Chutiye tu khud convince hora",
                "fake pressure leke kaha jaara",
                "bitch ass line rehearsed lagri",
            )
        elif register == "hindi":
            candidates = (
                "Bhenchod point bol bakchodi nahi",
                "Bhen k lode sentence to bana le",
                "Chutiye tu khud samajhra h kya bolra",
                "Gandu volume se logic ni aata",
                "Bhosdike same gaali repeat mat kar",
                "Lode pehle context samajh",
                "Bhenchod itna bolke bhi point missing",
                "Chutiye seedha bol nautanki band",
            )
        else:
            candidates = (
                "stfu bitch ass point bol",
                "fuck outta here u said nothing",
                "u loud as fuck n still wrong",
                "bitch ass sentence bhi complete nahi hua",
                "stfu n try that again properly",
                "all that mouth no point",
                "dumb ass take n full confidence",
                "fuck u yapping for say it straight",
                "bitch ass logic collapsed mid sentence",
            )
        return choose_fresh(sender_id, candidates)

    text = user_text.strip().lower()
    if "hoes" in normalized or "bitches" in normalized:
        return choose_fresh(
            sender_id,
            ("who told u that", "source kya h", "rumours moving fast", "u believe anything"),
        )
    if re.search(r"\b(?:this|that)\s+bitch\b", text):
        return choose_fresh(
            sender_id,
            ("what happened now", "what she do", "fir kya kardiya", "context de"),
        )
    if re.search(r"\b(?:h+i+|he+y+|hello+|yo+|wsg|wassup|whats up|sup)\b", text):
        return choose_fresh(
            sender_id,
            ("wsg", "hii kya scene", "yo bol", "kya hora", "wya", "haanji bol"),
        )
    if any(word in normalized for word in ("price", "cost", "rate", "budget")):
        return choose_fresh(
            sender_id,
            ("send details n budget", "scope n budget bhej", "brief pehle", "budget kya h"),
        )
    if any(word in normalized for word in ("collab", "work", "project", "business")):
        return choose_fresh(
            sender_id,
            ("brief deadline budget bhej", "actual project bhej ill see", "scope kya h", "details bhej"),
        )
    if "?" in user_text:
        return choose_fresh(
            sender_id,
            ("context de", "depends kya scene h", "wait explain", "kis sense me", "haan but why"),
        )
    return choose_fresh(
        sender_id,
        ("haan n", "bol aage", "wait context", "fir kya hua", "ye kab hua", "real", "fair"),
    )


def obvious_nonsense(reply: str, *, work_mode: bool) -> bool:
    cleaned = sanitize_reply(reply).replace(DOUBLE_MARKER, " ")
    normalized = normalize_text(cleaned)
    words = normalized.split()
    if not words:
        return True
    if MODEL_META_REPLY.search(cleaned):
        return True
    if len(words) > (85 if work_mode else 34):
        return True
    if len(words) >= 6 and len(set(words)) <= max(2, len(words) // 4):
        return True
    if re.search(r"\b(\w+)\s+\1\s+\1\b", normalized):
        return True
    if cleaned.count("?") > 2 or cleaned.count("!") > 3:
        return True
    alphanumeric = sum(character.isalnum() for character in cleaned)
    visible = sum(not character.isspace() for character in cleaned)
    if visible >= 12 and alphanumeric / max(1, visible) < 0.45:
        return True
    return False


def unsafe_reply(reply: str) -> bool:
    return bool(
        PROTECTED_SLUR_REPLY.search(reply)
        or CREDIBLE_THREAT_REPLY.search(reply)
        or DANGEROUS_SUBSTANCE_REPLY.search(reply)
    )


def enforce_rare_mode(sender_id: str, reply: str, turn_mode: str) -> str:
    cleaned = sanitize_reply(reply)
    normalized = normalize_text(cleaned)
    if turn_mode != "chatak" or "chatak" in normalized or "tutan" in normalized:
        return cleaned
    lore_line = choose_fresh(
        sender_id,
        (
            "btw tutan hori chatak ki",
            "lowkey chatak ki tutan hori",
            "chatak ki tutan alag chalri",
            "tutan chalri chatak ki rn",
        ),
    )
    if DOUBLE_MARKER in cleaned:
        return f"{cleaned} {lore_line}".strip()
    return f"{cleaned}\n{DOUBLE_MARKER}\n{lore_line}".strip()


def repair_persona_reply(
    sender_id: str,
    user_text: str,
    draft: str,
    turn_mode: str,
) -> str:
    cleaned = sanitize_reply(draft)
    work_mode = turn_mode == "work"
    hostile_mode = turn_mode == "provoked"

    reason: str | None = None
    if not cleaned:
        reason = "empty"
    elif unsafe_reply(cleaned):
        reason = "unsafe"
        update_stats(unsafe_repairs=1)
    elif hostile_mode and WEAK_HOSTILE_REPLY.search(cleaned):
        reason = "weak_hostile"
    elif obvious_nonsense(cleaned, work_mode=work_mode):
        reason = "nonsense"
    elif is_repetitive_reply(sender_id, cleaned):
        reason = "repetition"
        update_stats(repetition_repairs=1)

    if reason:
        log.info(
            "Locally repaired Claude reply sender_suffix=%s reason=%s",
            sender_id[-6:],
            reason,
        )
        update_stats(persona_repairs=1, local_fallbacks=1)
        cleaned = fallback_reply(sender_id, user_text)

    cleaned = enforce_rare_mode(sender_id, cleaned, turn_mode)
    cleaned = sanitize_reply(cleaned)

    # A fallback can theoretically collide after a long runtime. Pick once more
    # locally instead of spending a second Claude call or going silent.
    if is_repetitive_reply(sender_id, cleaned):
        update_stats(repetition_repairs=1, persona_repairs=1, local_fallbacks=1)
        cleaned = fallback_reply(sender_id, user_text)

    remember_recent_reply(sender_id, cleaned)
    return cleaned


def request_claude(
    messages: list[dict[str, str]],
    system_prompt: str,
) -> str:
    if not claude_client:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured")

    log.info("Generating Claude reply model=%s", CLAUDE_MODEL)
    response = claude_client.messages.create(
        model=CLAUDE_MODEL,
        system=system_prompt,
        messages=messages,
        max_tokens=CLAUDE_MAX_TOKENS,
        temperature=0.82,
    )
    reply = "".join(
        block.text
        for block in response.content
        if getattr(block, "type", "") == "text"
    ).strip()
    usage = getattr(response, "usage", None)
    update_stats(
        claude_calls=1,
        claude_input_tokens=getattr(usage, "input_tokens", 0) or 0,
        claude_output_tokens=getattr(usage, "output_tokens", 0) or 0,
    )
    return reply


def generate_reply(sender_id: str, user_text: str) -> str:
    identity = fixed_identity_reply(user_text)
    if identity:
        remember_recent_reply(sender_id, identity)
        return identity

    with conversation_lock:
        history = list(conversations.get(sender_id, []))[-MAX_TURNS:]
    messages = history + [{"role": "user", "content": user_text}]
    system_prompt, turn_mode = build_turn_system_prompt(user_text, history)

    try:
        draft = request_claude(messages, system_prompt)
    except Exception as exc:
        log.exception("Claude generation failed; using local persona fallback")
        update_stats(
            errors=1,
            local_fallbacks=1,
            last_error=f"Claude: {type(exc).__name__}",
        )
        draft = fallback_reply(sender_id, user_text)

    return repair_persona_reply(sender_id, user_text, draft, turn_mode)


def split_reply_bubbles(reply: str) -> list[str]:
    cleaned = sanitize_reply(reply)
    parts = cleaned.split(DOUBLE_MARKER, maxsplit=1)
    bubbles = [re.sub(r"\s+", " ", part).strip()[:900] for part in parts]
    return [bubble for bubble in bubbles if bubble][:2]


def remember_turn(sender_id: str, user_text: str, reply: str) -> None:
    with conversation_lock:
        history = list(conversations.get(sender_id, []))
        history.extend(
            (
                {"role": "user", "content": user_text},
                {"role": "assistant", "content": reply},
            )
        )
        conversations[sender_id] = history[-MAX_TURNS:]


def is_data_deletion_request(text: str) -> bool:
    normalized = " ".join(text.lower().strip().split())
    return normalized in {
        "delete my data",
        "delete my chat data",
        "forget me",
        "clear my history",
    }


# ---------------------------------------------------------------------------
# Instagram delivery
# ---------------------------------------------------------------------------
def graph_error_is_transient(response: requests.Response) -> bool:
    if response.status_code == 429 or response.status_code >= 500:
        return True
    try:
        body = response.json()
    except ValueError:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return False
    if error.get("is_transient") is True:
        return True
    try:
        return int(error.get("code")) in {1, 2, 4, 17, 32, 341, 613}
    except (TypeError, ValueError):
        return False


def send_message(recipient_id: str, text: str) -> None:
    if not IG_ACCESS_TOKEN or not IG_ACCOUNT_ID:
        raise RuntimeError("IG_ACCESS_TOKEN and IG_ACCOUNT_ID must be configured")

    payload = {"recipient": {"id": recipient_id}, "message": {"text": text}}
    headers = {"Authorization": f"Bearer {IG_ACCESS_TOKEN}"}
    session = get_http_session()

    for attempt in range(1, 4):
        try:
            response = session.post(
                SEND_URL,
                headers=headers,
                json=payload,
                timeout=(5, 25),
            )
        except requests.ConnectTimeout:
            if attempt == 3:
                raise
            time.sleep(2 ** (attempt - 1))
            continue

        if 200 <= response.status_code < 300:
            log.info("Instagram reply sent recipient_suffix=%s", recipient_id[-6:])
            update_stats(replies_sent=1, last_reply_at=utc_now())
            return

        log.error(
            "Instagram send failed status=%s body=%s",
            response.status_code,
            response.text[:800],
        )
        if attempt == 3 or not graph_error_is_transient(response):
            response.raise_for_status()
        retry_after = response.headers.get("Retry-After")
        try:
            delay = min(30.0, max(0.0, float(retry_after))) if retry_after else 2 ** (attempt - 1)
        except ValueError:
            delay = 2 ** (attempt - 1)
        time.sleep(delay)


def first_reply_delay_seconds(
    user_text: str,
    first_bubble: str,
    received_monotonic: float,
) -> float:
    if MAX_REPLY_DELAY_SECONDS <= 0:
        return 0.0
    elapsed = max(0.0, time.monotonic() - received_monotonic)
    reading = random.uniform(0.8, 1.5) + min(1.8, len(user_text) / 110.0)
    typing = len(first_bubble) / random.uniform(14.0, 20.0)
    target = max(
        MIN_REPLY_DELAY_SECONDS,
        min(MAX_REPLY_DELAY_SECONDS, reading + typing),
    )
    return max(0.0, target - elapsed)


def process_message(sender_id: str, batch: list[QueuedMessage]) -> None:
    combined_text = "\n".join(item.text for item in batch).strip()
    received_at = max(item.received_monotonic for item in batch)

    try:
        if is_data_deletion_request(combined_text):
            with conversation_lock:
                conversations.pop(sender_id, None)
            reply = "done ur chat history is deleted"
        else:
            reply = generate_reply(sender_id, combined_text)

        bubbles = split_reply_bubbles(reply)
        if not bubbles:
            bubbles = [fallback_reply(sender_id, combined_text)]

        delivered: list[str] = []
        for index, bubble in enumerate(bubbles):
            if index == 0:
                delay = first_reply_delay_seconds(combined_text, bubble, received_at)
            else:
                delay = random.uniform(
                    DOUBLE_TEXT_DELAY_MIN_SECONDS,
                    DOUBLE_TEXT_DELAY_MAX_SECONDS,
                )
            if delay > 0:
                time.sleep(delay)
            send_message(sender_id, bubble)
            delivered.append(bubble)

        if not is_data_deletion_request(combined_text):
            remember_turn(sender_id, combined_text, "\n".join(delivered))
        update_stats(messages_processed=len(batch))
    except Exception as exc:
        update_stats(errors=1, last_error=f"{type(exc).__name__}: {exc}")
        log.exception("Failed to process Instagram DM sender_suffix=%s", sender_id[-6:])
    finally:
        for _ in batch:
            pending_message_slots.release()


def take_sender_batch(sender_id: str) -> list[QueuedMessage]:
    while True:
        with sender_queue_lock:
            queue = sender_queues.get(sender_id)
            if not queue:
                sender_queues.pop(sender_id, None)
                active_sender_workers.discard(sender_id)
                return []
            deadline = queue[-1].received_monotonic + MESSAGE_COALESCE_SECONDS

        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
            continue

        with sender_queue_lock:
            queue = sender_queues.get(sender_id)
            if not queue:
                continue
            batch = list(queue)
            queue.clear()
            return batch


def sender_worker(sender_id: str) -> None:
    try:
        while True:
            batch = take_sender_batch(sender_id)
            if not batch:
                return
            if len(batch) > 1:
                update_stats(messages_coalesced=len(batch) - 1)
            process_message(sender_id, batch)
    except Exception:
        log.exception("Sender worker crashed sender_suffix=%s", sender_id[-6:])
        with sender_queue_lock:
            remaining = list(sender_queues.pop(sender_id, deque()))
            active_sender_workers.discard(sender_id)
        for _ in remaining:
            pending_message_slots.release()


def enqueue_message(sender_id: str, message: QueuedMessage) -> bool:
    if not pending_message_slots.acquire(blocking=False):
        return False

    try:
        with sender_queue_lock:
            queue = sender_queues.setdefault(sender_id, deque())
            queue.append(message)
            if sender_id not in active_sender_workers:
                active_sender_workers.add(sender_id)
                executor.submit(sender_worker, sender_id)
        update_stats(messages_queued=1)
        return True
    except Exception:
        pending_message_slots.release()
        raise


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
def missing_required_config() -> list[str]:
    values = {
        "VERIFY_TOKEN": VERIFY_TOKEN,
        "IG_ACCESS_TOKEN": IG_ACCESS_TOKEN,
        "IG_ACCOUNT_ID": IG_ACCOUNT_ID,
        "ANTHROPIC_API_KEY": ANTHROPIC_API_KEY,
    }
    return [name for name, value in values.items() if not value]


@app.get("/")
@app.get("/health")
def health() -> tuple[Any, int]:
    missing = missing_required_config()
    return (
        jsonify(
            status="ok" if not missing else "configuration_incomplete",
            missing=missing,
            claude="configured" if ANTHROPIC_API_KEY else "local_fallback_only",
            model=CLAUDE_MODEL,
            pending=MAX_PENDING_MESSAGES - pending_message_slots._value,
            spam_policy="spam_only",
        ),
        200,
    )


@app.get("/ready")
def ready() -> tuple[Any, int]:
    missing = missing_required_config()
    return jsonify(status="ready" if not missing else "not_ready", missing=missing), (200 if not missing else 503)


@app.get("/diagnostics")
def diagnostics() -> tuple[Any, int]:
    if DIAGNOSTIC_TOKEN and request.args.get("token") != DIAGNOSTIC_TOKEN:
        return jsonify(status="unauthorized"), 401
    with stats_lock:
        snapshot = dict(stats)
    with sender_queue_lock:
        snapshot["active_sender_workers"] = len(active_sender_workers)
        snapshot["queued_senders"] = len(sender_queues)
    snapshot["model"] = CLAUDE_MODEL
    snapshot["spam_policy"] = "spam_only"
    return jsonify(snapshot), 200


@app.get("/webhook")
def verify_webhook() -> tuple[str, int]:
    if (
        request.args.get("hub.mode") == "subscribe"
        and request.args.get("hub.verify_token") == VERIFY_TOKEN
        and VERIFY_TOKEN
    ):
        return request.args.get("hub.challenge", ""), 200
    return "verification failed", 403


@app.post("/webhook")
def handle_webhook() -> tuple[Any, int]:
    update_stats(webhooks_received=1)
    raw_body = request.get_data(cache=True)
    if not validate_signature(raw_body, request.headers.get("X-Hub-Signature-256")):
        log.warning("Rejected webhook with invalid signature")
        return jsonify(status="invalid_signature"), 401

    data = request.get_json(silent=True) or {}
    if data.get("object") != "instagram":
        return jsonify(status="ignored"), 200

    queued = 0
    spammed = 0
    duplicates = 0

    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender_id = str(event.get("sender", {}).get("id", ""))
            message = event.get("message") or {}

            if (
                not sender_id
                or message.get("is_echo")
                or not isinstance(message.get("text"), str)
            ):
                continue

            user_text = message["text"].strip()
            if not user_text:
                continue

            key = event_key(sender_id, message, event)
            if not reserve_event(key):
                duplicates += 1
                update_stats(duplicates=1)
                continue

            spam_reason = inspect_spam(sender_id, user_text)
            if spam_reason:
                spammed += 1
                update_stats(spam_silenced=1)
                log.info(
                    "Silenced clear spam sender_suffix=%s reason=%s",
                    sender_id[-6:],
                    spam_reason,
                )
                continue

            queued_message = QueuedMessage(
                text=user_text,
                event_key=key,
                received_monotonic=time.monotonic(),
            )
            if not enqueue_message(sender_id, queued_message):
                release_event(key)
                log.error("Pending message queue is full; asking Meta to retry")
                return jsonify(status="busy"), 503
            queued += 1

    return jsonify(
        status="accepted",
        queued=queued,
        spammed=spammed,
        duplicates=duplicates,
    ), 200


for missing_name in missing_required_config():
    log.warning("Missing environment variable: %s", missing_name)
if not META_APP_SECRET:
    log.warning("META_APP_SECRET is not set; webhook signature checks are disabled")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(env("PORT", "5000")))
