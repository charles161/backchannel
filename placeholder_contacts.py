"""
placeholder_contacts.py — the masking for the DISPLAY-ONLY contact lines on candidate
and reference cards, plus the placeholder used when there's no real contact.

The real source is Crustdata contact-enrich (contacts.py). When it has nothing for a
person (no match, an error, no key) the card shows a placeholder from here, masked
the same way, so a card never shows a blank:

  mask_email  first character of each dot-part of the local name + the domain:
              "patrick.collison@stripe.com" -> "p•••.c•••@stripe.com".
  mask_phone  country code + first 2 digits, the middle masked, the last 4:
              ("+91", "9712343821") -> "+91 97XXX X3821".
  email_for   firstname.lastname@<company-slug>.com (example.com with no company).
  phone_for   a deterministic fake from a seed, already masked. It can't be dialed
              and can't belong to anyone.

NEVER DIALED: nothing here reaches bolna.place_call. Every call dials a configured
number (BACKCHANNEL_REF_PHONE, the VC's own typed number, BACKCHANNEL_FOUNDER_PHONE),
and pipeline._live_call refuses any masked number outright (is_placeholder_phone).
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

FALLBACK_DOMAIN = "example.com"
MASK = "X"
_DOT = "•••"
# legal-entity noise dropped from a company before it becomes a domain slug
_LEGAL = {"inc", "llc", "ltd", "limited", "pvt", "private", "corp", "corporation", "co",
          "gmbh", "plc", "llp", "pte", "sa", "ag", "bv"}
_HONORIFIC = {"dr", "mr", "mrs", "ms", "prof"}


def _ascii(s: Any) -> str:
    return unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()


def _name_parts(name: Any) -> list[str]:
    """'Dr. Sandra Wasicek-Woegenstein, PhD' -> ['sandra', 'wasicekwoegenstein']."""
    head = _ascii(name).split(",")[0]
    words = [re.sub(r"[^a-z0-9]", "", w) for w in head.split()]
    words = [w for w in words if w and w not in _HONORIFIC]
    if len(words) > 2:
        words = [words[0], words[-1]]
    return words


def company_slug(company: Any) -> str:
    """'Asset Token Ventures (ATV)' -> 'assettokenventures', "Faircent - India's #1 P2P"
    -> 'faircent' (a tagline after a separator is dropped); '' when nothing usable."""
    s = _ascii(re.split(r"\s[-–—|:]\s|[|,:;]", str(company or ""))[0])
    s = re.sub(r"\(.*?\)", " ", s)
    words = [re.sub(r"[^a-z0-9]", "", w) for w in s.split()]
    words = [w for w in words if w]
    while len(words) > 1 and words[-1] in _LEGAL:
        words.pop()
    return "".join(words)[:40]


def mask_email(addr: Any) -> str | None:
    """'patrick.collison@stripe.com' -> 'p•••.c•••@stripe.com'; None if it isn't an email."""
    local, at, domain = str(addr or "").strip().lower().rpartition("@")
    parts = [p for p in local.split(".") if p]
    if not at or not parts or "." not in domain:
        return None
    return ".".join(p[0] + _DOT for p in parts) + "@" + domain


def mask_phone(country_code: Any, number: Any) -> str | None:
    """('+91', '9712343821') -> '+91 97XXX X3821': at least 4 digits always hidden. A
    shorter number keeps fewer: 8-9 digits show the last 4 ('+65 XXX X4567'), 7 digits
    the last 2. None if there are fewer than 7 digits."""
    d = re.sub(r"\D", "", str(number or ""))
    if len(d) < 7:
        return None
    cc = re.sub(r"\D", "", str(country_code or ""))
    if len(d) >= 10:
        body = f"{d[:2]}{MASK * (len(d) - 7)} {MASK}{d[-4:]}"
    elif len(d) >= 8:
        body = f"{MASK * (len(d) - 5)} {MASK}{d[-4:]}"
    else:
        body = MASK * (len(d) - 2) + d[-2:]
    return f"+{cc} {body}" if cc else body


def email_for(name: Any, company: Any, *, masked: bool = False) -> str:
    slug = company_slug(company)
    domain = f"{slug}.com" if slug else FALLBACK_DOMAIN
    addr = ".".join(_name_parts(name) or ["contact"]) + "@" + domain
    return (mask_email(addr) or addr) if masked else addr


def phone_for(seed: Any) -> str:
    """A stable, masked Indian-mobile-shaped placeholder: '+91 97XXX X3821'."""
    d = [int(c, 16) % 10 for c in hashlib.sha256(f"bc-placeholder|{seed}".encode()).hexdigest()]
    lead = "6789"[d[0] % 4]
    return f"+91 {lead}{d[1]}{MASK * 3} {MASK}{d[2]}{d[3]}{d[4]}{d[5]}"


def is_placeholder_phone(phone: Any) -> bool:
    """True for anything with a mask character — a configured number never has one."""
    return bool(re.search(r"[Xx•]", str(phone or "")))


def for_person(name: Any, company: Any, seed: Any = None, *, masked_email: bool = False) -> dict:
    """{"email", "phone"} display fields. `seed` is the person id when there is one."""
    seed = seed if seed not in (None, "", 0) else f"{name or ''}|{company or ''}"
    return {"email": email_for(name, company, masked=masked_email), "phone": phone_for(seed)}
