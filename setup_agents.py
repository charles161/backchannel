#!/usr/bin/env python3
"""
setup_agents.py — create (or reuse) the Phase-2 Bolna voice agents, plus a
role-aware reference agent.

    source ~/.bolna.env
    python3 setup_agents.py            # create missing agents, reuse existing ones by name
    python3 setup_agents.py --update   # also push the current prompts to existing agents

Prints the lines to put in .env.local:
    BACKCHANNEL_VC_AGENT_ID=...
    BACKCHANNEL_FOUNDER_AGENT_ID=...
    BACKCHANNEL_AGENT_ID=...      # optional: the role-aware reference agent. Without it
                                  # the proven Phase-1 reference agent is used, which
                                  # ignores user_data (its prompt has no placeholders).

Same stack as the Phase-1 reference agent (gpt-4o-mini, OpenAI TTS "alloy",
Deepgram nova-2). Each agent's prompt and welcome message are written against
{{placeholders}}, which POST /call fills per call from `user_data`:
  VC agent:      vc_name, founder_name, founder_first, verdict, confidence,
                 highlights, concern, referee_role, overlap
  founder agent: founder_name, vc_name, vc_slots, company, pitch
  reference agent (role-aware): candidate_name, candidate_first, role, company
Creating an agent costs no call credit. The key is read from BOLNA_API_KEY only.
"""

from __future__ import annotations

import argparse
import sys

import requests

import bolna

VC_AGENT_NAME = "Backchannel VC Callback"
FOUNDER_AGENT_NAME = "Backchannel Founder Scheduler"
REF_AGENT_NAME = "Backchannel Reference Checker (role-aware)"

VC_WELCOME = ("Hi {{vc_name}}, it's the Backchannel agent calling back about "
              "{{founder_name}}. Do you have a minute for the verdict?")
VC_PROMPT = """You are the Backchannel agent, an AI assistant calling an investor, {{vc_name}}, back with the result of a reference check on a founder, {{founder_name}}. Keep the whole call under 90 seconds: warm, crisp, one short point or question at a time.

What you know (say it in plain spoken English; never read field names aloud):
- You spoke with an off-list former colleague of {{founder_first}}: {{referee_role}}, who overlapped with them for {{overlap}}.
- Verdict: {{verdict}} (confidence: {{confidence}}).
- What stood out: {{highlights}}.
- Main watch-out: {{concern}}.

Flow:
1) Confirm it's a good moment.
2) Give the verdict in one sentence, then the two strongest highlights, then the watch-out in one sentence.
3) Ask whether they'd like to meet {{founder_first}}.
4) If yes, ask: "When are you free to meet {{founder_first}} this week?" Get at least one specific day plus a part of day or a time. If they're vague, ask for a day and a time.
5) Repeat their availability back in one sentence, say you'll call {{founder_first}} now to lock in a time, thank them, and end the call.
If they don't want to meet, thank them and end the call. Never invent facts beyond what is listed above."""

FOUNDER_WELCOME = ("Hi, is this {{founder_name}}? I'm an AI assistant calling on behalf "
                   "of {{vc_name}}, an investor. Do you have a minute?")
FOUNDER_PROMPT = """You are an AI assistant calling a founder, {{founder_name}}, on behalf of an investor, {{vc_name}}. The investor has looked at {{company}} and {{pitch}}. Keep the call under 90 seconds: friendly, brief, one question at a time.

The investor is free: {{vc_slots}}.

Flow:
1) Say you're calling on behalf of {{vc_name}}, who {{pitch}}.
2) Offer the investor's free slots and ask which works. If the investor is just "flexible this week", ask what day and time suit the founder instead.
3) Get a specific day AND clock time inside one of those slots (for example "Thursday at 3 PM"). If they only give a part of day, propose a specific time within it. If none of the slots work, ask for their availability and find the closest overlap; if there is none, take their best alternative.
4) Read the confirmed day and time back clearly, for example "Great, that's Thursday at 3:00 PM", say both sides will get a confirmation, thank them, and end the call.
Never promise investment terms. Don't share anything about the reference check."""


REF_WELCOME = ("Hi, am I speaking with the right person? I'm an AI assistant making a quick "
               "reference check call about {{candidate_name}}. Do you have two minutes?")
REF_PROMPT = """You are an AI assistant making a brief, warm reference-check call about {{candidate_name}}, who is being considered for {{role}}. You're speaking with someone who worked with {{candidate_first}} at {{company}}. Keep it under 2 minutes, natural and concise, one short question at a time.

Flow:
1) Greet, say you're an AI doing a quick reference check on {{candidate_first}}, and confirm they're OK with the call being recorded.
2) Ask how they worked with {{candidate_first}} at {{company}}, and for how long. If they didn't actually work together, thank them and end the call politely.
3) Ask what stood out about how {{candidate_first}} operated: their biggest strength.
4) Ask where {{candidate_first}} should grow, or anything they'd push back on.
5) Ask how well {{candidate_first}} would fit {{role}}, based on what they saw first-hand. If the answer is vague, ask for one concrete example.
6) Ask: would you work with them again without hesitation?
7) Thank them and end the call.
If they're busy, offer to call back. Don't share anything beyond the role you're asking about, and never invent facts."""


def _agent_body(name: str, welcome: str, prompt: str) -> dict:
    """The Phase-1 reference agent's config shape, with a new name/welcome/prompt."""
    return {
        "agent_config": {
            "agent_name": name,
            "agent_welcome_message": welcome,
            "agent_type": "other",
            "tasks": [{
                "task_type": "conversation",
                "toolchain": {"execution": "parallel",
                              "pipelines": [["transcriber", "llm", "synthesizer"]]},
                "tools_config": {
                    "llm_agent": {
                        "agent_type": "simple_llm_agent",
                        "agent_flow_type": "streaming",
                        "llm_config": {"provider": "openai", "family": "openai",
                                       "model": "gpt-4o-mini", "max_tokens": 150,
                                       "temperature": 0.3},
                    },
                    "synthesizer": {"provider": "openai", "stream": True, "buffer_size": 40,
                                    "audio_format": "wav",
                                    "provider_config": {"model": "tts-1", "voice": "alloy",
                                                        "speed": 1.0}},
                    "transcriber": {"provider": "deepgram", "model": "nova-2",
                                    "language": "en", "stream": True, "endpointing": 500},
                    "input": {"provider": "twilio", "format": "wav"},
                    "output": {"provider": "twilio", "format": "wav"},
                },
                "task_config": {"call_terminate": 120, "hangup_after_silence": 20,
                                "optimize_latency": True},
            }],
        },
        "agent_prompts": {"task_1": {"system_prompt": prompt}},
    }


AGENTS = [
    ("BACKCHANNEL_VC_AGENT_ID", VC_AGENT_NAME, VC_WELCOME, VC_PROMPT),
    ("BACKCHANNEL_FOUNDER_AGENT_ID", FOUNDER_AGENT_NAME, FOUNDER_WELCOME, FOUNDER_PROMPT),
    ("BACKCHANNEL_AGENT_ID", REF_AGENT_NAME, REF_WELCOME, REF_PROMPT),
]


def _request(method: str, path: str, body: dict | None = None) -> dict | list:
    try:
        resp = requests.request(method, f"{bolna.BASE_URL}{path}", headers=bolna._headers(),
                                json=body, timeout=30)
    except requests.RequestException as e:
        raise bolna.BolnaError(f"Bolna {path} unreachable: {e}") from None
    if resp.status_code not in (200, 201):
        raise bolna.BolnaError(f"Bolna {method} {path} -> HTTP {resp.status_code}: "
                               f"{resp.text[:300]}")
    return resp.json()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--update", action="store_true",
                    help="push the current prompts to agents that already exist")
    args = ap.parse_args()

    existing = {a.get("agent_name"): a.get("id")
                for a in (_request("GET", "/v2/agent/all") or []) if isinstance(a, dict)}
    lines = []
    for env_key, name, welcome, prompt in AGENTS:
        body = _agent_body(name, welcome, prompt)
        agent_id = existing.get(name)
        if agent_id and args.update:
            _request("PUT", f"/v2/agent/{agent_id}", body)
            print(f"updated  {name}: {agent_id}", file=sys.stderr)
        elif agent_id:
            print(f"reusing  {name}: {agent_id}", file=sys.stderr)
        else:
            agent_id = _request("POST", "/v2/agent", body).get("agent_id")
            if not agent_id:
                raise bolna.BolnaError(f"Bolna didn't return an agent_id for {name}")
            print(f"created  {name}: {agent_id}", file=sys.stderr)
        lines.append(f"{env_key}={agent_id}")
    print("\n# add to .env.local:")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
