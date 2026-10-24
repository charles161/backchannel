# Backchannel — UI Spec

**Principle:** The VC starts a task, then **watches it run live or walks away**. The agent does the work and **calls them back** when it matters. The screen is a live window into an autonomous agent — like watching a colleague do the job.

Three screens: **Start → Live run → Result.**

---

## Screen 1 — Start

A single clean card:
- **Your name** · **Your phone number** (so the agent can call you back)
- **Who to check** — founder's name + company *(or upload a deck / paste details)*
- Big button: **"Verify him"**

*(Multiple founders = one tab/pane each. Demo = one.)*

Press it → go straight to the Live run.

---

## Screen 2 — Live run  ← the heart of it

A **vertical timeline** that fills in step by step, in real time. Each step: **pending → ● active (spinner) → ✓ done** with its result inline.

```
●  Finding Priya's work history               ✓  Stripe, Razorpay, 2 more
●  Mapping ex-colleagues by overlap           ✓  6 found · top: Arjun (CTO, 58mo)
●  Calling Arjun to verify                    ● live  ▸ [transcript streaming…]
○  Synthesizing the signal
○  Calling you with the verdict
○  Getting the founder's availability
○  Booking the meeting
```

- The **active call** expands to show the **live transcript** (turn by turn) + a ringing/▮ talking indicator.
- When a call connects, a small **"📞 your phone is ringing"** banner appears (for call 2).
- Everything streams — the VC can just sit and watch it move.
- A quiet line up top: **"You can close this — the agent will call you."** (fire-and-forget).

---

## Screen 3 — Result

A single result card when it's done:
- **Verdict:** FUND / LEAN / PASS + confidence
- **What the reference said** — 3–4 highlights, sourced
- **Signal scores** (credibility · enthusiasm · corroboration)
- **✅ Meeting booked:** Thursday 3:00 PM with Priya
- Buttons: *Listen to the call · Copy the intro email*

---

## How "live" works
- Backend emits an **event per step** (step id, status, result payload) as the agent runs.
- UI subscribes via **SSE / WebSocket** (or 1s polling as a fallback) and updates the timeline.
- State persists, so if the VC leaves and comes back, the page shows exactly where it is.

---

## Style
- Minimal, calm, VC-grade. Lots of whitespace. One column.
- Timeline + live transcript are the only "busy" element — that's the magic, so let it breathe.
- Dark or light, one accent color. No dashboards, no clutter.

---

## Demo scope
- One founder, the 7-step timeline above, live.
- The **call-2 "your phone is ringing" moment** + the streaming transcript are the on-stage wow.
- Skip: multi-founder panes, auth, history, settings.
