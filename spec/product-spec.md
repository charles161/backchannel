# Backchannel — Product Spec

**One line:** A VC enters a founder they're thinking of funding. An agent finds the founder's ex-colleagues, calls one to verify him, then **calls the VC back** with the signal — and books the meeting, live.

**User:** A busy VC doing founder diligence before writing a check.

**The deliverable isn't a report. It's a decision + a booked meeting.**

---

## The flow

**1. VC starts a check.**
Enters their **name**, **phone number**, and the **founder** to check. One pane per founder (can run several).

**2. Pick the founder.**
Already has someone → type the name (or upload details/a deck). Or search and select.

**3. VC says: "Verify him — is this someone I should fund?"**
Optionally uploads the founder's deck / claims to check against.

**4. Agent finds the backchannel.**
Pulls the founder's work history (**Crustdata**) and finds the ex-colleagues most likely to have actually worked with him — ranked by **dated overlap**. Picks the **top 1** (keeps 2–3 in reserve).

**5. Agent calls the reference.**
- Says who's calling and why.
- Checks: did they **know him / work with him / see him work?**
- Based on that, asks the diligence questions — how he operates, strengths, would-you-back-him.
- Call ends → agent decides if the signal is enough.

**6. Agent calls the VC back.**
> "We've got signal on **[founder]** — here's what we heard." Highlights the key points + a verdict, then: **"Want to meet him? When are you free?"** — grabs the VC's available slots, right there on the call.

**7. The close — the agent calls the founder and schedules it.**
The agent **calls the founder**: "A VC wants to fund you and set up a meeting." Gets the founder's availability, **matches it against the VC's slots**, confirms a time to both — and the meeting is booked. (Optional: fires calendar invites.)

**The agent does the human coordination itself — three phone calls, zero people in the loop.**

---

## In scope for the demo — the 3-call chain
- **Call 1:** agent → **reference** (verify the founder).
- **Call 2:** agent → **VC** (deliver signal + grab the VC's free slots).
- **Call 3:** agent → **founder** (pitch the interest, get availability, confirm the meeting).
- One founder, happy path, end to end, fully autonomous. *(All 3 are controlled/known numbers for the demo.)*

## Out of scope (for now)
- Reference doesn't know the founder → try the next one. *(edge case, later)*
- Multiple references / parallel calls.
- Multiple founders at once.

---

## How it works (stack)
| Step | Tech |
|---|---|
| Founder history + ex-colleague discovery (overlap graph) | **Crustdata** |
| The **three** live calls (reference · VC · founder) on real phones | **Bolna** |
| Question selection · signal synthesis · verdict · slot-matching | **Haiku + Jev** (AI gateway) |
| Confirm the meeting | Done **on the calls** (verbal). Optional: a calendar invite email |

---

## Why it wins
Every other demo hands back a document. **Backchannel makes three real phone calls and schedules a meeting between two humans** — reference, VC, founder — all autonomous, all live. The agent doesn't book via an API; it does the human coordination itself. That's "the accepted offer, not a list" — taken all the way.
