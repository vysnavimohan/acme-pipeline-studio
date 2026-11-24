# Demo script: Flagship (Margin & Cash) — copy and paste

App: your app URL (`bin/install_app.sh status`) → **New request**.
The grill asks ONE thing per turn. Paste the answer that matches what it just asked (the order can vary a little;
the label on each agent bubble, e.g. "AUDIENCE", tells you which step it is on).

## 1. The ask (first message)
```
I want a Genie Agent the finance team can ask where we're leaking margin and what's happening with cash, and a dashboard so we can track it every week.
```

## 2. If it asks what it is FOR (purpose)
```
To see each week where gross margin is leaking against plan, and what is happening with cash: receivables, overdue debt and aged stock.
```

## 3. If it asks which OUTPUTS (dashboard, Genie Agent or both)
```
Both: the Genie Agent for ad-hoc questions during the week, and a dashboard for the Monday meeting.
```

## 4. WHO uses it and what DECISIONS (audience)
```
FP&A analysts, to explain the gap to plan in the Monday meeting. Category managers, to decide where to fix pricing, discounting and freight. Credit control, to decide which overdue accounts to chase first.
```

## 5. Day-one QUESTIONS (it proposes a list)
Accept its list:
```
Yes, those questions are right.
```
Or give your own:
```
1. Which categories and branches are below plan gross margin this week, and by how many bps?
2. Is the leak from discounting, freight or supplier rebates?
3. What is DSO against the 42-day target?
4. Which customer segments have the most overdue more than 30 days?
5. How much stock is older than 180 days, and in which branches?
```

## 6. MEASURES and definitions (it proposes them from the catalog)
```
Yes, use those measures and definitions.
```

## 7. CADENCE
```
Weekly, Monday 07:00 Sydney, for the prior Monday-to-Sunday week.
```
The agent then says it is checking what already exists.

## 8. The four cards (click, in order)
1. **What exists (reuse card)**: show REUSE / EXTEND / GAP and the coverage %.
   - Optional pushback: click **"Build a new Genie space on the raw tables instead"**; it refuses (must sit on metric views).
   - Click **Agree / extend** to continue.
2. **Metric view card**: either a validated new view for the gap (show the YAML and sample rows) → **Add to spec**, or "everything is reused".
3. **Data quality card**:
   - Show the existing rules from the rules repo.
   - Try to switch one off: it needs a reason. Type a reason, e.g. `Duplicate of the branch_id rule upstream` → confirm.
   - Optional pushback (paste in the chat): `rebate_pct must be under 20% on the sales invoice lines` → rejected, no such column.
   - Tick the proposed gap rules you want → **Save DQ review**.
4. **Design card**: Genie Agent (title, instructions, benchmarks with computed expected answers) + dashboard (pages, widgets).
   - Optional pushback: tick **"Include customer names on the shared tracker"** → refused, unticks.
   - Click **Confirm design**.

## 9. Governance (chat)
```
confirm
```
(If it asks again: `Yes, I'm happy with these defaults.`)
Then click **Submit** → **Open the request in the tracker**.

## 10. Tracker
1. Plan card → **Approve plan → dispatch to Genie Code** (human gate #1). Builds take ~5–8 min.
2. When it pauses: **PR preview** → show artefacts + AI review → **Approve** (human gate #2).
3. Staging → prod → release (~4 min). **Use it** panel: prod Genie Agent, dashboard, benchmark table.
4. In the prod Genie Agent ask:
```
Where are we leaking margin this week, by category?
```
Expect: Fasteners 30.1% vs 31.5% plan, about -140 bps.

---

# Other scenarios — copy and paste

Same rule as above: paste the answer that matches the step the agent is on (the bubble label tells you). After
cadence, use the CARD BUTTONS (Agree → Add to spec → DQ save → Confirm design), then type `confirm`, then Submit.

---

## Scenario A — Vague ask (shows the grilling)

**1. Ask**
```
Create a new AI/BI dashboard
```
Expect: "What is the dashboard for? …" — nothing assumed, the "What it is for" panel is empty.

**2. Stay vague on purpose (it pushes back)**
```
It's for the business.
```
```
Everyone, really.
```

**3. Then give it a real answer (this becomes a dashboard-only build)**
Purpose:
```
So category managers can see which branches give away the most discount and coach them.
```
Outputs:
```
Just a dashboard, no Genie Agent.
```
Audience and decisions:
```
Category managers, to decide which branches need pricing coaching this week.
```
Questions:
```
Yes, those questions are right.
```
Measures:
```
Yes, use those measures and definitions.
```
Cadence:
```
Weekly, Monday 07:00 Sydney.
```

---

## Scenario B — Dashboard only (discounting by branch)

Ask:
```
A dashboard for category managers showing discounting by branch.
```
Purpose:
```
So category managers can see which branches give away the most discount and coach them.
```
Outputs:
```
Just a dashboard, no Genie Agent.
```
Audience and decisions:
```
Category managers, to decide which branches need pricing coaching this week.
```
Questions / Measures / Cadence:
```
Yes, those questions are right.
```
```
Yes, use those measures and definitions.
```
```
Weekly, Monday 07:00 Sydney.
```
Optional extra (once the design-change step is deployed): on the design card
```
Add product type and sales count to the dashboard.
```

---

## Scenario C — Genie Agent only (DSO by segment)

Ask:
```
A Genie Agent for credit control about DSO by customer segment.
```
Purpose:
```
So credit control can see which customer segments pay slowest and chase them first.
```
Outputs:
```
Only the Genie Agent, no dashboard.
```
Audience and decisions:
```
Credit control, to decide which segments to chase first each morning.
```
Questions / Measures / Cadence:
```
Yes, those questions are right.
```
```
Yes, use those measures and definitions.
```
```
Daily freshness is fine; it's for ad-hoc questions.
```
After release, ask the prod agent:
```
What is DSO against the 42-day target, and which segment is slowest?
```

---

## Scenario D — Pipeline from raw files (delivery times)

Ask:
```
A dashboard of delivery times for the warehouse team.
```
Purpose:
```
To see which carriers deliver late so warehouse managers can escalate them.
```
Outputs:
```
Just a dashboard.
```
Audience and decisions:
```
Warehouse managers, to decide which carriers to escalate each week.
```
Questions:
```
Yes, those questions are right.
```
(or your own)
```
1. What is our on-time delivery % this week, by carrier?
2. Which carrier and region has the worst on-time %?
3. What is the average days from order to delivery, by branch?
```
Measures / Cadence:
```
Yes, use those measures and definitions.
```
```
Weekly, Monday 07:00 Sydney.
```
Expect: reuse card shows **RAW · pipeline needed** → Agree → **Pipeline card** validates (~1 min; shows the raw feed,
bronze/silver/gold, expectations, SQL) → Add to spec → metric view "validated at build" → DQ → design → confirm →
Submit. Tracker plan starts with "Build the pipeline (Genie Code)". Planted answer: on-time ≈ 91%, FastFreight in
the East is the worst (~18% late).

---

## Scenario E — Data that doesn't exist (honest refusal)

Ask:
```
A dashboard comparing our prices with competitor prices for the category managers.
```
Expect: "That is not in the Acme Corp data I can see…", a stop card with **Log this request**, Submit stays locked,
and it never steers you to a different topic.

---

## Pushbacks you can show anywhere (paste in the chat)
```
Can Power BI calculate DSO instead?
```
```
Just build a new Genie space on the raw tables.
```
```
Put customer names on the shared dashboard.
```
During the DQ card:
```
rebate_pct must be under 20% on the sales invoice lines
```
(rejected: no such column)
