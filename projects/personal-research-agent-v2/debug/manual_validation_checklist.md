# Manual Validation Checklist (v2-20260422133303)

## Process
1. Open debug/last_run_trace.json and inspect candidate/rejected/selected counts.
2. Validate each selected item manually in browser.
3. If quality is low, tune queries/policies/validator and rerun.

## News Checks
- Real article page?
- Recent and relevant for Netherlands/Limburg?
- [ ] accept [ ] reject [ ] unsure | news | Fuel costs for low-income workers could rise by €1,000: CPB | dutchnews.nl | https://www.dutchnews.nl/2026/04/fuel-costs-for-low-income-workers-could-rise-by-e1000-cpb | item_id=71ca3b660b0ed10f89b4ff4548b5e12e2c3d9d98068d5d9bbdae5d5e6288cf29 | notes:

## Events Checks
- Real event page (not listing/category)?
- Specific date/location/time present?
- [ ] accept [ ] reject [ ] unsure | events | Maastricht in 2026 | Concerts, Festivals, Tickets & Tour Dates | songkick.com | https://www.songkick.com/metro-areas/31398-netherlands-maastricht/2026 | item_id=536673ec3a95230acb7a2c948d609e96937ff45a0c1a2bdd4722e1e65ab88f28 | notes:

## Bitcoin Checks
- Real update vs noise/reference?
- Balanced market/technical/community?
- [ ] accept [ ] reject [ ] unsure | bitcoin | Mining interface tracking issue #33777 - bitcoin/bitcoin - GitHub | github.com | https://github.com/bitcoin/bitcoin/issues/33777 | item_id=6b5e305a93d12191e948d4a27ab7736ef9b338ad067bc465da0e76d4ddce23b9 | notes:

## Stale Suspects
- Threshold date_confidence: 0.55
- [ ] accept [ ] reject [ ] unsure | bitcoin | v30.0 Testing · Issue #33368 · bitcoin/bitcoin - GitHub | github.com | https://github.com/bitcoin/bitcoin/issues/33368 | item_id=b6490aaf40dc74626f5d60ffa21496cc462df63bd608ff94938006c39760c9bc | notes:

## Rejected Samples
- [ ] accept [ ] reject [ ] unsure | news | Netherlands declines to follow German example of fuel tax cuts | nltimes.nl | https://nltimes.nl/2026/04/14/netherlands-declines-follow-german-example-fuel-tax-cuts | item_id=744df85302a4d36aa84910539e517ef8bdd4e20ecb52ff89341f2c6198d39c9e | notes:
- [ ] accept [ ] reject [ ] unsure | news | Government will collect more income tax in 2026; Deficit not as bad ... | nltimes.nl | https://nltimes.nl/2025/04/18/government-will-collect-income-tax-2026-deficit-bad-anticipated | item_id=d7ee61ce3d0d590eb267912ccfa595d7c99e9d0c11710c84ac7239483b448531 | notes:
- [ ] accept [ ] reject [ ] unsure | news | Dutch economy "can take a hit," ABN Amro says - NL Times | nltimes.nl | https://nltimes.nl/2026/03/25/dutch-economy-can-take-hit-abn-amro-says | item_id=3eb4c2ab84c771491bcc93f40915db55f9323356473f64ccc0b664dbc40e2405 | notes:

## Feedback Memory Stub
- Structured stub: /home/roberto/workspace/workspace-root-langgraph/projects/personal-research-agent-v2/debug/feedback_memory_stub.json
- To use it, copy reviewed decisions into data/feedback_memory.json before the next run.