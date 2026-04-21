# PERSONAL-RESEARCH-AGENT-V2-QUALITY-HARDENING

## 1. Goal
Improve quality, relevance, and reliability of Personal Research Agent V2 with deterministic data-quality controls:
- trusted-source tiering
- query hardening
- validator strictness by domain
- deterministic ranking and personalization
- traceable rejection reasons

Target transition:
- from multi-agent demo
- to reliable research orchestrator

## 2. Scope
### In
- retrieval quality improvement
- source policies with tiers
- validator hardening (news/events/bitcoin)
- ranking tuning
- debug trace and rejection taxonomy

### Out
- new agents
- new integrations (email/calendar)
- DB persistence and UI work

## 3. Architecture (unchanged)
Planner -> Analysts (parallel) -> Validator -> Personalization/Ranking -> Output

## 4. Implementation Priorities

### Priority 1: Source Policy System (critical)
Use `config/source_policies.json` with canonical schema:
- `tier1_trusted[]`
- `tier2_allowed[]`
- `block[]`
- domain constraints (`freshness_days`, `weekend_only`, `location_terms`)

Rules:
- Tier1 preferred
- Tier2 fallback
- Block always reject
- Backward compatibility: `allow[]` can still be read as fallback tier

### Priority 2: Query Hardening
Replace generic queries with structured templates and `site:` constraints.

News:
- Netherlands last 24h (tier1 sites)
- Limburg/Maastricht/Borgharen last 3 days (tier1+tier2)

Events:
- this weekend + exact date/location expectation + trusted sites

Bitcoin mandatory split:
- market
- technical (BIP/GitHub/protocol)
- community (meetup/event only)

### Priority 3: Validator Hardening
Reject noisy/irrelevant pages using deterministic checks.

News rules:
- recent (<=3 days)
- real article page (no landing/listing)
- factual content

Events rules:
- specific title/date/location
- upcoming (weekend or next 7 days)
- reject generic listings/monthly aggregations

Bitcoin rules:
- market: price/macro/ETF/regulation signal
- technical: BIP/GitHub/protocol signal
- community: real meetup/event signal
- reject altcoin-heavy content

Date strategy:
- multi-step inference (published field, content date, relative cues)
- accept inferred dates with score/trust penalty
- reject when insufficient date confidence for domain constraints

Rejection taxonomy (closed set):
- `low_trust_source`
- `not_recent`
- `not_local`
- `generic_listing`
- `missing_specific_date`
- `not_article_page`
- `altcoin_noise`
- `duplicate_in_run`

### Priority 4: Ranking/Personalization
Maintain deterministic weights:
- recency 30%
- trust 25%
- geo relevance 20%
- category relevance 15%
- novelty 10%

Enhancements:
- trust by tier (`tier1 > tier2 > unknown`)
- penalties for aggregators and low date confidence
- preserve local/family-friendly boosts and dedupe

### Priority 5: Runtime Robustness
Reasoning model policy:
- preflight check provider/dependency/key
- reasoning only in validator
- fallback to quality model with explicit trace flag

Graph quality:
- ensure analyst memo `sections` are aggregated in main graph state

### Priority 6: Acceptance Soft Gate
Soft targets:
- >=5 news
- >=3 events
- >=3 bitcoin

Behavior:
- no hard fail on target miss
- emit `quality_gate_status` in trace and debug outputs (`pass`/`warn`)

## 5. DOE Alignment
- Directive: source policies and quality rules
- Orchestration: LangGraph flow and routing
- Execution: retrieval, filtering, validation, ranking

## 6. Acceptance Criteria
Successful if:
- output quality improves without empty digest in normal runs
- no static/reference/PDF-like pages in final sections
- events are specific, local, and upcoming
- bitcoin section includes real updates (not generic crypto noise)
- rejection reasons are standardized and tracked
- quality gate and model fallback status are visible in debug trace

## 7. Key Principle
Do not add complexity.
Prioritize:
1. data quality
2. validation
3. ranking

Not:
- more agents
- more tools
- more integrations
