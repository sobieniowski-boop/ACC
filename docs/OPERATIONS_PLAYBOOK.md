# ACC Operations Playbook
## Amazon Command Center — Operational Procedures & Standards

| Field | Value |
|-------|-------|
| **Document** | OPERATIONS_PLAYBOOK.md |
| **Version** | 1.0 |
| **Created** | 2026-03-13 |
| **Owner** | Miłosz Sobieniowski (msobieniowski) |
| **Scope** | Git workflow, communication, documentation, collaboration, sprint ops |
| **Horizon** | Phase 1 (Apr 2026) → Phase 4 (Mar 2027+) |
| **Team size** | Solo → 2–3 people (Phase 4+) |

---

## Table of Contents

1. [Git Workflow](#1-git-workflow)
2. [Communication Channels](#2-communication-channels)
3. [Documentation Templates](#3-documentation-templates)
4. [Collaboration Tools](#4-collaboration-tools)
5. [Sprint Operations Cadence](#5-sprint-operations-cadence)
6. [Setup Automation Script](#6-setup-automation-script)

---

# 1. Git Workflow

## 1.1 Branch Strategy

**Model: GitHub Flow + Release Tags** (not GitFlow — too heavy for a solo/small team).

Current CI/CD (`ci-cd.yml`) already triggers on `main` and `develop`. Adopt `develop` as the integration branch immediately so CI validates code before it hits `main`.

```
main        ─────●─────────────────●───────────── (production-ready, tagged releases)
                  ↑                 ↑
develop     ──●──●──●──●──●──●──●──●──●──●────── (integration branch, CI runs here)
               ↑     ↑     ↑
feature/    ──●──●  ●──●  ●──●──●                (short-lived, PR into develop)
hotfix/     ──────────────────●──●─────────────── (branch from main, PR into main + develop)
```

### Why this model

| Concern | Decision | Rationale |
|---------|----------|-----------|
| Solo simplicity | 2-branch core (`main` + `develop`) | No release branches needed until Phase 4 |
| CI/CD compatibility | `ci-cd.yml` already watches `main` + `develop` | Zero config change |
| Deploy trigger | Push to `main` triggers staging + production deploy | Existing pipeline behavior |
| Scale-ready | Feature branches + PRs create audit trail for contractor (T-404) | Easy to enforce review when team grows |

### Transition plan

1. **Now (pre-Phase 1)**: Create `develop` from `main`. Start using feature branches.
2. **Phase 1–3 (solo)**: Self-review PRs. Merge `develop → main` for releases.
3. **Phase 4 (team)**: Require 1 approval on PRs to `develop`. Branch protection enforced.

## 1.2 Branch Naming Conventions

```
<type>/<ticket>-<short-description>

Types:
  feature/   — New functionality
  fix/       — Bug fix
  hotfix/    — Production emergency (branches from main)
  refactor/  — Code restructuring, no behavior change
  docs/      — Documentation only
  chore/     — CI, config, dependency updates
  perf/      — Performance improvement
```

### Examples

```
feature/T-106-sql-pagination-backend
feature/T-201-multi-tenant-schema
fix/T-102-fx-rate-silent-failure
hotfix/stripe-webhook-500
refactor/T-112-archive-empty-tables
docs/T-116-core-runbooks
chore/dependabot-fastapi-bump
perf/T-105-db-indexes-ppt
```

### Rules

- Lowercase, hyphens only (no underscores, no spaces)
- Include task ID when one exists
- Max 50 characters after the type prefix
- Delete branches after merge (GitHub auto-delete setting)

## 1.3 PR Review Process

### Phase 1–3 (Solo Developer)

1. Create PR from feature branch → `develop`
2. CI runs automatically (lint, test, security scan)
3. Self-review the diff — use the **PR checklist** (see §3.1)
4. Squash-merge into `develop`
5. When ready to release: create PR `develop → main`, merge (merge commit)
6. Tag the release on `main`

### Phase 4+ (With Contractor — T-404)

1. All PRs require **1 approval** before merge
2. Contractor PRs: Miłosz reviews
3. Miłosz PRs: Contractor reviews (or self-merge after 24h if no review)
4. PRs touching `profit_engine.py`, `db_connection.py`, or `config.py` require Miłosz approval (CODEOWNERS)

### CODEOWNERS file

Create `.github/CODEOWNERS`:

```
# Default owner
* @msobieniowski

# Critical paths — always require owner review
apps/api/app/services/profit_engine.py    @msobieniowski
apps/api/app/core/db_connection.py        @msobieniowski
apps/api/app/core/config.py               @msobieniowski
apps/api/app/connectors/                  @msobieniowski
infrastructure/                           @msobieniowski
.github/workflows/                        @msobieniowski
```

## 1.4 Merge Policies

| Target Branch | Merge Method | Rationale |
|---------------|-------------|-----------|
| `develop` ← feature | **Squash merge** | Clean history, one commit per feature |
| `main` ← `develop` | **Merge commit** | Preserves the full develop history for auditing |
| `main` ← hotfix | **Merge commit** | Then cherry-pick/merge hotfix into `develop` |

### Branch Protection Rules

Apply to `main` immediately. Apply to `develop` in Phase 4 when team grows.

**`main` branch protection (apply now):**

```bash
# Requires gh CLI authenticated to sobieniowski-boop/ACC
gh api repos/sobieniowski-boop/ACC/branches/main/protection -X PUT \
  -H "Accept: application/vnd.github+json" \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["Backend (lint + test)", "Test (Frontend)"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
EOF
```

> **Phase 1–3**: No required reviewers (solo). CI must pass. No force pushes.
> **Phase 4**: Add `"required_pull_request_reviews": {"required_approving_review_count": 1}`.

**`develop` branch protection (Phase 4):**

```bash
gh api repos/sobieniowski-boop/ACC/branches/develop/protection -X PUT \
  -H "Accept: application/vnd.github+json" \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["Backend (lint + test)", "Test (Frontend)"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
EOF
```

## 1.5 Release Tagging Strategy

Tags follow **semver** and align with the 4-phase plan.

### Version scheme

```
v<major>.<minor>.<patch>[-<prerelease>]

Major = Phase number mapping:
  0.x.x  = Phase 1 (HARDEN) — pre-release, internal only
  1.0.0  = Phase 2 Beta launch (T-215)
  1.x.x  = Phase 2 iterations
  2.0.0  = Phase 3 public launch
  2.x.x  = Phase 3 iterations
  3.0.0  = Phase 4 Scale GA
```

### Tag timeline

| Tag | Sprint | Date | Milestone |
|-----|--------|------|-----------|
| `v0.1.0` | S1.1 end | Apr 14, 2026 | Monitoring + indexes live |
| `v0.2.0` | S1.2 end | Apr 28, 2026 | Observability + FBA bridge |
| `v0.3.0` | S1.3 end | May 12, 2026 | PPT pagination + tests ≥85% |
| `v0.4.0` | S1.4 / T-117 | May 15, 2026 | **Phase 1 Gate PASS** |
| `v1.0.0-beta.1` | S2.5 / T-215 | Jul 17, 2026 | **Private Beta Launch** |
| `v1.1.0-beta.2` | S2.7 end | Aug 21, 2026 | Logistics v3 + analytics |
| `v1.2.0-beta.3` | S2.9 end | Sep 18, 2026 | UX polish + error standards |
| `v2.0.0-rc.1` | S3.1 end | Oct 14, 2026 | Security hardened |
| `v2.0.0` | S3.4 end | Nov 25, 2026 | **Public Launch** |
| `v2.1.0` | S3.8 / T-314 | Jan 31, 2027 | **Phase 3 Gate PASS** |
| `v3.0.0` | S4.2+ | Feb 28, 2027 | **DACH + Scale GA** |

### Tagging commands

```bash
# After merging develop → main for a release:
git tag -a v0.1.0 -m "Phase 1 Sprint S1.1: Monitoring, indexes, sidebar, JWT"
git push origin v0.1.0

# For beta releases:
git tag -a v1.0.0-beta.1 -m "Phase 2 S2.5: Private Beta Launch (T-215)"
git push origin v1.0.0-beta.1
```

## 1.6 Hotfix Process

```
1. Branch from main:     git checkout -b hotfix/description main
2. Fix the issue:        (minimal, scoped change)
3. Test locally:         pytest + manual verification
4. PR → main:           gh pr create --base main --title "HOTFIX: description"
5. CI passes → merge:   Squash merge to main
6. Tag patch version:   git tag -a v0.1.1 -m "Hotfix: description"
7. Backport to develop: git checkout develop && git merge main
                        (or cherry-pick if develop has diverged)
```

**Hotfix criteria**: Production is broken or data integrity is at risk. Everything else goes through the normal `feature → develop → main` flow.

---

# 2. Communication Channels

## 2.1 Recommended Tools

| Tool | Purpose | Phase | Cost |
|------|---------|-------|------|
| **GitHub Issues** | Task tracking, bug reports, feature requests | All | Free |
| **GitHub Projects** | Sprint board, backlog management | All | Free |
| **GitHub Discussions** | Beta user feedback, Q&A, announcements | Phase 2+ | Free |
| **Discord** (1 server) | Real-time chat with beta users, quick decisions | Phase 2+ | Free |
| **Email** (Resend — T-214) | Transactional: onboarding, alerts, reports | Phase 2+ | ~$20/mo |
| **Loom** | Async video updates for beta users, bug demos | Phase 2+ | Free (25 min) |

### What NOT to use yet

- **Slack**: Overkill for 1–3 people. Discord covers casual + community.
- **Jira**: GitHub Projects + Issues is sufficient until 5+ people.
- **Notion**: Docs live in the repo (Markdown). Single source of truth.
- **Linear**: Evaluate only if GitHub Projects becomes limiting (Phase 4+).

## 2.2 Notification Routing

| Event | Channel | Who | Urgency |
|-------|---------|-----|---------|
| CI/CD failure on `main` | GitHub email + mobile push | Miłosz | 🔴 Immediate |
| UptimeRobot alert (T-101) | SMS + email | Miłosz | 🔴 Immediate |
| Data Observability alarm (T-110) | GitHub Issue (auto-created) + email | Miłosz | 🟡 1h |
| Dependabot PR | GitHub email | Miłosz | 🟢 Weekly batch |
| Security scan finding (Bandit/pip-audit) | CI artifact + email | Miłosz | 🟡 24h |
| Beta user feedback | GitHub Discussions + Discord #feedback | Miłosz | 🟢 48h |
| Stripe webhook event | Application log + email (T-214) | Miłosz | 🟡 1h |
| PR ready for review (Phase 4) | GitHub notification | Reviewer | 🟡 24h SLA |
| Sprint complete | Self-review (retro doc) | Miłosz | 🟢 End of sprint |

### GitHub notification settings (recommended)

```
Settings → Notifications:
  ✅ Email: Participating and @mentions
  ✅ Email: Security alerts
  ❌ Email: Watching (too noisy)
  ✅ Mobile: CI failures (via GitHub mobile app)
```

## 2.3 Status Update Cadence

### Solo Developer Rhythm (Phase 1–3)

| Cadence | Activity | Output | Time |
|---------|----------|--------|------|
| **Daily** | 5-min self-standup (mental checklist) | None (internal) | 9:00 AM |
| **Weekly** (Friday) | Week review + plan next week | Update sprint board, close issues | 30 min |
| **Bi-weekly** (end of sprint) | Sprint retrospective | `docs/sprints/RETRO_S{x.y}.md` | 45 min |
| **Monthly** | Progress review vs plan | Update `PRIORITIZED_SPRINT_PLAN` status | 1h |
| **Phase gate** | Formal gate review | Gate review document (T-117, T-314) | 2h |

### With Team (Phase 4+)

| Cadence | Activity | Format | Time |
|---------|----------|--------|------|
| **Daily** | Async standup | Discord #standup (text: done/doing/blocked) | 5 min |
| **Weekly** (Monday) | Sprint sync | 30-min video call (Discord or Google Meet) | 30 min |
| **Bi-weekly** | Sprint planning + retro | Video call + retro doc | 1.5h |
| **Monthly** | Demo + review | Loom recording shared to stakeholders | 1h |

## 2.4 Stakeholder Communication Plan

### Beta Users (Phase 2, starting S2.5)

| Channel | Content | Frequency |
|---------|---------|-----------|
| Email (Resend) | Release notes, feature announcements | Per release |
| GitHub Discussions #announcements | What's new, roadmap updates | Bi-weekly |
| Discord #general | Casual updates, sneak peeks | As needed |
| NPS survey (T-217) | Satisfaction measurement | Monthly |
| In-app banner | Critical changes, maintenance windows | As needed |

### Investors / Advisors (if applicable)

| Channel | Content | Frequency |
|---------|---------|-----------|
| Email | Monthly progress report (MRR, users, milestones) | Monthly |
| Loom | Demo of key features | Quarterly |

### Template: Monthly Progress Report (email)

```
Subject: ACC Monthly Update — [Month Year]

Key Metrics:
- MRR: $X (+Y% MoM)
- Active Users: N
- Uptime: 99.X%
- Sprint velocity: X SP/sprint

Completed This Month:
- [Feature 1]
- [Feature 2]

Next Month:
- [Planned work]

Blockers / Asks:
- [None / specific request]
```

---

# 3. Documentation Templates

## 3.1 PR Template

**File: `.github/PULL_REQUEST_TEMPLATE.md`**

```markdown
## Summary
<!-- What does this PR do? Link the task: Closes #XX or refs T-XXX -->


## Type
- [ ] Feature
- [ ] Bug fix
- [ ] Refactor
- [ ] Docs
- [ ] Chore (CI, deps, config)
- [ ] Performance

## Changes
<!-- Bullet list of what changed -->
-

## Testing
<!-- How was this tested? -->
- [ ] Unit tests added/updated
- [ ] Manual testing done
- [ ] No tests needed (docs/config only)

## Checklist
- [ ] Code follows project conventions (raw pyodbc SQL, `WITH (NOLOCK)`, etc.)
- [ ] No hardcoded credentials or secrets
- [ ] SQL writes use `SET LOCK_TIMEOUT 30000`
- [ ] No UPDATE/DELETE without WHERE clause
- [ ] CI passes (lint + tests)
- [ ] Self-reviewed the diff

## Screenshots / API Response
<!-- If UI change or new endpoint, paste screenshot or curl output -->

```

## 3.2 Issue Templates

### Bug Report

**File: `.github/ISSUE_TEMPLATE/bug_report.md`**

```markdown
---
name: Bug Report
about: Report a bug in ACC
title: "[BUG] "
labels: bug, triage
assignees: msobieniowski
---

## Description
<!-- Clear description of the bug -->

## Steps to Reproduce
1.
2.
3.

## Expected Behavior
<!-- What should happen -->

## Actual Behavior
<!-- What actually happens -->

## Environment
- **Component**: Backend / Frontend / Both
- **Browser** (if frontend):
- **Endpoint** (if API):
- **Date/time of occurrence**:

## Evidence
<!-- Paste error message, screenshot, API response, or log snippet -->
```text

```

## Severity
- [ ] P0 — Production down / data corruption
- [ ] P1 — Major feature broken, workaround exists
- [ ] P2 — Minor issue, cosmetic, or edge case
- [ ] P3 — Nice to fix, low impact

```

### Feature Request

**File: `.github/ISSUE_TEMPLATE/feature_request.md`**

```markdown
---
name: Feature Request
about: Suggest a new feature or enhancement
title: "[FEATURE] "
labels: enhancement, triage
assignees: msobieniowski
---

## Problem
<!-- What problem does this solve? -->

## Proposed Solution
<!-- How should it work? -->

## Alternatives Considered
<!-- Other approaches you considered -->

## Acceptance Criteria
- [ ]
- [ ]

## Task Reference
<!-- Link to sprint plan task if applicable: T-XXX, Sprint S{x.y} -->

## Priority Suggestion
- [ ] Must Have — Blocks a phase gate
- [ ] Should Have — High value, strategic
- [ ] Could Have — Nice enhancement
- [ ] Won't Have — Defer to future cycle

```

### Task

**File: `.github/ISSUE_TEMPLATE/task.md`**

```markdown
---
name: Task
about: Sprint task from the backlog
title: "[T-XXX] "
labels: task
assignees: msobieniowski
---

## Task
**ID**: T-XXX
**Sprint**: S{x.y}
**Story Points**: X
**Priority**: P{0-3}
**MoSCoW**: Must / Should / Could

## Description
<!-- What needs to be done -->

## Acceptance Criteria
- [ ]
- [ ]

## Dependencies
<!-- Tasks that must be complete before this one -->
- [ ] T-XXX ✅

## Definition of Done
- [ ] Code complete and tested
- [ ] PR merged to develop
- [ ] No regressions in CI
- [ ] Documentation updated (if applicable)

## Notes
<!-- Technical approach, references, ADRs -->

```

### Config: Issue Template Chooser

**File: `.github/ISSUE_TEMPLATE/config.yml`**

```yaml
blank_issues_enabled: true
contact_links:
  - name: Documentation
    url: https://github.com/sobieniowski-boop/ACC/tree/main/docs
    about: Check existing documentation before filing an issue
```

## 3.3 ADR Template

**File: `docs/decisions/TEMPLATE.md`**

```markdown
# ADR-NNN: [Title]

**Status**: Proposed | Accepted | Deprecated | Superseded by ADR-XXX
**Date**: YYYY-MM-DD
**Author**: [Name / Agent]
**Task**: T-XXX (if applicable)
**Sprint**: S{x.y} (if applicable)

## Context

<!-- What is the issue that we're seeing that is motivating this decision? -->

## Decision

<!-- What is the change that we're proposing and/or doing? -->

## Alternatives Considered

<!-- What other options were evaluated? Why were they rejected? -->

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| A | | | |
| B | | | Chosen |

## Consequences

<!-- What becomes easier or more difficult to do because of this change? -->

### Positive
-

### Negative
-

### Risks
-

## Review / Follow-up

<!-- When should this decision be revisited? -->
- **Review date**: YYYY-MM-DD
- **Success criteria**:
```

### ADR Numbering

- Sequential: `001`, `002`, `003`... (existing: `004-rollup-divergence.md`)
- File name: `NNN-short-kebab-title.md`
- Next ADR: `005`

## 3.4 Sprint Retrospective Template

**File: `docs/sprints/RETRO_TEMPLATE.md`**

```markdown
# Sprint Retrospective: S{x.y} — "[Sprint Theme]"

| Field | Value |
|-------|-------|
| **Sprint** | S{x.y} |
| **Dates** | [Start] – [End] |
| **Phase** | [1-4] ([HARDEN/BETA/LAUNCH/SCALE]) |
| **Planned SP** | X |
| **Completed SP** | Y |
| **Velocity** | Y SP (target: Z) |
| **Carry-over** | [Tasks spilled to next sprint, if any] |

## Completed Tasks

| Task | Title | SP | Notes |
|------|-------|----|-------|
| T-XXX | | | |

## Not Completed

| Task | Title | SP | Reason | Moved To |
|------|-------|----|--------|----------|
| T-XXX | | | [Blocked by / underestimated / deprioritized] | S{x.y+1} |

## What Went Well
-

## What Didn't Go Well
-

## What To Change Next Sprint
-

## Key Metrics
- **CI pass rate**: X%
- **Test coverage**: X% (backend) / X% (frontend)
- **Uptime**: 99.X%
- **Open bugs**: N (P0: _, P1: _, P2: _)

## Action Items
- [ ] [Action] — due [date]

---
*Retrospective completed: [date]*
```

## 3.5 Decision Log (Lightweight)

For decisions that don't warrant a full ADR. Kept in a single running file.

**File: `docs/decisions/DECISION_LOG.md`**

```markdown
# ACC Decision Log

Quick decisions that don't warrant a full ADR. For significant architectural choices, create an ADR (see `TEMPLATE.md`).

| # | Date | Decision | Context | Decided By | Refs |
|---|------|----------|---------|------------|------|
| 1 | 2026-03-13 | Adopt GitHub Flow + release tags | Solo dev, existing CI on main+develop | Miłosz | OPS_PLAYBOOK §1.1 |
| 2 | 2026-03-13 | Use Discord (not Slack) for community | Free, lightweight, suits beta community | Miłosz | OPS_PLAYBOOK §2.1 |

<!-- Add new rows at the bottom. Use sequential numbering. -->
```

---

# 4. Collaboration Tools

## 4.1 GitHub Project Board Setup

Create a project board with **Table** view (primary) and **Board** view (secondary).

### Board Columns

| Column | Automation | Description |
|--------|-----------|-------------|
| **📥 Backlog** | Default for new issues | All tasks not yet scheduled |
| **📋 Sprint Ready** | Manual | Pulled into current sprint during planning |
| **🔨 In Progress** | Auto: when branch created or PR opened | Actively being worked on |
| **👀 In Review** | Auto: when PR ready for review | PR open, awaiting review |
| **✅ Done** | Auto: when issue closed or PR merged | Completed |
| **🧊 Blocked** | Manual | Waiting on dependency or external factor |

### Setup commands

```bash
# Create the project (note: gh project commands use the --owner flag for org, omit for user)
gh project create --title "ACC Sprint Board" --owner @me

# After creation, note the PROJECT_NUMBER from the output.
# Then configure fields via the GitHub web UI:
#   1. Add "Sprint" field (Iteration type, 2-week cycles, starting Apr 1 2026)
#   2. Add "Phase" field (Single select: Phase 1, Phase 2, Phase 3, Phase 4)
#   3. Add "Story Points" field (Number)
#   4. Add "Priority" field (Single select: P0, P1, P2, P3)
#   5. Add "MoSCoW" field (Single select: Must, Should, Could, Won't)
```

## 4.2 Sprint Tracking Configuration

### Mapping sprints to the board

Each sprint is an **Iteration** in the GitHub Project. Configure iterations:

| Iteration | Sprint | Dates | Phase |
|-----------|--------|-------|-------|
| 1 | S1.1 | Apr 1–14, 2026 | Phase 1 |
| 2 | S1.2 | Apr 15–28, 2026 | Phase 1 |
| 3 | S1.3 | Apr 29 – May 12, 2026 | Phase 1 |
| 4 | S1.4 | May 5–15, 2026 | Phase 1 |
| 5 | S2.1 | May 16–29, 2026 | Phase 2 |
| ... | ... | ... | ... |
| 26 | S4.4 | Mar 15–28, 2027 | Phase 4 |

### Sprint workflow

1. **Sprint Planning** (day 1): Move issues from Backlog → Sprint Ready. Set Sprint iteration field.
2. **During sprint**: Issues move In Progress → In Review → Done automatically.
3. **Sprint Review** (last day): Verify all Done items. Move incomplete to next sprint.
4. **Sprint Retro**: Fill out `docs/sprints/RETRO_S{x.y}.md`.

### Velocity tracking

Use GitHub Project's **Insights** tab:
- Chart: "Sum of Story Points by Sprint iteration, grouped by Status"
- This gives a burn-up chart per sprint.

## 4.3 Label Taxonomy

### Setup all labels at once

```bash
REPO="sobieniowski-boop/ACC"

# Delete default labels that we don't need
gh label delete "good first issue" --repo $REPO --yes 2>/dev/null
gh label delete "help wanted" --repo $REPO --yes 2>/dev/null
gh label delete "invalid" --repo $REPO --yes 2>/dev/null
gh label delete "wontfix" --repo $REPO --yes 2>/dev/null
gh label delete "question" --repo $REPO --yes 2>/dev/null

# === Priority ===
gh label create "P0-critical"  --color "B60205" --description "Production down or data corruption" --repo $REPO
gh label create "P1-high"      --color "D93F0B" --description "Major feature broken, workaround exists" --repo $REPO
gh label create "P2-medium"    --color "FBCA04" --description "Minor issue, cosmetic, or edge case" --repo $REPO
gh label create "P3-low"       --color "0E8A16" --description "Nice to fix, low impact" --repo $REPO

# === Phase ===
gh label create "phase-1-harden" --color "5319E7" --description "Phase 1: HARDEN (Apr-May 2026)" --repo $REPO
gh label create "phase-2-beta"   --color "1D76DB" --description "Phase 2: BETA (May-Sep 2026)" --repo $REPO
gh label create "phase-3-launch" --color "0075CA" --description "Phase 3: LAUNCH (Oct 2026-Jan 2027)" --repo $REPO
gh label create "phase-4-scale"  --color "006B75" --description "Phase 4: SCALE (Feb-Mar 2027+)" --repo $REPO

# === MoSCoW ===
gh label create "must-have"   --color "B60205" --description "Required for phase gate" --repo $REPO
gh label create "should-have" --color "D93F0B" --description "High value, strategic" --repo $REPO
gh label create "could-have"  --color "FBCA04" --description "Deferrable enhancement" --repo $REPO
gh label create "wont-have"   --color "CCCCCC" --description "Explicitly excluded this cycle" --repo $REPO

# === Type ===
gh label create "bug"          --color "D73A4A" --description "Something isn't working" --repo $REPO
gh label create "enhancement"  --color "A2EEEF" --description "New feature or improvement" --repo $REPO
gh label create "task"         --color "0075CA" --description "Sprint backlog task" --repo $REPO
gh label create "tech-debt"    --color "BFD4F2" --description "Code quality / refactor" --repo $REPO
gh label create "security"     --color "EE0701" --description "Security-related" --repo $REPO
gh label create "performance"  --color "C5DEF5" --description "Performance improvement" --repo $REPO
gh label create "documentation" --color "0075CA" --description "Documentation" --repo $REPO
gh label create "infrastructure" --color "D4C5F9" --description "CI/CD, Terraform, Azure" --repo $REPO
gh label create "ux"           --color "BFDADC" --description "User experience / UI" --repo $REPO

# === Component ===
gh label create "backend"      --color "F9D0C4" --description "Python / FastAPI / API" --repo $REPO
gh label create "frontend"     --color "C2E0C6" --description "React / TypeScript / UI" --repo $REPO
gh label create "database"     --color "E4E669" --description "Azure SQL / schema / queries" --repo $REPO
gh label create "ads"          --color "FEF2C0" --description "Amazon Ads API" --repo $REPO
gh label create "profit-engine" --color "F9D0C4" --description "CM1/CM2 profit calculations" --repo $REPO

# === Workflow ===
gh label create "triage"       --color "EDEDED" --description "Needs triage" --repo $REPO
gh label create "blocked"      --color "B60205" --description "Blocked by dependency" --repo $REPO
gh label create "carry-over"   --color "FBCA04" --description "Spilled from previous sprint" --repo $REPO
gh label create "gate-review"  --color "5319E7" --description "Phase gate review task" --repo $REPO

# === Dependabot (already partially created) ===
gh label create "dependencies" --color "0366D6" --description "Dependency update" --repo $REPO 2>/dev/null
gh label create "ci"           --color "D4C5F9" --description "CI/CD pipeline" --repo $REPO 2>/dev/null
```

## 4.4 Milestone Setup

Milestones align with phase gates and key releases.

```bash
REPO="sobieniowski-boop/ACC"

gh api repos/$REPO/milestones -X POST -f title="Phase 1: HARDEN" \
  -f description="Stability, performance, observability, data trust. Gate: T-117." \
  -f due_on="2026-05-15T23:59:59Z"

gh api repos/$REPO/milestones -X POST -f title="v1.0.0-beta: Private Beta" \
  -f description="Multi-tenant, billing, onboarding, beta launch (T-215). Sprint S2.5." \
  -f due_on="2026-07-24T23:59:59Z"

gh api repos/$REPO/milestones -X POST -f title="Phase 2: BETA Complete" \
  -f description="UX polish, error handling, all Phase 2 tasks. Buffer sprint S2.10." \
  -f due_on="2026-09-30T23:59:59Z"

gh api repos/$REPO/milestones -X POST -f title="v2.0.0: Public Launch" \
  -f description="Security hardened, DACH tested, help center, E2E tests." \
  -f due_on="2026-11-25T23:59:59Z"

gh api repos/$REPO/milestones -X POST -f title="Phase 3: LAUNCH Complete" \
  -f description="All Phase 3 tasks done. Gate: T-314." \
  -f due_on="2027-01-31T23:59:59Z"

gh api repos/$REPO/milestones -X POST -f title="v3.0.0: Scale GA" \
  -f description="DACH launch, contractor onboarded, horizontal scaling." \
  -f due_on="2027-03-28T23:59:59Z"
```

## 4.5 Definition of Done

### Per Task Type

| Type | Definition of Done |
|------|-------------------|
| **Feature** | Code complete, unit tests, PR merged to develop, CI green, no P0/P1 regressions, docs updated if user-facing |
| **Bug fix** | Root cause identified, fix implemented, regression test added, PR merged, verified in staging |
| **Refactor** | No behavior change, existing tests pass, PR merged, CI green |
| **Documentation** | Content written, reviewed for accuracy, committed to repo |
| **Infrastructure** | Terraform applied (or IaC committed), verified in staging, rollback plan documented |
| **Security** | Vulnerability patched, security scan clean, no new findings introduced |

### Phase Gate Definition of Done

**Phase 1 Gate (T-117):**
- [ ] PPT loads <2s for 500 SKUs (T-106, T-107)
- [ ] Uptime ≥99% over 2 weeks (T-101)
- [ ] Zero silent data failures (T-102, T-103, T-104)
- [ ] Sidebar ≤20 visible pages (T-108)
- [ ] Data Quality Score ≥82 (T-110, T-111)
- [ ] Test suite ≥85% pass rate (T-115)
- [ ] All Must Have Phase 1 tasks closed

**Phase 3 Gate (T-314):**
- [ ] Security audit passed (T-311, T-312)
- [ ] ≥200 beta signups achieved (T-215 metric)
- [ ] NPS ≥30 from beta users (T-217)
- [ ] Help center with ≥20 articles (T-308)
- [ ] E2E test suite operational (T-313)
- [ ] All Must Have Phase 3 tasks closed

---

# 5. Sprint Operations Cadence

## 5.1 Sprint Ceremony Schedule

### Solo Developer (Phase 1–3)

All "ceremonies" are lightweight self-directed activities. No meetings with yourself.

| Day | Time | Activity | Duration | Output |
|-----|------|----------|----------|--------|
| **Sprint Day 1** (Mon) | 9:00 | Sprint Planning | 30 min | Sprint board populated, issues assigned to sprint |
| **Daily** | 9:00 | Self-standup | 5 min | Mental check: What did I do? What's next? Blocked? |
| **Friday** | 16:00 | Weekly review | 30 min | Board cleanup, velocity check, update project insights |
| **Sprint Day 10** (Fri) | 14:00 | Sprint Review | 30 min | Demo to self (Loom if sharing), merge develop → main, tag release |
| **Sprint Day 10** (Fri) | 14:30 | Sprint Retro | 30 min | Fill `docs/sprints/RETRO_S{x.y}.md` |

### Sprint Planning Checklist (Day 1)

```markdown
1. Review previous sprint retro action items
2. Check carry-over tasks from previous sprint
3. Pull tasks from Backlog → Sprint Ready (use RICE priority)
4. Verify total SP ≤ sprint velocity target
5. Check dependencies (are prerequisites Done?)
6. Create feature branches for top 2-3 tasks
7. Set Sprint iteration field on all sprint issues
```

### Weekly Review Checklist (Every Friday)

```markdown
1. Update issue statuses (In Progress / Done / Blocked)
2. Check CI status — any failing builds?
3. Review Dependabot PRs — merge or dismiss
4. Check UptimeRobot dashboard (T-101)
5. Review Data Observability alerts (T-110)
6. Update Story Points completed this week
7. Plan next week's focus (top 3 tasks)
```

## 5.2 Weekly Rhythm

### Phase 1 Template (22h deep engineering/week)

```
┌────────────────────────────────────────────────────────────┐
│ MONDAY                                                      │
│ 09:00  Sprint planning (sprint start) OR daily standup      │
│ 09:30  Deep work block 1 (3h)                               │
│ 12:30  Lunch                                                │
│ 13:30  Deep work block 2 (3h)                               │
│ 16:30  PR review / code review / CI fixes                   │
│ 17:30  End                                                  │
├────────────────────────────────────────────────────────────┤
│ TUESDAY                                                     │
│ 09:00  Daily standup (mental)                               │
│ 09:05  Deep work block 1 (3.5h)                             │
│ 12:30  Lunch                                                │
│ 13:30  Deep work block 2 (3h)                               │
│ 16:30  Ops: Dependabot, monitoring check, email             │
│ 17:30  End                                                  │
├────────────────────────────────────────────────────────────┤
│ WEDNESDAY                                                   │
│ 09:00  Daily standup (mental)                               │
│ 09:05  Deep work block 1 (3.5h)                             │
│ 12:30  Lunch                                                │
│ 13:30  Deep work block 2 (3h)                               │
│ 16:30  Documentation / ADRs / tech writing                  │
│ 17:30  End                                                  │
├────────────────────────────────────────────────────────────┤
│ THURSDAY                                                    │
│ 09:00  Daily standup (mental)                               │
│ 09:05  Deep work block 1 (3.5h)                             │
│ 12:30  Lunch                                                │
│ 13:30  Deep work block 2 (2.5h)                             │
│ 16:00  Testing / QA / manual verification                   │
│ 17:30  End                                                  │
├────────────────────────────────────────────────────────────┤
│ FRIDAY                                                      │
│ 09:00  Daily standup (mental)                               │
│ 09:05  Deep work block 1 (2h) — finish in-progress work     │
│ 11:00  Code review / PR cleanup                             │
│ 12:30  Lunch                                                │
│ 13:30  Sprint retro (if sprint-end) OR deep work (2h)       │
│ 16:00  Weekly review (30 min)                               │
│ 16:30  Plan next week's priorities                          │
│ 17:00  End (early Friday!)                                  │
└────────────────────────────────────────────────────────────┘

Weekly engineering time: ~22h (target met)
Weekly ops/admin time: ~4h
Weekly planning/retro: ~2h
Total productive hours: ~28h of 40h working week
Buffer for context switching, breaks, overhead: ~12h
```

## 5.3 Phase Gate Review Process

Phase gates are formal checkpoints that determine whether to proceed to the next phase.

### Gate Review Steps

1. **Prepare** (1 day before gate):
   - Fill out gate verification matrix (from sprint plan §5.3)
   - Run full test suite, record results
   - Check all "Must Have" tasks for the phase
   - Collect metrics (uptime, DQ score, test coverage, velocity)

2. **Review** (gate day):
   - Go through each gate criterion with pass/fail
   - Document any conditional passes (criteria partially met)
   - Identify risks for next phase
   - Make explicit GO / NO-GO / CONDITIONAL-GO decision

3. **Document** (same day):
   - Create `docs/sprints/GATE_PHASE_{N}.md`
   - Tag release (e.g., `v0.4.0` for Phase 1 gate)
   - Close milestone in GitHub

4. **Communicate** (next day):
   - Update sprint plan status
   - If beta users exist: send announcement
   - Archive phase-specific issues

### Phase 1 Gate (T-117) — May 15, 2026

**Gate Criteria Checklist:**

| # | Criterion | Metric | Source | Pass? |
|---|-----------|--------|--------|-------|
| H-1 | PPT load time | <2s for 500 SKUs | Browser DevTools + API response time | |
| H-2 | Ads data freshness | <6h staleness | Data Observability dashboard | |
| H-3 | FBA fee bridge | >95% order lines matched | SQL audit query | |
| H-4 | Sidebar page count | ≤20 visible | Manual count | |
| H-5 | Uptime | ≥99% over 14 days | UptimeRobot report | |
| H-6 | Data Quality Score | ≥82 | T-110 DQ endpoint | |
| H-7 | Test pass rate | ≥85% | CI pytest output | |
| H-8 | Silent failures | 0 unhandled | T-102 FX alerts + T-103 heartbeat | |

### Phase 3 Gate (T-314) — Jan 31, 2027

| # | Criterion | Metric | Source | Pass? |
|---|-----------|--------|--------|-------|
| L-1 | Security audit | No P0/P1 findings | Bandit + manual review | |
| L-2 | GDPR compliance | PII audit passed | T-312 report | |
| L-3 | Beta signups | ≥200 | Stripe/DB count | |
| L-4 | NPS score | ≥30 | T-217 survey data | |
| L-5 | Help center | ≥20 articles | Doc site article count | |
| L-6 | E2E tests | Suite operational, >80% pass | Playwright CI | |
| L-7 | DACH readiness | DE marketplace tested | T-304 report | |

## 5.4 Sprint-to-Phase Quick Reference

```
PHASE 1: HARDEN (Apr 1 – May 15, 2026) — 34 SP, zero margin
├── S1.1 (Apr 1–14)   10 SP  Monitoring, indexes, sidebar, JWT
├── S1.2 (Apr 15–28)  10 SP  Observability, ads guard, FBA, snapshot
├── S1.3 (Apr 29–May 12) 12 SP  PPT perf, tests, cleanup
└── S1.4 (May 5–15)    5 SP  Freshness, runbooks, GATE (T-117)

PHASE 2: BETA (May 16 – Sep 30, 2026) — 63 SP, +7 SP buffer
├── S2.1 (May 16–29)   7 SP  Multi-tenant, email
├── S2.2 (May 30–Jun 12) 9 SP  RBAC, rate limit, landing, API ver
├── S2.3 (Jun 13–26)   8 SP  User registration, sidebar
├── S2.4 (Jun 27–Jul 10) 8 SP  Stripe billing, onboarding wizard
├── S2.5 (Jul 11–24)   8 SP  ★ BETA LAUNCH (T-215), pool, Alembic
├── S2.6 (Jul 25–Aug 7) 7 SP  Morning brief, logistics v3 (1/2)
├── S2.7 (Aug 8–21)    7 SP  Logistics v3 (2/2), PostHog, NPS
├── S2.8 (Aug 22–Sep 4) 7 SP  Breadcrumbs, search, module toggle
├── S2.9 (Sep 5–18)    7 SP  RFC 7807, alert triage
└── S2.10 (Sep 19–30)  — SP  Buffer / Phase 2 gate

PHASE 3: LAUNCH (Oct 1, 2026 – Jan 31, 2027) — 53 SP, +3 SP buffer
├── S3.1 (Oct 1–14)    8 SP  Security, GDPR, docs (1/2)
├── S3.2 (Oct 15–28)   8 SP  DACH, marketing site, docs (2/2)
├── S3.3 (Oct 29–Nov 11) 10 SP  Celery workers, P&L report
├── S3.4 (Nov 12–25)   8 SP  Funnel optimization, E2E testing
├── S3.5 (Nov 26–Dec 9) 8 SP  German i18n, refund drill path
├── S3.6 (Dec 10–23)   6 SP  Referral, bank feed (1/2)
├── S3.7 (Jan 5–18)    4 SP  Bank feed (2/2)
└── S3.8 (Jan 19–31)   1 SP  GATE (T-314)

PHASE 4: SCALE (Feb 1 – Mar 28, 2027+) — 36 SP
├── S4.1 (Feb 1–14)    7 SP  Azure SQL upgrade, horizontal scaling
├── S4.2 (Feb 15–28)   6 SP  DACH launch, contractor onboard (T-404)
├── S4.3 (Mar 1–14)    8 SP  Export infra, JWT RS256
└── S4.4 (Mar 15–28)  15 SP  Mobile, time-series, AI alerts (aspirational)
```

---

# 6. Setup Automation Script

Run this script to bootstrap all operational infrastructure at once. Requires `gh` CLI authenticated.

**File: `scripts/setup-ops-infra.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO="sobieniowski-boop/ACC"
echo "=== ACC Operations Infrastructure Setup ==="
echo "Repository: $REPO"
echo ""

# ── 1. Create develop branch ─────────────────────────────────────────
echo ">>> Creating develop branch from main..."
git checkout main
git pull origin main
git checkout -b develop
git push -u origin develop
echo "✓ develop branch created"

# ── 2. Branch protection on main ────────────────────────────────────
echo ">>> Setting branch protection on main..."
gh api repos/$REPO/branches/main/protection -X PUT \
  -H "Accept: application/vnd.github+json" \
  --input - <<'PROTECTION'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["Backend (lint + test)", "Test (Frontend)"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
PROTECTION
echo "✓ main branch protection set"

# ── 3. Labels ────────────────────────────────────────────────────────
echo ">>> Creating labels..."

# Priority
gh label create "P0-critical"    --color "B60205" --description "Production down or data corruption" --repo $REPO 2>/dev/null || true
gh label create "P1-high"        --color "D93F0B" --description "Major feature broken, workaround exists" --repo $REPO 2>/dev/null || true
gh label create "P2-medium"      --color "FBCA04" --description "Minor issue, cosmetic, or edge case" --repo $REPO 2>/dev/null || true
gh label create "P3-low"         --color "0E8A16" --description "Nice to fix, low impact" --repo $REPO 2>/dev/null || true

# Phase
gh label create "phase-1-harden" --color "5319E7" --description "Phase 1: HARDEN (Apr-May 2026)" --repo $REPO 2>/dev/null || true
gh label create "phase-2-beta"   --color "1D76DB" --description "Phase 2: BETA (May-Sep 2026)" --repo $REPO 2>/dev/null || true
gh label create "phase-3-launch" --color "0075CA" --description "Phase 3: LAUNCH (Oct 2026-Jan 2027)" --repo $REPO 2>/dev/null || true
gh label create "phase-4-scale"  --color "006B75" --description "Phase 4: SCALE (Feb-Mar 2027+)" --repo $REPO 2>/dev/null || true

# MoSCoW
gh label create "must-have"      --color "B60205" --description "Required for phase gate" --repo $REPO 2>/dev/null || true
gh label create "should-have"    --color "D93F0B" --description "High value, strategic" --repo $REPO 2>/dev/null || true
gh label create "could-have"     --color "FBCA04" --description "Deferrable enhancement" --repo $REPO 2>/dev/null || true
gh label create "wont-have"      --color "CCCCCC" --description "Explicitly excluded this cycle" --repo $REPO 2>/dev/null || true

# Type
gh label create "bug"            --color "D73A4A" --description "Something isn't working" --repo $REPO 2>/dev/null || true
gh label create "enhancement"    --color "A2EEEF" --description "New feature or improvement" --repo $REPO 2>/dev/null || true
gh label create "task"           --color "0075CA" --description "Sprint backlog task" --repo $REPO 2>/dev/null || true
gh label create "tech-debt"      --color "BFD4F2" --description "Code quality / refactor" --repo $REPO 2>/dev/null || true
gh label create "security"       --color "EE0701" --description "Security-related" --repo $REPO 2>/dev/null || true
gh label create "performance"    --color "C5DEF5" --description "Performance improvement" --repo $REPO 2>/dev/null || true
gh label create "documentation"  --color "0075CA" --description "Documentation" --repo $REPO 2>/dev/null || true
gh label create "infrastructure" --color "D4C5F9" --description "CI/CD, Terraform, Azure" --repo $REPO 2>/dev/null || true
gh label create "ux"             --color "BFDADC" --description "User experience / UI" --repo $REPO 2>/dev/null || true

# Component
gh label create "backend"        --color "F9D0C4" --description "Python / FastAPI / API" --repo $REPO 2>/dev/null || true
gh label create "frontend"       --color "C2E0C6" --description "React / TypeScript / UI" --repo $REPO 2>/dev/null || true
gh label create "database"       --color "E4E669" --description "Azure SQL / schema / queries" --repo $REPO 2>/dev/null || true
gh label create "ads"            --color "FEF2C0" --description "Amazon Ads API" --repo $REPO 2>/dev/null || true
gh label create "profit-engine"  --color "F9D0C4" --description "CM1/CM2 profit calculations" --repo $REPO 2>/dev/null || true

# Workflow
gh label create "triage"         --color "EDEDED" --description "Needs triage" --repo $REPO 2>/dev/null || true
gh label create "blocked"        --color "B60205" --description "Blocked by dependency" --repo $REPO 2>/dev/null || true
gh label create "carry-over"     --color "FBCA04" --description "Spilled from previous sprint" --repo $REPO 2>/dev/null || true
gh label create "gate-review"    --color "5319E7" --description "Phase gate review task" --repo $REPO 2>/dev/null || true

echo "✓ Labels created"

# ── 4. Milestones ────────────────────────────────────────────────────
echo ">>> Creating milestones..."
gh api repos/$REPO/milestones -X POST -f title="Phase 1: HARDEN" \
  -f description="Stability, performance, observability, data trust. Gate: T-117." \
  -f due_on="2026-05-15T23:59:59Z" 2>/dev/null || true

gh api repos/$REPO/milestones -X POST -f title="v1.0.0-beta: Private Beta" \
  -f description="Multi-tenant, billing, onboarding, beta launch (T-215). Sprint S2.5." \
  -f due_on="2026-07-24T23:59:59Z" 2>/dev/null || true

gh api repos/$REPO/milestones -X POST -f title="Phase 2: BETA Complete" \
  -f description="UX polish, error handling, all Phase 2 tasks." \
  -f due_on="2026-09-30T23:59:59Z" 2>/dev/null || true

gh api repos/$REPO/milestones -X POST -f title="v2.0.0: Public Launch" \
  -f description="Security hardened, DACH tested, help center, E2E tests." \
  -f due_on="2026-11-25T23:59:59Z" 2>/dev/null || true

gh api repos/$REPO/milestones -X POST -f title="Phase 3: LAUNCH Complete" \
  -f description="All Phase 3 tasks done. Gate: T-314." \
  -f due_on="2027-01-31T23:59:59Z" 2>/dev/null || true

gh api repos/$REPO/milestones -X POST -f title="v3.0.0: Scale GA" \
  -f description="DACH launch, contractor onboarded, horizontal scaling." \
  -f due_on="2027-03-28T23:59:59Z" 2>/dev/null || true

echo "✓ Milestones created"

# ── 5. Enable auto-delete branches ──────────────────────────────────
echo ">>> Enabling auto-delete head branches..."
gh api repos/$REPO -X PATCH -f delete_branch_on_merge=true
echo "✓ Auto-delete enabled"

# ── 6. Enable GitHub Discussions ─────────────────────────────────────
echo ">>> Note: Enable GitHub Discussions manually via repo Settings > Features"
echo "   Categories to create: Announcements, Feedback, Q&A, Ideas"

echo ""
echo "=== Setup Complete ==="
echo "Next steps:"
echo "  1. Commit .github/ template files"
echo "  2. Set up GitHub Project board (web UI)"
echo "  3. Begin Sprint S1.1 on Apr 1, 2026"
```

---

# Appendix A: File Inventory

This playbook requires the following files to be created in the repository:

| File | Section | Purpose |
|------|---------|---------|
| `.github/PULL_REQUEST_TEMPLATE.md` | §3.1 | PR checklist |
| `.github/ISSUE_TEMPLATE/bug_report.md` | §3.2 | Bug report template |
| `.github/ISSUE_TEMPLATE/feature_request.md` | §3.2 | Feature request template |
| `.github/ISSUE_TEMPLATE/task.md` | §3.2 | Sprint task template |
| `.github/ISSUE_TEMPLATE/config.yml` | §3.2 | Issue chooser config |
| `.github/CODEOWNERS` | §1.3 | Code ownership rules |
| `docs/decisions/TEMPLATE.md` | §3.3 | ADR template |
| `docs/decisions/DECISION_LOG.md` | §3.5 | Lightweight decision log |
| `docs/sprints/RETRO_TEMPLATE.md` | §3.4 | Sprint retro template |
| `scripts/setup-ops-infra.sh` | §6 | One-time setup script |

# Appendix B: Operational Metrics Dashboard

Track these metrics weekly. No tooling needed — just a section in the Friday weekly review.

| Metric | Target | Source | Review Cadence |
|--------|--------|--------|----------------|
| Sprint velocity | Phase-dependent (see §5.4) | GitHub Project insights | Per sprint |
| Test pass rate | ≥85% | CI pytest output | Weekly |
| Uptime | ≥99% | UptimeRobot (T-101) | Weekly |
| Data Quality Score | ≥82 | T-110 endpoint | Weekly |
| Open P0/P1 bugs | 0 | GitHub Issues | Daily |
| Dependabot PR age | <7 days | GitHub | Weekly |
| CI build time | <10 min | GitHub Actions | Monthly |
| Code coverage (backend) | ≥70% | pytest-cov | Per sprint |

# Appendix C: Contractor Onboarding Checklist (T-404, Phase 4)

When the first contractor joins (triggered by $5K MRR sustained):

- [ ] Add to GitHub repo with `write` access
- [ ] Add to CODEOWNERS for their assigned areas
- [ ] Enable required PR reviews on `develop` (see §1.4)
- [ ] Share this Operations Playbook
- [ ] Walk through sprint board and current sprint
- [ ] Share Discord server invite
- [ ] Set up 1:1 weekly sync (30 min)
- [ ] Assign first task (scoped, well-defined, with clear acceptance criteria)
- [ ] Review their first PR together (pair review)

---

*Document maintained by Studio Operations. Last updated: 2026-03-13.*
*Next review: Phase 1 start (Apr 1, 2026) — validate all templates and processes are in place.*
