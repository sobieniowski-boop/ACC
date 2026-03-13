"""Job registry — tracks which domain registered which job for diagnostics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RegisteredJob:
    job_id: str
    domain: str
    func_name: str
    trigger_desc: str


@dataclass
class JobRegistry:
    """Central registry of all scheduled jobs for diagnostics/UI."""
    _jobs: dict[str, RegisteredJob] = field(default_factory=dict)

    def add(self, job_id: str, domain: str, func_name: str, trigger_desc: str) -> None:
        self._jobs[job_id] = RegisteredJob(
            job_id=job_id, domain=domain, func_name=func_name, trigger_desc=trigger_desc,
        )

    def list_all(self) -> list[dict[str, Any]]:
        return [
            {"job_id": j.job_id, "domain": j.domain, "func": j.func_name, "trigger": j.trigger_desc}
            for j in self._jobs.values()
        ]

    def by_domain(self, domain: str) -> list[RegisteredJob]:
        return [j for j in self._jobs.values() if j.domain == domain]
