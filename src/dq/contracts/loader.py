"""Parse + validate data contract YAML files."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONTRACTS_DIR = Path("contracts")
SUPPORTED_VERSION = 1
VALID_SEVERITIES = {"critical", "warning"}


@dataclass
class HardRule:
    reason: str
    condition: str


@dataclass
class Expectation:
    id: str
    severity: str
    rationale: str
    type: str
    kwargs: dict[str, Any]


@dataclass
class Contract:
    version: int
    metadata: dict[str, Any]
    schema: list[dict[str, Any]]
    hard_rules: list[HardRule] = field(default_factory=list)
    expectations: list[Expectation] = field(default_factory=list)

    @property
    def table(self) -> str:
        return self.metadata["table"]

    @property
    def catalog_schema(self) -> str:
        return self.metadata["catalog_schema"]

    @property
    def natural_key(self) -> list[str]:
        return self.metadata.get("natural_key", [])


def load_contract(
    layer: str,
    table: str,
    contracts_dir: Path | str = DEFAULT_CONTRACTS_DIR,
) -> Contract:
    """Load `contracts/{layer}/{table}.yml` into a Contract."""
    path = Path(contracts_dir) / layer / f"{table}.yml"
    if not path.exists():
        raise FileNotFoundError(f"contract not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _parse(data, path)


def list_contracts(
    layer: str,
    contracts_dir: Path | str = DEFAULT_CONTRACTS_DIR,
) -> list[Contract]:
    """Load every `*.yml` under `contracts/{layer}/`."""
    layer_dir = Path(contracts_dir) / layer
    if not layer_dir.exists():
        return []
    return [load_contract(layer, p.stem, contracts_dir) for p in sorted(layer_dir.glob("*.yml"))]


def _parse(data: dict, path: Path) -> Contract:
    version = data.get("version")
    if version != SUPPORTED_VERSION:
        raise ValueError(f"{path}: unsupported contract version {version!r}")

    metadata = data.get("metadata") or {}
    for required in ("catalog_schema", "table", "owner"):
        if required not in metadata:
            raise ValueError(f"{path}: metadata.{required} is required")

    schema = data.get("schema") or []
    if not schema:
        raise ValueError(f"{path}: schema must declare at least one column")

    quality = data.get("quality") or {}
    hard_rules = [HardRule(**hr) for hr in quality.get("hard_rules") or []]
    expectations = [_parse_expectation(e, path) for e in quality.get("expectations") or []]

    return Contract(
        version=version,
        metadata=metadata,
        schema=schema,
        hard_rules=hard_rules,
        expectations=expectations,
    )


def _parse_expectation(e: dict, path: Path) -> Expectation:
    for required in ("id", "severity", "rationale", "type", "kwargs"):
        if required not in e:
            raise ValueError(f"{path}: expectation missing required field {required!r}: {e}")
    if e["severity"] not in VALID_SEVERITIES:
        raise ValueError(
            f"{path}: expectation {e['id']!r} has invalid severity {e['severity']!r} "
            f"(must be one of {VALID_SEVERITIES})"
        )
    return Expectation(
        id=e["id"],
        severity=e["severity"],
        rationale=e["rationale"],
        type=e["type"],
        kwargs=dict(e["kwargs"]),
    )
