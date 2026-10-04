#!/usr/bin/env python3
"""Check repository metadata and configuration without starting any services."""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from urllib.parse import unquote

import yaml
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

ROOT = Path(__file__).resolve().parents[1]


def requirements(lines: list[str]) -> set[str]:
    return {str(Requirement(line.split("#", 1)[0].strip())) for line in lines
            if line.split("#", 1)[0].strip() and not line.startswith("-r ")}


def main() -> int:
    errors: list[str] = []
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    if requirements(project["dependencies"]) != requirements((ROOT / "requirements.txt").read_text().splitlines()):
        errors.append("requirements.txt must match pyproject.toml project.dependencies")
    dev_lines = (ROOT / "requirements-dev.txt").read_text().splitlines()
    if "-r requirements.txt" not in dev_lines or requirements(dev_lines) != requirements(project["optional-dependencies"]["dev"]):
        errors.append("requirements-dev.txt must include requirements.txt and match the dev extra")
    if (ROOT / ".python-version").read_text().strip() not in SpecifierSet(project["requires-python"]):
        errors.append(".python-version must satisfy project.requires-python")
    tree = ast.parse((ROOT / "src/idp/__init__.py").read_text())
    version = next((n.value.value for n in tree.body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets)
                    and isinstance(n.value, ast.Constant)), None)
    if version != project["version"]:
        errors.append("idp.__version__ must match project.version")

    # Git's inventory includes new files while excluding .venv and local data.
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT,
    ).decode().split("\0")
    checked = 0
    for name in sorted(set(names) - {""}):
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            if path.suffix in {".json", ".avsc"}:
                json.loads(path.read_text())
            elif path.suffix in {".yaml", ".yml"}:
                # Syntax only: don't interpret GitHub's `on` key as YAML 1.1 True.
                list(yaml.load_all(path.read_text(), Loader=yaml.BaseLoader))
            elif path.suffix == ".sh":
                subprocess.run(["bash", "-n", str(path)], check=True, capture_output=True)
            else:
                continue
            checked += 1
        except (ValueError, yaml.YAMLError, subprocess.CalledProcessError) as exc:
            errors.append(f"{name}: {exc}")

    # Check the onboarding and operator-memory links, including phase archives.
    docs = [ROOT / name for name in ("AGENTS.md", "RELEASE_NOTES.md", "docs/README.md",
                                    "docs/project.md", "docs/roadmap.md", "docs/triggering.md")]
    docs.extend(sorted((ROOT / "internal").glob("*.md")))
    for path in docs:
        if not path.is_file():
            errors.append(f"missing onboarding file: {path.relative_to(ROOT)}")
            continue
        for target in re.findall(r"\[[^\]]+\]\(([^\s)]+)\)", path.read_text()):
            if target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            dest = unquote(target.split("#", 1)[0])
            if dest and not (path.parent / dest).exists():
                errors.append(f"{path.relative_to(ROOT)}: broken local link {target}")
    if errors:
        print("Workspace checks failed:\n" + "\n".join(f"- {e}" for e in errors), file=sys.stderr)
        return 1
    print(f"Workspace checks passed: metadata, {checked} config/shell files, {len(docs)} onboarding documents.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
