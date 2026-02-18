#!/usr/bin/env -S uv run --script
"""
Amalgamation
(noun)
/əˌmælɡəˈmeɪʃən/

1. The process of combining multiple crates into a single one. Used to prevent
   namespace pollution when publishing to crates.io.
2. The result of amalgamating.
"""
# /// script
# dependencies = [ "tomlkit" ]
# ///

import functools
import os.path
import re
import shutil
import subprocess
from pathlib import Path

import tomlkit

ROOT = Path(__file__).parent.parent.parent.parent
"""Repo root, assuming this script lays in <root>/lib/edge/publish/."""

AMALGAMATION = Path(__file__).parent / "qdrant-edge"

PACKAGES_TO_INCLUDE = [
    "api",
    "common",
    "edge",
    "gridstore",
    "posting_list",
    "quantization",
    "segment",
    "shard",
    "sparse",
]


def main() -> None:
    root_manifest = tomlkit.loads(Path(ROOT / "Cargo.toml").read_text())
    packages = all_file_dependencies(ROOT / "lib/edge")

    shutil.rmtree(AMALGAMATION, ignore_errors=True)

    # Copy Rust sources.
    for pkg, (path, manifest) in packages.items():
        shutil.copytree(path / "src", AMALGAMATION / "src" / pkg)
        os.rename(
            AMALGAMATION / "src" / pkg / "lib.rs",
            AMALGAMATION / "src" / pkg / "mod.rs",
        )

    # Copy C sources and related build scripts.
    shutil.copytree(ROOT / "lib/quantization/cpp", AMALGAMATION / "cpp/quantization")
    shutil.copy2(
        ROOT / "lib/quantization/build.rs",
        AMALGAMATION / "build-quantization.rs",
    )
    (AMALGAMATION / "build.rs").write_text('include!("build-quantization.rs");\n')
    # TODO: segment/build.rs - for arm neon .c files.

    # Copy resources.
    shutil.copytree(ROOT / "lib/segment/tokenizer", AMALGAMATION / "tokenizer")

    # Write Cargo.toml.
    manifest = {
        "package": {
            "name": "qdrant-edge",
            "version": "0.0.0",
            "authors": ["Qdrant Team <info@qdrant.tech>"],
            "license": "Apache-2.0",
            "edition": "2024",
            "publish": False,
        },
        **gather_dependencies(
            root_manifest, [manifest for _, manifest in packages.values()]
        ),
    }
    (AMALGAMATION / "Cargo.toml").write_text(tomlkit.dumps(manifest))

    # Write src/lib.rs.
    # It just re-exports everything.
    (AMALGAMATION / "src/lib.rs").write_text(
        "#![allow(unexpected_cfgs, unused_imports)]\n"
        + "".join(f"pub mod {pkg};\n" for pkg in packages.keys())
        + "pub use edge::*;\n"
    )

    # Regex-based fixups.
    substitute(
        AMALGAMATION / "src/api/grpc/qdrant.rs",
        (r'custom\(function = "crate::(.*)"\)', r'custom(function = "crate::api::\1")'),
        (r'custom\(function = "(common::.*)"\)', r'custom(function = "crate::\1")'),
    )
    substitute(
        AMALGAMATION / "build-quantization.rs",
        (r"cpp/", r"cpp/quantization/"),
    )
    # Remove code that doesn't compile.
    substitute(
        AMALGAMATION / "src/api/grpc/mod.rs",
        (r"^pub mod dynamic_channel_pool;$\n", ""),
        (r"^pub mod dynamic_pool;$\n", ""),
        (r"^pub mod transport_channel_pool;$\n", ""),
        (r"^pub const QDRANT_DESCRIPTOR_SET:.*$\n", ""),
    )
    substitute(
        AMALGAMATION / "src/segment/common/anonymize.rs",
        (r"^pub use macros::Anonymize;$\n", ""),
    )

    # Remove unused code.
    shutil.rmtree(AMALGAMATION / "src/segment/index/hnsw_index/gpu")

    # Ast-grep-based fixups.
    RULES_TEMPLATE = (Path(__file__).parent / "ast-grep-rules.yaml").read_text()
    for package, (_, manifest) in packages.items():
        deps = (
            "|".join(
                dep
                for dep in manifest["dependencies"].keys()
                if dep in PACKAGES_TO_INCLUDE
            )
            or "some-nonexistent-package"
        )
        rules = RULES_TEMPLATE.replace("%PACKAGE%", package).replace("%DEPS%", deps)
        subprocess.run(
            [
                "ast-grep",
                "scan",
                "--update-all",
                f"--inline-rules={rules}",
                AMALGAMATION / "src" / package,
            ],
            check=True,
        )


def gather_dependencies(
    root_manifest: tomlkit.TOMLDocument, crates: list[tomlkit.TOMLDocument]
) -> dict[str, dict]:
    """Collect and merge dependencies from the provided manifests.

    TODO: Ideally, we want to maintain hand-crafted Cargo.toml instead as
    there are a lot of dependencies that make no sense for edge.
    """
    dependencies: dict[str, dict] = {}
    workspace_deps = {
        name: {"version": spec} if isinstance(spec, str) else spec
        for name, spec in root_manifest["workspace"]["dependencies"].items()
    }

    def add_specs(path: tuple[str, ...], specs: dict) -> None:
        dest = None
        for name, spec in specs.items():
            spec = {"version": spec} if isinstance(spec, str) else spec
            if "path" in spec:
                continue
            if spec.get("workspace") is True:
                # Merge two specs. Sloppy as it won't merge features.
                spec = {
                    **workspace_deps[name],
                    **{k: v for k, v in spec.items() if k != "workspace"},
                }
            dest = dest or functools.reduce(
                lambda d, k: d.setdefault(k, {}), path, dependencies
            )
            table = tomlkit.inline_table()
            table.update(dest.get(name, {}) | spec)
            dest[name] = table

    SECTIONS = ("dependencies", "build-dependencies", "dev-dependencies")
    for manifest in crates:
        for section in SECTIONS:
            add_specs((section,), manifest.get(section, {}))
        for target_name, target_manifest in manifest.get("target", {}).items():
            for section in SECTIONS:
                add_specs(
                    ("target", target_name, section), target_manifest.get(section, {})
                )
    return dependencies


def substitute(path: Path, *replacements: tuple[str, str]) -> None:
    """Like sed(1) but worse. Complains if any pattern is not found."""
    rep = [
        (re.compile(pattern, flags=re.MULTILINE), replacement)
        for pattern, replacement in replacements
    ]
    text = path.read_text()
    for pattern, replacement in rep:
        new_text = pattern.sub(replacement, text)
        assert new_text != text, f"Pattern {pattern.pattern} not found in {path}"
        text = new_text
    path.write_text(text)


def all_file_dependencies(root: Path) -> dict[str, tuple[Path, tomlkit.TOMLDocument]]:
    """Recursively collect Cargo.toml files for all dependencies.
    Returns mapping package_name -> (path_to_package, manifest).
    """
    seen: set[Path] = {root}
    stack = [root]
    result: dict[str, tuple[Path, tomlkit.TOMLDocument]] = {}

    while stack:
        path = stack.pop()
        manifest = tomlkit.loads((path / "Cargo.toml").read_text())
        name = manifest["package"]["name"]
        if name in PACKAGES_TO_INCLUDE:
            result[manifest["package"]["name"]] = (path, manifest)
        for spec in manifest["dependencies"].values():
            if isinstance(spec, dict) and isinstance(spec.get("path"), str):
                dep = Path(os.path.normpath(path / spec["path"]))
                if dep not in seen:
                    seen.add(dep)
                    stack.append(dep)

    return dict(sorted(result.items()))


if __name__ == "__main__":
    main()
