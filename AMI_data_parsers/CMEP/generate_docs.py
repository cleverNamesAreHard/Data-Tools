import datetime
import html
import inspect
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


RE_DEF = re.compile(r"^\s*def\s+([A-Za-z_]\w*)\s*\(")
RE_SECTION_TITLE = re.compile(r"^\s*#\s*([A-Za-z].*functions)\s*$")


SPEC_PDF_URL = (
    "https://www.sce.com/sites/default/files/inline-files/14%2B-%2BCalifornia%2BMetering%2BExchange%2BProtocol%2B-%2BV4.1-022013_AA.pdf"
)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _scan_functions_and_sections(lines: List[str]) -> List[Tuple[str, str, int, str]]:
    # Returns: (name, section, def_lineno0, signature_without_def)
    refs: List[Tuple[str, str, int, str]] = []
    current_section = "Functions"

    i = 0
    while i < len(lines):
        line = lines[i]

        m_sec = RE_SECTION_TITLE.match(line)
        if m_sec:
            current_section = m_sec.group(1).strip()
            i += 1
            continue

        m_def = RE_DEF.match(line)
        if not m_def:
            i += 1
            continue

        name = m_def.group(1)
        def_lineno0 = i

        sig_lines: List[str] = [line.rstrip("\n")]
        j = i + 1
        while j < len(lines):
            if sig_lines[-1].rstrip().endswith(":"):
                break
            sig_lines.append(lines[j].rstrip("\n"))
            j += 1

        sig_raw = " ".join(s.strip() for s in sig_lines).strip()
        sig_raw = re.sub(r"\s+", " ", sig_raw)

        sig = sig_raw
        if sig.startswith("def "):
            sig = sig[len("def ") :]
        if sig.endswith(":"):
            sig = sig[:-1].rstrip()

        refs.append((name, current_section, def_lineno0, sig))
        i = j + 1

    return refs


def _load_module(repo_root: Path, module_path: Path, module_name: str):
    import importlib.util

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec for: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _clean_typing_str(s: str) -> str:
    s = s.replace("typing.", "")
    s = s.replace("NoneType", "None")
    return s


def _format_annotation(ann: Any) -> str:
    if ann is inspect._empty:
        return "Any"
    if isinstance(ann, str):
        return ann
    if getattr(ann, "__name__", None):
        return ann.__name__
    return _clean_typing_str(str(ann))


def _format_default(val: Any) -> str:
    if val is inspect._empty:
        return ""
    if isinstance(val, str):
        return repr(val)
    return repr(val)


def _extract_description(doc: str) -> str:
    lines = doc.replace("\t", "    ").splitlines()
    cut = len(lines)
    for i, line in enumerate(lines):
        t = line.strip()
        if t in ("Args:", "Returns:", "Raises:"):
            cut = i
            break
    desc_lines = lines[:cut]

    while desc_lines and desc_lines[0].strip() == "":
        desc_lines.pop(0)
    while desc_lines and desc_lines[-1].strip() == "":
        desc_lines.pop()

    return "\n".join(desc_lines).rstrip()


def _overview_html() -> str:
    return f"""
<div class="card">
  <h2>Overview</h2>
  <p>
    This repository contains a production-grade parser and validator for CMEP MEPMD01 interval data exports.
    It reads an input CSV with no header row, validates each field against the MEPMD01 rules, and writes a
    normalized output CSV with one interval per output row.
  </p>
  <p>
    The implementation writes output atomically: it writes to a temp file and only commits the final output
    when the entire input parses successfully.
  </p>
</div>
""".strip()


def _overview_md() -> str:
    return (
        "## Overview\n\n"
        "This repository contains a production-grade parser and validator for CMEP MEPMD01 interval data exports. "
        "It reads an input CSV with no header row, validates each field against the MEPMD01 rules, and writes a "
        "normalized output CSV with one interval per output row.\n\n"
        "The implementation writes output atomically: it writes to a temp file and only commits the final output "
        "when the entire input parses successfully.\n\n"
        f"CMEP specification reference (PDF): {SPEC_PDF_URL}\n"
    )


def _usage_html(module_name: str) -> str:
    return f"""
<div class="card" id="usage">
  <h2>Usage</h2>
  <div class="small">Run from the repository root.</div>
  <div style="margin-top: 10px;"></div>

  <div class="blob">
    <div class="blob-label">Command</div>
    <pre>python3 {html.escape(module_name, quote=False)} [OPTIONS] /path/to/cmep/file.csv</pre>
  </div>

  <div class="blob" style="margin-top: 10px;">
    <div class="blob-label">Options</div>
    <pre>-o, --output-file   Output CSV file path.
                   If not provided, output is written alongside the input as "&lt;input_stem&gt;_parsed.csv".
                   If provided without a directory, it is written alongside the input.</pre>
  </div>

  <div class="blob" style="margin-top: 10px;">
    <div class="blob-label">Examples</div>
    <pre>python3 {html.escape(module_name, quote=False)} data/input.csv
python3 {html.escape(module_name, quote=False)} -o out/normalized.csv data/input.csv</pre>
  </div>
</div>
""".strip()


def _usage_md(module_name: str) -> str:
    return (
        "## Usage\n\n"
        "Run from the repository root.\n\n"
        "```text\n"
        f"python3 {module_name} [OPTIONS] /path/to/cmep/file.csv\n"
        "```\n\n"
        "### Options\n\n"
        "```text\n"
        "-o, --output-file   Output CSV file path.\n"
        "                   If not provided, output is written alongside the input as \"<input_stem>_parsed.csv\".\n"
        "                   If provided without a directory, it is written alongside the input.\n"
        "```\n\n"
        "### Examples\n\n"
        "```text\n"
        f"python3 {module_name} data/input.csv\n"
        f"python3 {module_name} -o out/normalized.csv data/input.csv\n"
        "```\n"
    )


def _tests_html() -> str:
    return """
<div class="card" id="tests">
  <h2>Unit Tests</h2>
  <div class="small">Run from the repository root.</div>
  <div style="margin-top: 10px;"></div>

  <div class="blob">
    <div class="blob-label">Direct</div>
    <pre>python3 test/unit_tests.py</pre>
  </div>

  <div class="blob" style="margin-top: 10px;">
    <div class="blob-label">Wrapper</div>
    <pre>python3 test_mepmd01.py</pre>
  </div>
</div>
""".strip()


def _tests_md() -> str:
    return (
        "## Unit Tests\n\n"
        "Run from the repository root.\n\n"
        "### Direct\n\n"
        "```text\n"
        "python3 test/unit_tests.py\n"
        "```\n\n"
        "### Wrapper\n\n"
        "```text\n"
        "python3 test_mepmd01.py\n"
        "```\n"
    )

def _entrypoint_html() -> str:
    return """
<div class="card" id="entrypoint">
  <h2>Entrypoint</h2>
  <div class="small">Start of code execution</div>
  <div style="margin-top: 10px;"></div>

  <div class="blob">
    <pre>python3 parse_mepmd01.py:main()</pre>
  </div>
</div>
""".strip()

def _entrypoint_md() -> str:
    return (
        "## Entrypoint\n\n"
        "Start of code execution\n\n"
        "```text\n"
        "python3 parse_mepmd01.py:main()\n"
    )


def _build_html(
    title: str,
    module_file_name: str,
    generated_at: str,
    spec_table_html: str,
    function_refs: List[Tuple[str, str, int, str]],
    fn_meta: Dict[str, Dict[str, str]],
) -> str:
    def esc(s: str) -> str:
        return html.escape(s, quote=False)

    css = """
:root { color-scheme: light dark; }

body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
  margin: 0;
  line-height: 1.35;
}

header {
  padding: 18px 22px;
  border-bottom: 1px solid rgba(127,127,127,.35);
}

h1 { margin: 0 0 6px 0; font-size: 22px; }
.meta { opacity: .75; font-size: 13px; }

.layout {
  display: grid;
  grid-template-columns: 320px 1fr;
  min-height: 100vh;
}

nav {
  border-right: 1px solid rgba(127,127,127,.35);
  padding: 14px 14px 28px 14px;
  position: sticky;
  top: 0;
  height: 100vh;
  overflow: auto;
}

nav h2 { font-size: 14px; margin: 12px 0 8px; opacity: .85; }
nav a { display: block; text-decoration: none; padding: 6px 8px; border-radius: 8px; }
nav a:hover { background: rgba(127,127,127,.18); }

main { padding: 18px 22px 56px 22px; overflow: auto; }

.card {
  border: 1px solid rgba(127,127,127,.35);
  border-radius: 12px;
  padding: 14px 14px;
  margin: 0 0 18px 0;
}

.section-title {
  margin-top: 26px;
  padding-top: 10px;
  border-top: 1px solid rgba(127,127,127,.25);
}

.fn {
  border: 1px solid rgba(127,127,127,.35);
  border-radius: 12px;
  padding: 14px 14px;
  margin: 14px 0 18px 0;
}

.fn-name {
  margin: 0 0 10px 0;
  font-size: 18px;
}

.sigline {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 13px;
  background: rgba(127,127,127,.12);
  border-radius: 10px;
  padding: 10px 12px;
  overflow: auto;
  margin: 0 0 12px 0;
}

.blobs { display: grid; grid-template-columns: 1fr; gap: 10px; }

.blob {
  border: 1px solid rgba(127,127,127,.28);
  background: rgba(127,127,127,.08);
  border-radius: 10px;
  padding: 10px 12px;
}

.blob-label {
  font-size: 12px;
  letter-spacing: .02em;
  text-transform: uppercase;
  opacity: .75;
  margin-bottom: 8px;
}

pre {
  margin: 0;
  white-space: pre-wrap;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 13px;
}

.small { font-size: 13px; opacity: .85; }

/* Spec table styling */
.spec-table table {
  width: 100%;
  border-collapse: collapse;
  border: 1px solid rgba(127,127,127,.35);
  border-radius: 12px;
  overflow: hidden;
}

.spec-table th, .spec-table td {
  border-bottom: 1px solid rgba(127,127,127,.25);
  padding: 10px 10px;
  vertical-align: top;
}

.spec-table th {
  text-align: left;
  background: rgba(127,127,127,.14);
  font-weight: 600;
}

.spec-table tr:last-child td { border-bottom: none; }
"""

    nav_parts: List[str] = []
    nav_parts.append('<h2>Overview</h2>')
    nav_parts.append('<a href="#overview">Overview</a>')
    nav_parts.append('<h2>Important Information</h2>')
    nav_parts.append('<a href="#spec">Spec Table</a>')
    nav_parts.append('<a href="#usage">Usage</a>')
    nav_parts.append('<a href="#tests">Unit Tests</a>')
    nav_parts.append('<a href="#entrypoint">Entrypoint</a>')

    last_section = None
    for name, section, _, _ in function_refs:
        if section != last_section:
            nav_parts.append(f"<h2>{esc(section)}</h2>")
            last_section = section
        nav_parts.append(f'<a href="#fn-{esc(name)}">{esc(name)}</a>')

    main_parts: List[str] = []
    main_parts.append(f'<div id="overview">{_overview_html()}</div>')

    main_parts.append('<div class="card spec-table" id="spec">')
    main_parts.append("<h2>MEPMD01 Specifications</h2>")
    main_parts.append(
        f'<div class="small">Source: <a href="{html.escape(SPEC_PDF_URL, quote=True)}" target="_blank" rel="noopener noreferrer">{esc(SPEC_PDF_URL)}</a></div>'
    )
    main_parts.append("<div style=\"margin-top: 12px;\">")
    main_parts.append(spec_table_html)
    main_parts.append("</div>")
    main_parts.append("</div>")

    main_parts.append(_usage_html(module_file_name))
    main_parts.append(_tests_html())
    main_parts.append(_entrypoint_html())

    last_section = None
    for name, section, _, sig in function_refs:
        if section != last_section:
            main_parts.append(f'<h2 class="section-title" id="sec-{esc(section)}">{esc(section)}</h2>')
            last_section = section

        meta = fn_meta.get(name, {})
        params_blob = meta.get("params", "")
        returns_blob = meta.get("returns", "")
        desc_blob = meta.get("description", "")

        main_parts.append(f'<div class="fn" id="fn-{esc(name)}">')
        main_parts.append(f'<div class="fn-name">{esc(name)}</div>')
        main_parts.append(f'<div class="sigline">{esc(sig)}</div>')

        main_parts.append('<div class="blobs">')

        main_parts.append('<div class="blob">')
        main_parts.append('<div class="blob-label">Parameters</div>')
        main_parts.append(f"<pre>{esc(params_blob) if params_blob else esc('None')}</pre>")
        main_parts.append("</div>")

        main_parts.append('<div class="blob">')
        main_parts.append('<div class="blob-label">Returns</div>')
        main_parts.append(f"<pre>{esc(returns_blob) if returns_blob else esc('Any')}</pre>")
        main_parts.append("</div>")

        main_parts.append('<div class="blob">')
        main_parts.append('<div class="blob-label">Description</div>')
        main_parts.append(f"<pre>{esc(desc_blob) if desc_blob else esc('No docstring found.')}</pre>")
        main_parts.append("</div>")

        main_parts.append("</div>")
        main_parts.append("</div>")

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>{css}</style>
</head>
<body>
<header>
  <h1>{esc(title)}</h1>
  <div class="meta">Generated: {esc(generated_at)}</div>
</header>
<div class="layout">
  <nav>
    {''.join(nav_parts)}
  </nav>
  <main>
    {''.join(main_parts)}
  </main>
</div>
</body>
</html>
"""


def _build_markdown(
    module_file_name: str,
    generated_at: str,
    spec_table_html: str,
    function_refs: List[Tuple[str, str, int, str]],
    fn_meta: Dict[str, Dict[str, str]],
) -> str:
    out: List[str] = []
    out.append(f"# {module_file_name}")
    out.append("")
    out.append(f"Generated: {generated_at}")
    out.append("")
    out.append(_overview_md().rstrip())
    out.append("")
    out.append("## MEPMD01 Specifications")
    out.append("")
    out.append(f"Source (PDF): {SPEC_PDF_URL}")
    out.append("")
    out.append(spec_table_html.strip())
    out.append("")
    out.append(_usage_md(module_file_name).rstrip())
    out.append("")
    out.append(_tests_md().rstrip())
    out.append("")
    out.append(_entrypoint_md().rstrip())
    out.append("")
    out.append("## Function reference")
    out.append("")

    last_section = None
    for name, section, _, sig in function_refs:
        if section != last_section:
            out.append(f"### {section}")
            out.append("")
            last_section = section

        meta = fn_meta.get(name, {})
        params_blob = meta.get("params", "")
        returns_blob = meta.get("returns", "")
        desc_blob = meta.get("description", "")

        out.append(f"#### `{name}`")
        out.append("")
        out.append("```python")
        out.append(sig)
        out.append("```")
        out.append("")
        out.append("**Parameters**")
        out.append("")
        out.append("```text")
        out.append(params_blob if params_blob else "None")
        out.append("```")
        out.append("")
        out.append("**Returns**")
        out.append("")
        out.append("```text")
        out.append(returns_blob if returns_blob else "Any")
        out.append("```")
        out.append("")
        out.append("**Description**")
        out.append("")
        out.append("```text")
        out.append(desc_blob if desc_blob else "No docstring found.")
        out.append("```")
        out.append("")

    return "\n".join(out).rstrip() + "\n"


def main() -> None:
    repo_root = Path(__file__).resolve().parent
    module_path = repo_root / "parse_mepmd01.py"
    module_name = "parse_mepmd01"
    docs_dir = repo_root / "docs"
    spec_table_path = docs_dir / "spec_table.html"

    if not module_path.exists():
        raise FileNotFoundError(f"Expected module not found: {module_path}")

    if not spec_table_path.exists():
        raise FileNotFoundError(
            f"Expected spec table not found: {spec_table_path}. Create docs/spec_table.html first."
        )

    source = _read_text(module_path)
    lines = source.splitlines(True)
    spec_table_html = _read_text(spec_table_path)

    function_refs = _scan_functions_and_sections(lines)

    module = _load_module(repo_root, module_path, module_name)

    fn_meta: Dict[str, Dict[str, str]] = {}
    for name, _, _, _ in function_refs:
        obj = getattr(module, name, None)
        if obj is None:
            fn_meta[name] = {"params": "", "returns": "", "description": ""}
            continue

        try:
            sig = inspect.signature(obj)
        except (TypeError, ValueError):
            fn_meta[name] = {"params": "", "returns": "", "description": _extract_description(getattr(obj, "__doc__", "") or "")}
            continue

        params_lines: List[str] = []
        for p in sig.parameters.values():
            ann = _format_annotation(p.annotation)
            default = _format_default(p.default)

            if default:
                params_lines.append(f"{p.name}: {ann} = {default}")
            else:
                params_lines.append(f"{p.name}: {ann}")

        returns_str = _format_annotation(sig.return_annotation)

        doc = getattr(obj, "__doc__", None) or ""
        desc = _extract_description(doc)

        fn_meta[name] = {
            "params": "\n".join(params_lines).rstrip(),
            "returns": returns_str,
            "description": desc,
        }

    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_out = _build_html(
        title=f"{module_path.name} docs",
        module_file_name=module_path.name,
        generated_at=generated_at,
        spec_table_html=spec_table_html,
        function_refs=function_refs,
        fn_meta=fn_meta,
    )

    md_out = _build_markdown(
        module_file_name=module_path.name,
        generated_at=generated_at,
        spec_table_html=spec_table_html,
        function_refs=function_refs,
        fn_meta=fn_meta,
    )

    _write_text(docs_dir / f"{module_name}.html", html_out)
    _write_text(docs_dir / f"{module_name}.md", md_out)

    print(f"Wrote {os.path.relpath(docs_dir / f'{module_name}.html', repo_root)}")
    print(f"Wrote {os.path.relpath(docs_dir / f'{module_name}.md', repo_root)}")


if __name__ == "__main__":
    main()