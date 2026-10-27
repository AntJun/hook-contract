"""Small, script-free review artifacts containing only approved result fields."""

import html
import json
import xml.etree.ElementTree as ET
from string import Template

HTML = Template("""<!doctype html>
<html lang="en-US"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<title>$title | HookContract</title><style>
:root{font:16px/1.55 system-ui,sans-serif;background:#f2f6f5;color:#17332f}*{box-sizing:border-box}
body{margin:0}main{max-width:1150px;padding:48px 24px;margin:auto}header{background:#fff;border-top:5px solid #087f70;padding:28px;border-radius:6px}
.brand{font-weight:800;letter-spacing:.04em;color:#087f70}h1{font-size:clamp(2rem,4vw,3rem);line-height:1.15}
a{color:#05695d}.cards{display:flex;flex-wrap:wrap;gap:16px;margin:28px 0}.card{flex:1;min-width:150px;background:white;border:1px solid #cadbd6;border-radius:6px;padding:20px}
.card strong{display:block;font-size:1.8rem}.pass{color:#087544}.fail{color:#a22732}.error{color:#8b5200}
.pass,.fail,.error{font-weight:800}.table-wrap{overflow-x:auto;background:#fff;border:1px solid #cadbd6;border-radius:6px}
table{border-collapse:collapse;width:100%;text-align:left}th,td{padding:14px 20px;vertical-align:top;border-bottom:1px solid #dce7e3}th{background:#eaf2ef}
small{color:#506d65}details{margin-top:8px}summary{cursor:pointer;color:#05695d}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:.8rem;max-height:450px;overflow:auto;background:#f2f6f5;padding:12px}
footer{margin-top:30px;color:#506d65;font-size:.85rem}code{background:#e5efea;padding:2px 5px}
</style></head><body><main><header><div class="brand">HookContract / 0.1.0</div><h1>$title</h1>
<p>Delivery acknowledgments and committed state, checked together under the
<code>durable-snapshot-v1</code> fixture profile.</p></header>
<section class="cards" aria-label="Contract summary"><div class="card">Overall<strong class="$status">$status_upper</strong></div>
<div class="card">Passed<strong>$passed</strong></div><div class="card">Violations<strong>$failed</strong></div>
<div class="card">Incomplete<strong>$errors</strong></div></section>
<section class="table-wrap"><table><thead><tr><th scope="col">Scenario</th><th scope="col">Decision</th><th scope="col">Evidence</th></tr></thead><tbody>$rows</tbody></table></section>
<section><h2>Interpretation</h2><p>A pass describes the observed scenarios and fixture profile.
Missing delivery, independent observation, settling, or restart evidence produces an incomplete result.
It supplies no general exactly-once guarantee or production certification.</p>
<p>Expected effect lists show allowed counts for scheduler-dependent races. The reference observer
uses an independent SQLite read transaction. The post-commit failure terminates the fixture process
before acknowledgment; a new process reads the same database.</p>
<p>Artifacts omit raw payloads, HTTP headers, keys, signatures, database files, addresses, and ports.
Timing values describe these test requests and establish no performance target.</p></section>
<footer>Maintained by <a href="https://github.com/AntJun">AntJun</a> &middot;
<a href="https://github.com/AntJun/hook-contract">Source and profile</a> &middot; Exit codes: 0 pass, 1 violation, 2 incomplete.</footer>
</main></body></html>""")


def render_html(report, title="Contract report"):
    rows = []
    for row in report["results"]:
        payload = {key: row[key] for key in ("requests", "observations", "confirmed_restarts")}
        rows.append("<tr><td><strong>" + html.escape(row["id"].replace("_", " "))
                    + '</strong></td><td class="' + row["status"] + '">'
                    + row["status"].upper() + "</td><td>" + html.escape(row["reason"])
                    + "<details><summary>Inspect observations and HTTP attempts</summary><pre>"
                    + html.escape(json.dumps(payload, indent=2, allow_nan=False))
                    + "</pre></details></td></tr>")
    return HTML.substitute(title=html.escape(title), status=report["status"],
                           status_upper=report["status"].upper(), passed=report["summary"]["pass"],
                           failed=report["summary"]["fail"], errors=report["summary"]["error"],
                           rows="".join(rows))


def render_junit(report):
    root = ET.Element("testsuite", name="HookContract", tests=str(len(report["results"])),
                      failures=str(report["summary"]["fail"]), errors=str(report["summary"]["error"]))
    for row in report["results"]:
        case = ET.SubElement(root, "testcase", classname=report["profile"], name=row["id"])
        if row["status"] != "pass":
            ET.SubElement(case, "error" if row["status"] == "error" else "failure",
                          message=row["reason"]).text = row["reason"]
    return ET.tostring(root, encoding="unicode", xml_declaration=True) + "\n"
