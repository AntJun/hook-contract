"""Check positive and negative controls without exposing request data."""

import json
import sys
from pathlib import Path

folder = Path(sys.argv[1])
healthy = json.loads((folder / "healthy" / "report.json").read_text(encoding="utf-8"))
naive = json.loads((folder / "naive" / "report.json").read_text(encoding="utf-8"))
expected = {"duplicate", "concurrent_duplicate", "lost_ack_restart", "out_of_order"}
found = {row["id"] for row in naive["results"] if row["status"] == "fail"}
if healthy["exit_code"] != 0 or healthy["summary"]["pass"] != 21:
    raise SystemExit("Transactional positive control failed.")
if naive["exit_code"] != 1 or naive["summary"]["error"] or not expected <= found:
    raise SystemExit("Seeded-defect negative control failed.")
print("Both controls and named detector findings are verified.")
