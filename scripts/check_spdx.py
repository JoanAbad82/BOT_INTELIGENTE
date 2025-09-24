import sys

missing = []
for f in sys.argv[1:]:
    try:
        with open(f, encoding="utf-8", errors="replace") as fh:
            if "SPDX-License-Identifier:" not in fh.read():
                missing.append(f)
    except Exception:
        pass

if missing:
    print("Missing SPDX header in:")
    for m in missing:
        print(m)
    sys.exit(1)
