#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python3.withPackages(ps: [ps.pymysql])"
"""Check the archive still answers the things Pippijn knows to be true.

⚠ **THIS IS A POSITIVE CONTROL, WHICH IS THE ONLY KIND OF CHECK THAT EARNS A
NULL.** On 2026-09-18 four absences were reported that the checking method had
manufactured — Google Chat "had no replies" while 126 sat unread at index 36,
the capture was "clean" while it took no attachments at all, Signal "had no read
state" after fifteen months of discarding receipts. Every one of those checks
came back clean because it could not have come back otherwise.

A row in `known_truths.tsv` is something known INDEPENDENTLY of this archive. If
a query stops returning it, the pipeline has lost something and says so. That is
the difference between a test and an audit: an audit finds what you thought to
look for, a test tells you when what you already found goes away.

⚠ **`expect` IS A FLOOR, NOT AN EQUALITY.** These counts grow — more messages,
more reactions, more calls. Asserting equality would turn every ordinary day
into a failure and the check would be muted within a week; asserting a floor
fails only when something has been LOST, which is the event worth waking up for.

Usage (env: DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME):

    ./check_known_truths.py [--sql]

⚠ `--sql` prints the queries for a database this machine cannot reach — the
archive lives in the cluster and the fleet is not routable from here. Same rows,
same file, so the two cannot disagree.
"""
import os
import sys
import pathlib

TRUTHS = pathlib.Path(__file__).resolve().parent / "known_truths.tsv"


def rows():
    for line in TRUTHS.read_text().splitlines():
        if not line.strip() or line.startswith("#") or line.startswith("name\t"):
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            print(f"malformed row: {line[:60]}", file=sys.stderr)
            continue
        name, expect, sql = parts
        yield name.strip(), int(expect), sql.strip()


def main():
    if "--sql" in sys.argv:
        for name, expect, sql in rows():
            # The name is echoed as a literal so the output is readable beside
            # the number, wherever it is piped.
            print(f"SELECT {expect} AS expect, ({sql}) AS actual, "
                  f"{expect} <= ({sql}) AS ok, '{name.replace(chr(39), '')}' AS what;")
        return

    import pymysql

    conn = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"], charset="utf8mb4")
    cur = conn.cursor()
    bad = 0
    for name, expect, sql in rows():
        cur.execute(sql)
        got = cur.fetchone()[0]
        ok = got >= expect
        bad += 0 if ok else 1
        print(f"{'ok  ' if ok else 'LOST'}  {got:>6} (>= {expect:<6}) {name}")
    conn.close()
    if bad:
        print(f"\n{bad} truth(s) the archive can no longer answer", file=sys.stderr)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
