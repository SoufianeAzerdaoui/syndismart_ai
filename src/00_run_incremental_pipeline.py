# src/00_run_incremental_pipeline.py
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
import pandas as pd


def run(cmd: list[str]) -> None:
    print("\n🧩 RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="dataset/new_messages.csv", help="CSV new messages (message_id,text)")
    ap.add_argument("--limit", type=int, default=10, help="Limit rows for demo")
    ap.add_argument("--base-dir", default=".", help="Project root")
    args = ap.parse_args()

    base = Path(args.base_dir).resolve()

    inbox_path = base / args.input
    if not inbox_path.exists():
        raise SystemExit(f"❌ input not found: {inbox_path}")

    clean_dir = base / "cleanData"
    ensure_parent(clean_dir / "x")  # create cleanData if missing

    # Paths used by your pipeline
    processed_path = clean_dir / "messages_processed.csv"
    rules_path = clean_dir / "messages_rules.csv"
    with_ctx_path = clean_dir / "messages_with_context.csv"
    final_path = clean_dir / "messages_final.csv"

    # Temporary batch files (only new rows)
    tmp_dir = clean_dir / "_tmp_batch"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_processed = tmp_dir / "messages_processed.csv"
    tmp_rules = tmp_dir / "messages_rules.csv"
    tmp_with_ctx = tmp_dir / "messages_with_context.csv"
    tmp_final = tmp_dir / "messages_final.csv"

    # Load inbox
    inbox = pd.read_csv(inbox_path)
    if "message_id" not in inbox.columns or "text" not in inbox.columns:
        raise SystemExit("❌ inbox must contain columns: message_id,text")

    inbox["message_id"] = inbox["message_id"].astype(str).str.strip()
    inbox["text"] = inbox["text"].astype(str)

    # Filter already processed (based on final file)
    if final_path.exists():
        done = pd.read_csv(final_path)
        if "message_id" in done.columns:
            done_ids = set(done["message_id"].astype(str).str.strip().tolist())
        else:
            done_ids = set()
    else:
        done_ids = set()

    new_rows = inbox[~inbox["message_id"].isin(done_ids)].copy()
    if args.limit:
        new_rows = new_rows.head(args.limit).copy()

    print(f"📥 Inbox rows: {len(inbox)} | ✅ New rows to process: {len(new_rows)}")
    if len(new_rows) == 0:
        print("✅ Nothing new. Exiting.")
        return

    # --- Step 0: save "processed" input for the next scripts
    # Here we keep it simple: just rename text -> text_clean if your next scripts expect text_clean later,
    # but your notebook produces messages_processed.csv. For demo: we create minimal processed schema.
    # If your rules script expects text_clean, we create it.
    new_rows["text_clean"] = new_rows["text"]
    new_rows.to_csv(tmp_processed, index=False, encoding="utf-8")
    print("✅ tmp processed:", tmp_processed)

    # --- Step 1: run rules baseline on tmp_processed
    # We run your existing script but it reads/writes fixed paths.
    # So we temporarily swap cleanData/messages_processed.csv with our tmp batch.
    # (Safe for demo; for prod we can add --input/--output args later.)
    backup_processed = None
    if processed_path.exists():
        backup_processed = tmp_dir / "_backup_messages_processed.csv"
        shutil.copy2(processed_path, backup_processed)

    shutil.copy2(tmp_processed, processed_path)
    run(["python", str(base / "src" / "03_rules_baseline.py")])

    # after run, rules output should exist:
    if not rules_path.exists():
        raise SystemExit("❌ rules output not found: cleanData/messages_rules.csv")
    shutil.copy2(rules_path, tmp_rules)

    # restore original processed
    if backup_processed:
        shutil.copy2(backup_processed, processed_path)

    # --- Step 2: RAG retrieve (reads messages_rules.csv -> writes messages_with_context.csv)
    # Swap in tmp_rules as current messages_rules.csv
    backup_rules = None
    if rules_path.exists():
        backup_rules = tmp_dir / "_backup_messages_rules.csv"
        shutil.copy2(rules_path, backup_rules)

    shutil.copy2(tmp_rules, rules_path)
    run(["python", str(base / "src" / "07_rag_retrieve_for_messages.py")])

    if not with_ctx_path.exists():
        raise SystemExit("❌ rag context output not found: cleanData/messages_with_context.csv")
    shutil.copy2(with_ctx_path, tmp_with_ctx)

    if backup_rules:
        shutil.copy2(backup_rules, rules_path)

    # --- Step 3: LLM generation (reads messages_with_context.csv -> writes messages_final.csv)
    backup_with = None
    if with_ctx_path.exists():
        backup_with = tmp_dir / "_backup_messages_with_context.csv"
        shutil.copy2(with_ctx_path, backup_with)

    shutil.copy2(tmp_with_ctx, with_ctx_path)
    run(["python", str(base / "src" / "09_rag_generate_responses.py"), "--limit", str(args.limit)])

    if not final_path.exists():
        raise SystemExit("❌ final output not found: cleanData/messages_final.csv")
    shutil.copy2(final_path, tmp_final)

    if backup_with:
        shutil.copy2(backup_with, with_ctx_path)

    # --- Merge tmp_final into real final (append unique message_id)
    batch_final = pd.read_csv(tmp_final)
    if final_path.exists():
        master = pd.read_csv(final_path)
    else:
        master = pd.DataFrame()

    if len(master) > 0 and "message_id" in master.columns:
        master_ids = set(master["message_id"].astype(str).str.strip().tolist())
        batch_final["message_id"] = batch_final["message_id"].astype(str).str.strip()
        to_add = batch_final[~batch_final["message_id"].isin(master_ids)].copy()
        merged = pd.concat([master, to_add], ignore_index=True)
    else:
        merged = batch_final

    merged.to_csv(final_path, index=False, encoding="utf-8")
    print(f"\n✅ MERGED OK -> {final_path}")
    print(f"➕ Added rows: {len(new_rows)}")


if __name__ == "__main__":
    main()
