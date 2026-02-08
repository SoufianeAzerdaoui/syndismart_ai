#!/usr/bin/env bash
set -eo pipefail

PROJECT_DIR="/home/onizuka/Bureau/ai_project"
VENV="$PROJECT_DIR/venv"
export AIRFLOW__CLI__OUTPUT_COLOR=never
export PYTHONWARNINGS="ignore::UserWarning"

export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CLI__OUTPUT_COLOR=never   # ✅ IMPORTANT

DAG_ID="syndic_rag_pipeline"
LIMIT="${1:-10}"

source "$VENV/bin/activate"

echo "▶️ Starting Airflow scheduler (background)..."
SCHED_LOG="/tmp/airflow_scheduler_${DAG_ID}.log"

airflow scheduler >"$SCHED_LOG" 2>&1 &
SCHED_PID=$!
echo "   scheduler pid=$SCHED_PID"
sleep 3

cleanup() {
  if ps -p "$SCHED_PID" >/dev/null 2>&1; then
    echo "🛑 Stopping scheduler (pid=$SCHED_PID)"
    kill "$SCHED_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "▶️ Triggering DAG: $DAG_ID (limit=$LIMIT)"
airflow dags trigger "$DAG_ID" --conf "{\"limit\": $LIMIT}" >/dev/null

# récupérer le run_id le plus récent
RUN_ID="$(airflow dags list-runs -d "$DAG_ID" 2>/dev/null | awk 'NR==2{print $1}')"

if [[ -z "$RUN_ID" ]]; then
  echo "❌ Impossible de récupérer le run_id"
  exit 1
fi

echo "🆔 Run id: $RUN_ID"
echo "⏳ Waiting for DAG completion..."

while true; do
  STATE="$(airflow dags list-runs -d "$DAG_ID" 2>/dev/null | awk -v r="$RUN_ID" '$1==r{print $NF}')"
  printf "\r   state=%s" "$STATE"

  if [[ "$STATE" == "success" || "$STATE" == "failed" ]]; then
    echo ""
    break
  fi
  sleep 3
done

if [[ "$STATE" == "success" ]]; then
  echo "✅ DAG SUCCESS ($RUN_ID)"
  exit 0
fi

echo "❌ DAG FAILED ($RUN_ID)"
echo ""
echo "📌 Task states:"
airflow tasks states-for-dag-run "$DAG_ID" "$RUN_ID" || true

echo ""
echo "📄 Logs des tâches échouées:"
FAILED_TASKS=$(airflow tasks states-for-dag-run "$DAG_ID" "$RUN_ID" \
  | awk 'NR>1 && ($2=="failed" || $2=="upstream_failed"){print $1}')

for t in $FAILED_TASKS; do
  echo ""
  echo "================ $t ================"
  airflow tasks logs "$DAG_ID" "$t" "$RUN_ID" || true
done

echo ""
echo "📄 Scheduler log (tail):"
tail -n 120 "$SCHED_LOG" || true

exit 1
