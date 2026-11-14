
import json

from src import monitoring


def test_successful_run_is_logged(tmp_path, monkeypatch):
    log_path = tmp_path / "pipeline_runs.jsonl"

    monkeypatch.setattr(
        monitoring,
        "RUN_LOG_PATH",
        str(log_path)
    )

    run = monitoring.start_run()

    record = monitoring.finish_run(
        run,
        status="SUCCESS",
        input_records=1000,
        valid_records=950,
        rejected_records=50,
        aggregation_groups=100,
    )

    assert record["status"] == "SUCCESS"
    assert record["input_records"] == 1000
    assert record["valid_records"] == 950
    assert record["rejected_records"] == 50
    assert record["aggregation_groups"] == 100
    assert record["error"] is None
    assert record["duration_seconds"] >= 0

    saved_record = json.loads(
        log_path.read_text(encoding="utf-8").strip()
    )

    assert saved_record["run_id"] == run["run_id"]


def test_failed_run_is_logged(tmp_path, monkeypatch):
    log_path = tmp_path / "pipeline_runs.jsonl"

    monkeypatch.setattr(
        monitoring,
        "RUN_LOG_PATH",
        str(log_path)
    )

    run = monitoring.start_run()

    record = monitoring.finish_run(
        run,
        status="FAILED",
        input_records=500,
        error=RuntimeError("Simulated pipeline failure"),
    )

    assert record["status"] == "FAILED"
    assert record["input_records"] == 500
    assert record["error"] == "Simulated pipeline failure"

    saved_record = json.loads(
        log_path.read_text(encoding="utf-8").strip()
    )

    assert saved_record["status"] == "FAILED"
    assert saved_record["error"] == "Simulated pipeline failure"


def test_each_run_has_a_unique_id():
    first_run = monitoring.start_run()
    second_run = monitoring.start_run()

    assert first_run["run_id"] != second_run["run_id"]


def test_multiple_runs_are_appended(tmp_path, monkeypatch):
    log_path = tmp_path / "pipeline_runs.jsonl"

    monkeypatch.setattr(
        monitoring,
        "RUN_LOG_PATH",
        str(log_path)
    )

    for _ in range(2):
        run = monitoring.start_run()
        monitoring.finish_run(run, status="SUCCESS")

    records = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]

    assert len(records) == 2
    assert records[0]["run_id"] != records[1]["run_id"]
