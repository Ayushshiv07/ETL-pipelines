"""
======================================================
E-commerce ETL Pipeline — Scheduler Service
======================================================
Runs the ETL pipeline daily at a configurable time.
Features:
  - Detached background daemon process for Windows/Unix
  - PID-based process tracking (start, stop, status)
  - Execution logging to logs/scheduler.log
  - Configurable daily run time
======================================================
"""

import os
import sys
import time
import argparse
import logging
import subprocess
from datetime import datetime
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Setup scheduler logging
log_file = os.path.join(LOG_DIR, "scheduler.log")
pid_file = os.path.join(LOG_DIR, "scheduler.pid")

is_daemon = "--run-daemon" in sys.argv
handlers = [logging.FileHandler(log_file, encoding="utf-8")]
if not is_daemon:
    handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Scheduler) %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
    config = yaml.safe_load(f)


def get_schedule_time() -> str:
    """Get the daily scheduled time from config or default to 02:00."""
    try:
        # Default fallback to 02:00 (2 AM)
        return config.get("scheduler", {}).get("run_time", "02:00")
    except Exception:
        return "02:00"


def is_pid_running(pid: int) -> bool:
    """Check if a process with given PID is running."""
    if pid <= 0:
        return False
    try:
        if sys.platform == "win32":
            # On Windows, use tasklist to check PID
            out = subprocess.check_output(f"tasklist /FI \"PID eq {pid}\"", shell=True, text=True)
            return str(pid) in out
        else:
            # On Unix, use kill -0 signal
            os.kill(pid, 0)
            return True
    except Exception:
        return False


def get_status() -> dict:
    """Get the running status of the scheduler daemon."""
    if not os.path.exists(pid_file):
        return {"status": "Stopped", "pid": None}
    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
        if is_pid_running(pid):
            return {"status": "Running", "pid": pid}
        else:
            # Stale PID file
            return {"status": "Stopped (Stale PID)", "pid": pid}
    except Exception:
        return {"status": "Stopped", "pid": None}


def start_daemon():
    """Start the scheduler as a detached daemon process."""
    status = get_status()
    if status["status"] == "Running":
        logger.info(f"Scheduler is already running with PID: {status['pid']}")
        return

    logger.info("Starting scheduler daemon...")
    
    # Detach process depending on OS and discard stdout/stderr to avoid Windows handle crashes
    script_path = os.path.abspath(__file__)
    if sys.platform == "win32":
        proc = subprocess.Popen(
            [sys.executable, script_path, "--run-daemon"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True
        )
    else:
        proc = subprocess.Popen(
            [sys.executable, script_path, "--run-daemon"],
            cwd=PROJECT_ROOT,
            preexec_fn=os.setpgrp,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True
        )
    
    logger.info(f"Scheduler daemon started with PID: {proc.pid}")


def stop_daemon():
    """Stop the scheduler daemon process."""
    status = get_status()
    if status["status"] != "Running":
        logger.info("Scheduler is not running.")
        # Clean up pid file if it exists
        if os.path.exists(pid_file):
            os.remove(pid_file)
        return

    pid = status["pid"]
    logger.info(f"Stopping scheduler daemon (PID: {pid})...")
    try:
        if sys.platform == "win32":
            subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=True)
        else:
            os.kill(pid, 9)
        logger.info("Scheduler daemon stopped [OK].")
    except Exception as e:
        logger.error(f"Failed to kill process {pid}: {e}")
    
    if os.path.exists(pid_file):
        os.remove(pid_file)


def run_pipeline_job():
    """Execute the ETL pipeline."""
    logger.info("Triggering ETL Pipeline run...")
    try:
        # Import and run standalone pipeline
        from scripts.run_pipeline import run_pipeline
        # Run SQLite pipeline in incremental mode daily
        run_pipeline(target="sqlite", mode="incremental", generate_data=True)
        logger.info("ETL Pipeline job completed successfully.")
    except Exception as e:
        logger.error(f"ETL Pipeline job failed: {e}")


def run_daemon_loop():
    """Main loop for the scheduler daemon."""
    # Write PID file
    pid = os.getpid()
    with open(pid_file, "w") as f:
        f.write(str(pid))

    scheduled_time = get_schedule_time()
    logger.info(f"Scheduler daemon active. PID: {pid}. Scheduled daily run time: {scheduled_time}")

    last_run_date = None

    while True:
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            current_date = now.strftime("%Y-%m-%d")

            if current_time == scheduled_time and current_date != last_run_date:
                logger.info(f"Time matches schedule ({scheduled_time}). Running scheduled job.")
                run_pipeline_job()
                last_run_date = current_date
                time.sleep(60) # Avoid double triggers
            else:
                time.sleep(10) # Poll every 10 seconds
        except KeyboardInterrupt:
            logger.info("Daemon loop interrupted by user. Exiting.")
            break
        except Exception as e:
            logger.error(f"Error in daemon loop: {e}")
            time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Scheduler Controller")
    parser.add_argument("--start", action="store_true", help="Start the scheduler daemon")
    parser.add_argument("--stop", action="store_true", help="Stop the scheduler daemon")
    parser.add_argument("--status", action="store_true", help="Check status of the scheduler daemon")
    parser.add_argument("--run-daemon", action="store_true", help="Run the daemon loop (internal use)")
    parser.add_argument("--run-now", action="store_true", help="Run the pipeline job immediately")
    args = parser.parse_args()

    if args.start:
        start_daemon()
    elif args.stop:
        stop_daemon()
    elif args.status:
        status = get_status()
        print(f"Status: {status['status']} | PID: {status['pid']}")
    elif args.run_daemon:
        run_daemon_loop()
    elif args.run_now:
        run_pipeline_job()
    else:
        parser.print_help()
