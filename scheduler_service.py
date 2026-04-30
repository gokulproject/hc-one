"""
healthcheck/scheduler_service.py  (v10 – start_at gate fixed)
==============================================================
FIXES from v9:
  1. _normalise_utc() — strips tzinfo consistently so all comparisons
     are naive-UTC vs naive-UTC. Eliminates the +5:30 drift when Django
     USE_TZ=True causes the DB driver to return aware datetimes.
  2. After start_at is reached and the run fires, start_at is set to
     NULL in the DB and the job is re-registered without the gate arg,
     so subsequent cron ticks fire unconditionally.
  3. _register_jobs now passes start_at=None for rows where start_at
     is already NULL (already fired once), avoiding a redundant check.
"""
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("hc.scheduler")

_scheduler       = None
_scheduler_lock  = threading.Lock()
_active_runs: dict = {}
_active_runs_lock  = threading.Lock()


# ── IST helpers ───────────────────────────────────────────────────────────
def _now_ist():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() + timedelta(hours=5, minutes=30)

def _now_utc():
    return datetime.utcnow()

def _utc_to_ist(dt):
    if dt is None: return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        try:
            from zoneinfo import ZoneInfo
            return dt.astimezone(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        except Exception:
            return dt.replace(tzinfo=None) + timedelta(hours=5, minutes=30)
    return dt + timedelta(hours=5, minutes=30)

def _ist_to_utc(dt):
    if dt is None: return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        return dt.astimezone(__import__('datetime').timezone.utc).replace(tzinfo=None)
    return dt - timedelta(hours=5, minutes=30)

def _fmt_ist_12h(dt):
    if dt is None: return "Never"
    return _utc_to_ist(dt).strftime("%d-%b-%Y %I:%M %p")

def _next_cron_utc(cron_expr: str, after_ist: datetime) -> datetime:
    from croniter import croniter
    ci = croniter(cron_expr, after_ist)
    return _ist_to_utc(ci.get_next(datetime))


# FIX 1 ── Normalise any datetime to naive UTC ────────────────────────────
# Problem: DB driver (via Django USE_TZ=True) may return start_at as an
# *aware* UTC datetime, or even as a naive value that is actually in IST
# depending on MySQL time_zone setting.  Stripping tzinfo inline inside
# _fire_schedule was inconsistent and could leave a value that is really
# IST-as-naive, causing now_utc < sa_utc to always be True (+5:30 gap).
#
# Solution: always go through _normalise_utc() which:
#   • If aware → convert to UTC then strip tzinfo (always correct).
#   • If naive → assume it is already UTC (Django's default DB storage).
# This keeps every comparison as naive-UTC vs naive-UTC.
def _normalise_utc(dt) -> Optional[datetime]:
    """Return dt as a naive UTC datetime, or None."""
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        # Aware datetime: convert to UTC then strip
        import datetime as _dt_mod
        return dt.astimezone(_dt_mod.timezone.utc).replace(tzinfo=None)
    # Naive datetime: assumed to already be UTC (Django DB default)
    return dt


# ── DB error types ────────────────────────────────────────────────────────
def _db_err():
    types = (ConnectionError,)
    try:
        from healthcheck.engine.core.db_manager import DBConnectionError
        types += (DBConnectionError,)
    except ImportError: pass
    try:
        import pymysql.err as pe
        types += (pe.OperationalError, pe.InterfaceError)
    except ImportError: pass
    return types


def _get_engine():
    from healthcheck.engine_bridge import get_engine
    return get_engine()


# ── Tracker helpers ───────────────────────────────────────────────────────
def _update_tracker(db, tracker_id: int, **kwargs):
    try:
        if not kwargs or not tracker_id: return
        sets = ", ".join(f"{k}=%s" for k in kwargs)
        vals = list(kwargs.values()) + [tracker_id]
        db.execute(
            f"UPDATE scheduler_tracker SET {sets}, updated_at=NOW() WHERE id=%s", vals)
    except Exception as e:
        logger.warning(f"tracker update: {e}")

def _create_tracker(db, schedule_id, customer_id, env_types,
                    schedule_name, cron_expr, fire_at_utc,
                    triggered_by="apscheduler") -> int:
    try:
        return db.insert_get_id(
            """INSERT INTO scheduler_tracker
               (schedule_id,customer_id,env_types,schedule_name,cron_expression,
                status,scheduled_fire_at,triggered_by,is_active,created_at,updated_at)
               VALUES(%s,%s,%s,%s,%s,'PENDING',%s,%s,1,NOW(),NOW())""",
            (schedule_id, customer_id, env_types, schedule_name,
             cron_expr, fire_at_utc, triggered_by))
    except Exception as e:
        logger.warning(f"tracker insert: {e}"); return 0


# ── Execution ─────────────────────────────────────────────────────────────
def _execute_run(run_id: int, label: str, tracker_id: int = 0):
    _coinit = False
    try:
        import pythoncom; pythoncom.CoInitialize(); _coinit = True
    except Exception: pass

    eng = _get_engine()
    with _active_runs_lock:
        _active_runs[run_id] = label

    try:
        with eng["DBManager"]() as db:
            eng["ConfigLoader"].load(db)
            if tracker_id:
                _update_tracker(db, tracker_id,
                                run_id=run_id, status="RUNNING",
                                actual_start_at=_now_utc())
            eng["RunLauncher"](db, run_id, eng["ConfigLoader"].get()).launch()

        final = _get_status(run_id)
        logger.info(f"[Scheduler] Run #{run_id} ({label}) → {final}")

        if tracker_id:
            with eng["DBManager"]() as db:
                st = "COMPLETED" if final in ("COMPLETED","PARTIAL") else "FAILED"
                _update_tracker(db, tracker_id, status=st,
                                completed_at=_now_utc(),
                                error_message=None if st=="COMPLETED" else final)
    except Exception as exc:
        logger.error(f"[Scheduler] Run #{run_id} error: {exc}", exc_info=True)
        if tracker_id:
            try:
                with eng["DBManager"]() as db:
                    _update_tracker(db, tracker_id, status="FAILED",
                                    completed_at=_now_utc(),
                                    error_message=str(exc)[:1000])
            except Exception: pass
    finally:
        with _active_runs_lock:
            _active_runs.pop(run_id, None)
        if _coinit:
            try:
                import pythoncom; pythoncom.CoUninitialize()
            except Exception: pass


def _get_status(run_id: int) -> str:
    try:
        eng = _get_engine()
        with eng["DBManager"]() as db:
            row = db.fetch_one("SELECT status FROM execution_runs WHERE id=%s",(run_id,))
            return row["status"] if row else "UNKNOWN"
    except Exception: return "UNKNOWN"


# ── Schedule firing ────────────────────────────────────────────────────────
def _fire_schedule(schedule_id: int, customer_id: int, env_types: str,
                   schedule_name: str, customer_name: str,
                   cron_expression: str,
                   start_at_utc=None):
    """
    Called by APScheduler on every cron tick.

    start_at gate: if start_at_utc is set, skip until now >= start_at.
    After the gate passes for the first time, start_at is cleared in the
    DB so future ticks are unconditional (FIX 2).
    """
    eng       = _get_engine()
    label     = f"{customer_name}/{env_types}"
    now_utc   = _now_utc()
    now_ist   = _utc_to_ist(now_utc)

    # FIX 1 applied: normalise start_at to naive UTC before comparing.
    # Previously, an aware datetime from Django USE_TZ=True would be
    # stripped with .replace(tzinfo=None) WITHOUT converting to UTC first,
    # leaving the value in IST-as-naive and causing a permanent +5:30 offset.
    if start_at_utc is not None:
        sa_utc_norm = _normalise_utc(start_at_utc)   # always naive UTC now
        if now_utc < sa_utc_norm:
            sa_ist = _utc_to_ist(sa_utc_norm)
            logger.info(
                f"[Scheduler] '{schedule_name}' — start_at="
                f"{sa_ist.strftime('%d-%b-%Y %I:%M %p IST')} not reached yet "
                f"(now={now_ist.strftime('%I:%M %p IST')}). Skipping.")
            return

    logger.info(
        f"[Scheduler] Firing '{schedule_name}' | {label} | "
        f"IST={now_ist.strftime('%d-%b-%Y %I:%M:%S %p')}")

    tracker_id = 0
    try:
        with eng["DBManager"]() as db:
            eng["ConfigLoader"].load(db)

            next_utc = _next_cron_utc(cron_expression, now_ist)
            next_ist = _utc_to_ist(next_utc)

            tracker_id = _create_tracker(
                db, schedule_id, customer_id, env_types,
                schedule_name, cron_expression, now_utc)

            run_id = db.insert_get_id(
                """INSERT INTO execution_runs
                   (schedule_id,customer_id,env_types,
                    run_type,status,triggered_by)
                   VALUES(%s,%s,%s,'SCHEDULED','PENDING','apscheduler')""",
                (schedule_id, customer_id, env_types))

            # FIX 2: Clear start_at after the gate has been passed once.
            # Without this, start_at_utc is re-checked every tick from the
            # job arg, but the job arg never updates — so if the DB value
            # was already NULL (a second run), the arg still holds the old
            # datetime and the gate would block again next tick.
            # Clearing it in the DB AND re-registering jobs every 5 min
            # (the existing refresh job) ensures the arg becomes None on
            # the next registration cycle.
            if start_at_utc is not None:
                db.execute(
                    "UPDATE schedules SET start_at=NULL WHERE id=%s AND start_at IS NOT NULL",
                    (schedule_id,))
                logger.info(
                    f"[Scheduler] '{schedule_name}' start_at gate passed — "
                    f"cleared start_at in DB.")

            db.execute(
                "UPDATE schedules SET last_run_at=%s, next_run_at=%s WHERE id=%s",
                (now_utc, next_utc, schedule_id))

            if tracker_id:
                _update_tracker(db, tracker_id,
                                run_id=run_id,
                                last_run_at=now_utc,
                                next_run_at=next_utc)

        logger.info(
            f"[Scheduler] Run #{run_id} | "
            f"last={now_ist.strftime('%d-%b-%Y %I:%M %p IST')} | "
            f"next={next_ist.strftime('%d-%b-%Y %I:%M %p IST')}")
        _execute_run(run_id, label, tracker_id)

    except Exception as exc:
        if isinstance(exc, _db_err()):
            logger.error(
                f"[Scheduler] DB down for '{schedule_name}'. "
                f"Will retry next tick. {exc}")
            return
        logger.error(f"[Scheduler] Error firing '{schedule_name}': {exc}", exc_info=True)
        if tracker_id:
            try:
                with eng["DBManager"]() as db:
                    _update_tracker(db, tracker_id, status="FAILED",
                                    error_message=str(exc)[:1000])
            except Exception: pass


# ── Schedule registration ─────────────────────────────────────────────────
def _load_schedules() -> list:
    try:
        eng = _get_engine()
        with eng["DBManager"]() as db:
            return db.fetch_all(
                """SELECT s.id,s.customer_id,s.env_types,s.cron_expression,
                          s.schedule_name,s.start_at,s.last_run_at,s.next_run_at,
                          c.name AS customer_name
                   FROM schedules s
                   JOIN customers c ON c.id=s.customer_id
                   WHERE s.is_active=1 AND c.is_active=1""")
    except Exception as exc:
        logger.error(f"Cannot load schedules: {exc}"); return []


def _register_jobs(scheduler):
    """
    Register all active schedules.
    CronTrigger timezone = Asia/Kolkata (IST).
    start_at_utc is normalised via _normalise_utc() before being passed
    as a job arg, so _fire_schedule always receives naive UTC or None.
    """
    from apscheduler.triggers.cron import CronTrigger

    for job in scheduler.get_jobs():
        if job.id.startswith("sched_"):
            try: job.remove()
            except Exception: pass

    schedules = _load_schedules()
    logger.info(f"[Scheduler] Registering {len(schedules)} schedule(s) | tz=Asia/Kolkata")

    for s in schedules:
        job_id = f"sched_{s['id']}"
        try:
            parts = s["cron_expression"].strip().split()
            if len(parts) != 5:
                logger.error(f"  ✗ [{s['id']}] Bad cron '{s['cron_expression']}'"); continue

            trigger = CronTrigger(
                minute=parts[0], hour=parts[1], day=parts[2],
                month=parts[3], day_of_week=parts[4],
                timezone="Asia/Kolkata")

            # FIX 1 applied at registration: normalise before storing in arg.
            # If DB returns an aware datetime (Django USE_TZ=True), we convert
            # to naive UTC here so the job arg is always consistent.
            raw_start_at = s.get("start_at")
            start_at_utc = _normalise_utc(raw_start_at)   # naive UTC or None

            if start_at_utc:
                sa_ist = _utc_to_ist(start_at_utc)
                sa_str = sa_ist.strftime("%d-%b-%Y %I:%M %p IST")
            else:
                sa_str = "Next cron tick (no start_at)"

            nxt = s.get("next_run_at")
            nxt_str = _utc_to_ist(nxt).strftime("%I:%M %p IST") if nxt else "TBD"

            scheduler.add_job(
                _fire_schedule, trigger=trigger, id=job_id,
                name=s["schedule_name"],
                args=[s["id"], s["customer_id"], s["env_types"],
                      s["schedule_name"], s["customer_name"],
                      s["cron_expression"], start_at_utc],   # normalised
                max_instances=1, coalesce=True,
                replace_existing=True, misfire_grace_time=300)

            logger.info(
                f"  ✓ [{s['id']}] {s['schedule_name']} | "
                f"cron={s['cron_expression']} | start_at={sa_str} | next≈{nxt_str}")
        except Exception as exc:
            logger.error(f"  ✗ [{s['id']}] {s['schedule_name']}: {exc}")


# ── Lifecycle ─────────────────────────────────────────────────────────────
def get_scheduler():
    return _scheduler


def start_scheduler():
    global _scheduler
    with _scheduler_lock:
        if _scheduler and _scheduler.running:
            logger.info("Scheduler already running"); return _scheduler
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.executors.pool import ThreadPoolExecutor
            _scheduler = BackgroundScheduler(
                executors={"default": ThreadPoolExecutor(max_workers=20)},
                job_defaults={"coalesce": True, "max_instances": 1},
                timezone="Asia/Kolkata")
            _register_jobs(_scheduler)
            _scheduler.add_job(
                lambda: _register_jobs(_scheduler),
                "interval", minutes=5,
                id="refresh_schedules", name="DB schedule refresh",
                replace_existing=True)
            _scheduler.start()
            logger.info("APScheduler started | tz=Asia/Kolkata | pool=20")
            return _scheduler
        except ImportError:
            logger.error("APScheduler not installed"); return None
        except Exception as exc:
            logger.error(f"Scheduler start failed: {exc}", exc_info=True); return None


def stop_scheduler():
    global _scheduler
    with _scheduler_lock:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown(wait=False)
            logger.info("APScheduler stopped")
        _scheduler = None


def restart_scheduler():
    stop_scheduler(); return start_scheduler()


def scheduler_status() -> dict:
    now_ist_str = _now_ist().strftime("%d-%b-%Y %I:%M:%S %p")
    if _scheduler is None or not _scheduler.running:
        return {"running": False, "job_count": 0, "jobs": [],
                "active_runs": [], "active_run_count": 0,
                "timezone": "Asia/Kolkata", "server_time_ist": now_ist_str}

    jobs = []
    for job in _scheduler.get_jobs():
        if not job.id.startswith("sched_"): continue
        nxt = job.next_run_time
        if nxt:
            nxt_ist = _utc_to_ist(nxt) if nxt.tzinfo else nxt
            nxt_str = nxt_ist.strftime("%d-%b-%Y %I:%M:%S %p IST")
        else:
            nxt_str = "—"
        jobs.append({"id": job.id, "name": job.name, "next_run": nxt_str})

    with _active_runs_lock:
        active = [{"run_id": rid, "label": lbl}
                  for rid, lbl in _active_runs.items()]

    return {"running": True, "job_count": len(jobs), "jobs": jobs,
            "active_runs": active, "active_run_count": len(active),
            "timezone": "Asia/Kolkata", "server_time_ist": now_ist_str}


# ── Manual run helpers ────────────────────────────────────────────────────
def run_now_sync(schedule_id: int) -> Optional[int]:
    eng = _get_engine()
    try:
        with eng["DBManager"]() as db:
            eng["ConfigLoader"].load(db)
            s = db.fetch_one(
                "SELECT s.*,c.name AS cname FROM schedules s "
                "JOIN customers c ON c.id=s.customer_id WHERE s.id=%s",
                (schedule_id,))
            if not s: return None
            now_utc  = _now_utc()
            now_ist  = _utc_to_ist(now_utc)
            next_utc = _next_cron_utc(s["cron_expression"], now_ist)
            run_id = db.insert_get_id(
                """INSERT INTO execution_runs
                   (schedule_id,customer_id,env_types,
                    run_type,status,triggered_by)
                   VALUES(%s,%s,%s,'MANUAL','PENDING','admin:run_now')""",
                (s["id"], s["customer_id"], s["env_types"]))
            db.execute(
                "UPDATE schedules SET last_run_at=%s, next_run_at=%s WHERE id=%s",
                (now_utc, next_utc, schedule_id))
            tracker_id = _create_tracker(
                db, schedule_id, s["customer_id"], s["env_types"],
                s["schedule_name"], s["cron_expression"],
                now_utc, triggered_by="admin:run_now")
        label = f"{s['cname']}/{s['env_types']}"
        t = threading.Thread(target=_execute_run, args=(run_id, label, tracker_id),
                             daemon=True, name=f"hc_run_{run_id}")
        t.start()
        return run_id
    except Exception as exc:
        logger.error(f"run_now_sync: {exc}", exc_info=True); return None


def run_customer_now(customer_id: int, env_types: str) -> Optional[int]:
    eng = _get_engine()
    try:
        with eng["DBManager"]() as db:
            eng["ConfigLoader"].load(db)
            run_id = db.insert_get_id(
                """INSERT INTO execution_runs
                   (customer_id,env_types,run_type,status,triggered_by)
                   VALUES(%s,%s,'MANUAL','PENDING','admin:manual')""",
                (customer_id, env_types))
            cust  = db.fetch_one("SELECT name FROM customers WHERE id=%s",(customer_id,))
            cname = cust["name"] if cust else str(customer_id)
        label = f"{cname}/{env_types}"
        t = threading.Thread(target=_execute_run, args=(run_id, label, 0),
                             daemon=True, name=f"hc_run_{run_id}")
        t.start(); return run_id
    except Exception as exc:
        logger.error(f"run_customer_now: {exc}", exc_info=True); return None
