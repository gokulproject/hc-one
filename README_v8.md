# Oracle DB Connection Checker — README (v8)

A scheduled/manual job that checks Oracle database connectivity for every
customer's DEV/VAL/PRD environment, records the result, builds an Excel
report, and emails it out.

It reads customer, environment, server and Oracle DB details from the
**health_check** schema — **read only, nothing is ever written there.**
All of this project's own data (config, run history, results, logs) lives
in its own schema: **dbconnection_checker**.

There is **no WMI / Windows server check** in this version. The only check
performed is a direct Oracle database connection using the `oracledb`
Python driver.

---

## 1. What It Does — End to End

```
1.  Start
2.  Connect to health_check DB (read-only) and dbconnection_checker DB
    Load app_config + email_config
3.  Load active customers from health_check.Customers
    (optionally filtered by app_config.config_customers)
4.  For each customer, load active environments from health_check.Environments
    (DEV / VAL / PRD — optionally filtered by app_config.config_envs)
5.  For each environment, map the DB Server
    (health_check.Servers WHERE ServerName='DB Server')
    -> gets server_id, server_name, host (IP/hostname)
6.  Load Oracle DB(s) for that server from health_check.OracleDatabase
    (filtered by app_config.config_dbtype, e.g. 'FMW')
7.  For each Oracle DB: connect using oracledb
      - Thick mode if app_config.oracle_dll_path is set and valid
      - Thin mode automatically if not (a WARNING is logged)
      - No connect timeout is configured — OS/driver default applies
8.  Classify any connection error into one of three types:
      TCP_TIMEOUT | PASSWORD | UNKNOWN
9.  Write one row per Oracle DB checked to process_db_connection_result
10. Generate an Excel report (with column filters)
11. Email the report (plain text body + Excel attachment)
12. Update process_run with final status, end time, Excel path, log path,
    email status
```

If anything stops the run partway — a crash, or you press **Ctrl+C** to
stop it manually — the run is still marked **FAILED** and
`execution_end_at` is recorded, so you always know when and how a run ended.

---

## 2. Two Databases Involved

| Schema | Access | Purpose |
|---|---|---|
| `health_check` | **READ ONLY** | Source of truth: Customers, Environments, Servers, OracleDatabase. Never written to. |
| `dbconnection_checker` | Read/Write | This project's own schema: configuration, run history, results, audit log. |

Both are configured in **`config.py`** (or via environment variables — see §10).

---

## 3. Folder Structure

```
dbconnection_checker_v8/
├── main.py                      Entry point — run this
├── process_manager.py           Orchestrates the whole run, start/end times, email, excel
├── checker.py                   Customer -> Env -> Server -> Oracle DB loop + error classification
├── config.py                    DB credentials for health_check + dbconnection_checker
├── config_loader.py             Loads app_config + email_config rows from the DB into objects
├── schema.sql                   Fresh schema — run once on a new database
├── schema_alter.sql             Alter script — run on an EXISTING v7/early-v8 schema
├── requirements.txt             Python dependencies
├── README.md                    This file
│
├── Common/
│   ├── db_manager.py             MySQL connection wrapper (pymysql, retries, context manager)
│   ├── email_manager.py          Builds & sends the report email
│   ├── logger.py                 Logging setup (console + file)
│   ├── crypto.py                 AES-256-CBC decrypt for Oracle passwords
│   └── __init__.py
│
├── ExcelCreation/
│   ├── excel_manager.py          Builds the .xlsx report with filters & colour coding
│   └── __init__.py
│
├── Excel/                        Generated reports land here (path is configurable)
├── Logs/                         Log files land here (path is configurable)
└── oracledb_dll/                 Optional: put Oracle Instant Client files here
```

---

## 4. Database Schema (`dbconnection_checker`)

Run **`schema.sql`** once to create everything fresh, or **`schema_alter.sql`**
if you already have an older v7/v8 schema and want to upgrade it in place
(renames `started_at`/`completed_at`, adds the new email/path columns).

### 4.1 `app_config` — key/value settings

| config_key | Default | Meaning |
|---|---|---|
| `excel_path` | `Excel` | Folder where generated `.xlsx` reports are saved |
| `log_path` | `Logs` | Folder where log files are written |
| `oracle_dll_path` | *(empty)* | Oracle Instant Client folder. Empty = thin mode (a WARNING is logged). Set to enable thick mode. |
| `config_customers` | *(empty)* | Comma-separated customer names to restrict the run to. Empty = **all active customers**. |
| `config_envs` | *(empty)* | Comma-separated env types to restrict to, e.g. `PRD,VAL`. Empty = **all** (`DEV,VAL,PRD`). |
| `config_dbtype` | `FMW` | Comma-separated `DatabaseType` value(s) to check, matched against `health_check.OracleDatabase.DatabaseType`. Change this (no code change needed) when you need to check a different DB type, e.g. `OAM`, or `FMW,OAM` for both. |
| `report_label` | `FMW` | Display label used in the Excel title, filename, and email subject. Update this together with `config_dbtype`. |

Every key is a separate row. Update with plain SQL, e.g.:
```sql
UPDATE app_config SET config_value='PRD' WHERE config_key='config_envs';
```

### 4.2 `email_config` — SMTP + recipients

| Column | Meaning |
|---|---|
| `smtp_host`, `smtp_port` | SMTP server address |
| `smtp_user`, `smtp_pass` | SMTP login credentials |
| `use_tls` | `1` to use STARTTLS, `0` to skip |
| `from_addr` | Sender address |
| `to_addr` | Comma-separated TO recipients (**required** — email is skipped if empty) |
| `cc_addr` | Comma-separated CC recipients (optional) |
| `bcc_addr` | Comma-separated BCC recipients (optional) |
| `is_active` | Only the row with `is_active=1` (lowest id) is used |

### 4.3 `process_run` — one row per execution

| Column | Meaning |
|---|---|
| `id` | Run ID — used in Excel filename, email subject, log filename |
| `status` | `RUNNING` → one of `COMPLETED` / `PARTIAL` / `FAILED` / `SKIPPED` / `NO_DATA` |
| `db_type` | The `config_dbtype` value(s) used for this run |
| `execution_start_at` | Timestamp the run started |
| `execution_end_at` | Timestamp the run ended — set in every case: success, error, or **manual stop (Ctrl+C)** |
| `total_envs` | Total Oracle DB rows processed this run |
| `success_count` | Count of `COMPLETED` (DB connected) |
| `fail_count` | Count of `FAILED` (DB not connected) |
| `skipped_count` | Count of `SKIPPED` (no env/server/db found) |
| `excel_path` | Full path to the Excel file generated this run |
| `log_file` | Full path to the log file for this run |
| `email_sent_status` | `SENT` / `FAILED` / `SKIPPED` |
| `email_sent_error` | Error text if the email failed to send |
| `created_at` | Row creation timestamp |

**Status meaning:**
- `COMPLETED` — every DB connected successfully
- `PARTIAL` — some connected, some failed/skipped
- `FAILED` — none connected (or a fatal/unexpected error, or manual stop)
- `SKIPPED` — nothing failed, but nothing connected either (all rows were skipped)
- `NO_DATA` — no Oracle DB rows were found to check at all

### 4.4 `process_db_connection_result` — one row per Oracle DB checked

| Column | Meaning |
|---|---|
| `run_id` | FK to `process_run.id` |
| `customer_id`, `customer_name` | From `health_check.Customers` |
| `env_type` | `DEV` / `VAL` / `PRD` |
| `server_id`, `server_name`, `server_host` | DB Server mapping from `health_check.Servers` |
| `db_name` | Oracle DB name (`OracleDatabase.OrdName`) |
| `db_type` | The `DatabaseType` value matched (e.g. `FMW`) |
| `db_host`, `db_port`, `db_service` | Connection details used to build the DSN |
| `connection_status` | `COMPLETED` (connected) / `FAILED` (not connected) / `SKIPPED` |
| `error_message` | **Full** Oracle error text — never truncated |
| `error_type` | `TCP_TIMEOUT` / `PASSWORD` / `UNKNOWN` / empty if connected |
| `duration_ms` | How long the connection attempt took |
| `checked_at` | Timestamp (UTC) the check ran |

**`SKIPPED` rows are always written** so nothing is silently dropped. A row is `SKIPPED` when:
1. No active environment matches `config_envs` for a customer
2. No active `'DB Server'` row exists in `Servers` for an environment
3. The server lookup itself errors out
4. No active `OracleDatabase` row matches `config_dbtype` for that server

### 4.5 `process_run_log` — step-by-step audit trail

| Column | Meaning |
|---|---|
| `run_id` | FK to `process_run.id` |
| `step_no` | 0–10 (0 = unexpected/fatal error) |
| `step_name` | e.g. `CONNECT_DATABASES`, `MAP_DB_SERVER`, `DB_CONNECTION_CHECK`, `EMAIL` |
| `status` | `STARTED` / `COMPLETED` / `FAILED` / `SKIPPED` |
| `message` | Free-text detail |
| `customer`, `env_type` | Context, where applicable |
| `logged_at` | Timestamp |

Use this table to trace exactly what happened for a specific customer/environment in a specific run.

---

## 5. Error Classification

Every failed Oracle connection is automatically classified, based on
keyword matching against the raw Oracle/TNS error text:

| `error_type` | Matched against | Meaning |
|---|---|---|
| `TCP_TIMEOUT` | `ORA-12170`, `ORA-12541`, `ORA-12543`, `ORA-12547`, `TNS-12535`, `TNS-12606`, `TNS-12609`, "timed out", "timeout", "no listener", "network adapter" | Network/listener unreachable or connection timed out |
| `PASSWORD` | `ORA-28001`–`28003`, `ORA-28009`, `ORA-28000`, `ORA-01017`, `ORA-01005`, "invalid username/password", "logon denied", "password expired", "account is locked" | Password expired, invalid credentials, or account locked |
| `UNKNOWN` | Anything else | Any other Oracle error |

`error_type` is empty when `connection_status = COMPLETED`.

---

## 6. Oracle Client Mode (Thick vs Thin)

The tool uses the **`oracledb`** Python package and decides its mode at
startup, based on `app_config.oracle_dll_path`:

| `oracle_dll_path` value | Result |
|---|---|
| Empty / not set | **Thin mode.** A WARNING is logged: *"Oracle DLL path not set in app_config — running in THIN mode"*. Connections still work fine — thin mode is fully supported by `oracledb`. |
| Set, but folder doesn't exist | **Thin mode**, with a WARNING: *"Oracle DLL path not found: \<path\> — running in THIN mode"* |
| Set, folder exists but is empty | **Thin mode**, with a WARNING: *"Oracle DLL folder is empty: \<path\> — running in THIN mode"* |
| Set, folder exists and has files | **Thick mode.** Logged as INFO: *"Oracle THICK mode initialised \| path = \<path\>"* |

There is **no connection timeout configuration** — by design, the OS/driver
default timeout behaviour applies as-is; no `oracle_timeout` setting exists
in this version.

---

## 7. Excel Report

One file is generated per run, saved to the `excel_path` folder.

**Filename:** `{REPORT_LABEL}_CONNECTION_CHECKS_DD-MM-YY-{RUN_ID}.xlsx`
e.g. `FMW_CONNECTION_CHECKS_15-06-26-42.xlsx`

### Layout
- **Row 1** — Title bar: report label, run ID, timestamp
- **Row 2** — Summary bar: total / connected / not connected / skipped / error type counts + an overall verdict (`ALL CONNECTED`, `PARTIAL`, `NO SUCCESSFUL CONNECTIONS`)
- **Row 3** — Column headers, **with a filter dropdown on every single column**
- **Row 4 onward** — One row per Oracle DB checked, colour-coded by status (green = connected, red = not connected, amber = skipped)

### Columns (14 total — all filterable)

| # | Column | Source |
|---|---|---|
| 1 | `#` | Row number |
| 2 | Customer | `customer_name` |
| 3 | Environment | `env_type` |
| 4 | Server Name | `server_name` |
| 5 | Server ID | `server_id` |
| 6 | Server Host | `server_host` |
| 7 | DB Name | `db_name` |
| 8 | DB Type | `db_type` |
| 9 | DSN (Host:Port/Service) | Built from `db_host:db_port/db_service` |
| 10 | DB Connection Status | `connection_status`, displayed as **DB CONNECTED** / **DB NOT CONNECTED** / **SKIPPED** |
| 11 | Duration (ms) | `duration_ms` |
| 12 | Error | `error_message` (full text, never truncated) |
| 13 | Error Type | `error_type`, displayed as a readable label |
| 14 | Checked At (IST) | `checked_at` converted to IST |

The header row is frozen (`freeze_panes = "A4"`) so headers stay visible
while scrolling.

---

## 8. Email Report

Sent after the Excel file is generated. If `to_addr` in `email_config` is
empty, the email step is skipped entirely (logged, not a failure).

**Subject:**
```
{REPORT_LABEL} DB Connection Check — {Customer1, Customer2, ...} — Run #{run_id}
```

**Body (plain text, kept short and clean):**
```
Hi Team,

Please find the FMW Oracle DB connection check report attached.

Run ID       : 42
Started      : 15-Jun-2026 10:30:00 AM IST
Ended        : 15-Jun-2026 10:35:12 AM IST

Results
-------
Total        : 24
Connected    : 18
Not Connected: 4
Skipped      : 2

Error Breakdown
---------------
TCP Timeout  : 2
Password     : 1
Unknown      : 1

Overall      : Some connections failed or were skipped. Please review the report.

Thanks & Regards,
RPA Team
```

The "Error Breakdown" section is only included if there are failures.
The Excel report is attached as-is.

---

## 9. health_check Schema — What Is Read (Read-Only Reference)

| Table | Columns used | Purpose |
|---|---|---|
| `Customers` | `CustomerID`, `CustomerName`, `Status` | List of active customers |
| `Environments` | `EnvID`, `CustomerID`, `ServerType`, `Status` | DEV/VAL/PRD environments per customer |
| `Servers` | `ServerID`, `EnvID`, `ServerName`, `Host`, `Status` | DB Server IP/hostname mapping (filtered to `ServerName='DB Server'`) |
| `OracleDatabase` | `dbid`, `ServerID`, `OrdName`, `Port`, `User`, `Password`, `DatabaseType`, `Status` | Oracle DB connection credentials |

`Environments.ServerUser` / `ServerPwd` are **not used** — there is no
server-level (WMI) login in this version, only the Oracle DB connection
itself.

`OracleDatabase.Password` is decrypted using AES-256-CBC via
`Common/crypto.py`, using the same key (`ENC_KEY`) as the health_check
project. If decryption fails for any reason, the raw value is used as-is
(assumed already plain text) — a warning is logged, the run still proceeds.

---

## 10. Setup

### 10.1 Install dependencies
```
pip install -r requirements.txt
```
No WMI / pywin32 packages are required.

### 10.2 Create the schema
Fresh install:
```
mysql -u root -p < schema.sql
```
Upgrading an existing v7/early-v8 schema instead:
```
mysql -u root -p < schema_alter.sql
```

### 10.3 Configure database credentials — `config.py`
```python
HC_DB = {
    "host":     "your_hc_db_host",
    "port":     3306,
    "user":     "your_user",
    "password": "your_password",
    "database": "health_check",
}

FMWCHECKS_DB = {
    "host":     "your_checker_db_host",
    "port":     3306,
    "user":     "your_user",
    "password": "your_password",
    "database": "dbconnection_checker",
}

ENC_KEY = "3cb950cf5dd21c4d4e3618ddf3f317b2"   # must match health_check's key, 32 chars
```
Or set via environment variables instead of editing the file:
```
HC_DB_HOST, HC_DB_PORT, HC_DB_USER, HC_DB_PASS, HC_DB_NAME
FMW_DB_HOST, FMW_DB_PORT, FMW_DB_USER, FMW_DB_PASS, FMW_DB_NAME
HC_ENC_KEY
```

### 10.4 Configure `app_config`
```sql
USE dbconnection_checker;

-- Optional: enable thick mode
UPDATE app_config SET config_value='C:\oracle\instantclient_21_3'
WHERE config_key='oracle_dll_path';

-- Optional: restrict to specific customers/envs
UPDATE app_config SET config_value='CustomerA,CustomerB' WHERE config_key='config_customers';
UPDATE app_config SET config_value='PRD'                 WHERE config_key='config_envs';

-- Required: the DatabaseType to check
UPDATE app_config SET config_value='FMW' WHERE config_key='config_dbtype';
UPDATE app_config SET config_value='FMW' WHERE config_key='report_label';
```

### 10.5 Configure `email_config`
```sql
UPDATE email_config
SET smtp_host='smtp.yourcompany.com',
    smtp_port=587,
    smtp_user='alerts@yourcompany.com',
    smtp_pass='yourpassword',
    from_addr='alerts@yourcompany.com',
    to_addr='team@yourcompany.com',
    cc_addr=''
WHERE is_active=1;
```

### 10.6 Run it
```
python main.py
```

---

## 11. What Happens If Something Is Missing

The tool is defensive at every step — missing data produces a logged,
recorded `SKIPPED`/`FAILED` outcome rather than a silent crash, **except**
for the two fatal pre-checks below.

| Missing / wrong | What happens |
|---|---|
| `dbconnection_checker` DB unreachable | **Fatal** — logged as CRITICAL, run aborts before a `process_run` row is even created (there's nowhere to record it) |
| `health_check` DB unreachable | **Fatal** — logged as CRITICAL, run aborts (the `process_run` row created in step 2 is left at status `RUNNING` with no end time, since the abort happens before the try/finally block) |
| `app_config` table missing/empty | Falls back to defaults: `excel_path=Excel`, `log_path=Logs`, `config_dbtype=['FMW']`, `report_label=FMW`. A warning is logged. |
| `email_config` has no active row | Email step is skipped (`email_sent_status=SKIPPED`); rest of the run continues and the Excel report is still produced |
| `email_config.to_addr` is empty | Email step is skipped, logged as a warning; not treated as a run failure |
| No active customers found at all | Run ends immediately with no rows; logged as `IDENTIFY_CUSTOMERS — SKIPPED` |
| No active environment for a customer (after `config_envs` filter) | One `SKIPPED` row is written for that customer with the reason in `error_message`; processing continues to the next customer |
| No `'DB Server'` row in `Servers` for an environment | One `SKIPPED` row written for that customer/env; processing continues |
| Server lookup query itself errors | One `SKIPPED` row written with the error in `error_message`; processing continues |
| No `OracleDatabase` row matches `config_dbtype` for a server | One `SKIPPED` row written; processing continues |
| Oracle connection fails (any reason) | One `FAILED` row written with full error text + classified `error_type`; processing continues to the next DB |
| `oracle_dll_path` empty/invalid | Falls back to thin mode automatically; a WARNING is logged but the run is **not** affected |
| Excel generation fails | Logged as ERROR; the run continues to the email step (which will simply have no attachment) and finishes with whatever final status the DB results warrant |
| Email sending fails (SMTP error, auth, etc.) | Logged as ERROR; `email_sent_status=FAILED` with `email_sent_error` populated; the run itself still finishes normally with its DB-check-based status |
| Password decryption fails for an Oracle DB | Falls back to using the raw stored value as-is; a warning is logged; the connection attempt still proceeds (it will simply fail with `PASSWORD`/`UNKNOWN` type if the raw value isn't valid) |
| Process is stopped manually (Ctrl+C) | Caught explicitly — run is marked `status=FAILED`, `execution_end_at` is recorded, and a log entry `"Stopped manually (Ctrl+C) — run marked FAILED"` is written before re-raising so the process exits cleanly |
| Any other unexpected exception mid-run | Caught by a general handler — run is marked `status=FAILED`, full traceback logged, `execution_end_at` still recorded via the `finally` block |

In short: **the `finally` block in `process_manager.py` always runs**
(except for the two fatal pre-checks before the run record exists), so
`execution_end_at`, counts, Excel path, log path and email status are
always written for any run that got far enough to be created.

---

## 12. Changing What Gets Checked (No Code Changes)

To switch the DB type being checked, e.g. from `FMW` to `OAM`:
```sql
UPDATE app_config SET config_value='OAM' WHERE config_key='config_dbtype';
UPDATE app_config SET config_value='OAM' WHERE config_key='report_label';
```
To check more than one type in the same run:
```sql
UPDATE app_config SET config_value='FMW,OAM' WHERE config_key='config_dbtype';
UPDATE app_config SET config_value='FMW/OAM' WHERE config_key='report_label';
```
No code changes are required for either case — `checker.py` reads
`config_dbtype` dynamically on every run.

---

## 13. Useful Queries

```sql
-- Latest 5 runs
SELECT id, status, db_type, total_envs, success_count, fail_count, skipped_count,
       execution_start_at, execution_end_at, email_sent_status, excel_path, log_file
FROM process_run ORDER BY id DESC LIMIT 5;

-- Full results of the latest run
SELECT customer_name, env_type, server_name, server_id, server_host,
       db_name, db_type, CONCAT(db_host,':',db_port,'/',db_service) AS dsn,
       connection_status, error_type, error_message, duration_ms, checked_at
FROM process_db_connection_result
WHERE run_id = (SELECT MAX(id) FROM process_run)
ORDER BY customer_name, env_type;

-- Error type breakdown for the latest run
SELECT error_type, COUNT(*) AS cnt
FROM process_db_connection_result
WHERE run_id = (SELECT MAX(id) FROM process_run) AND connection_status='FAILED'
GROUP BY error_type;

-- All failures across all runs this week
SELECT r.id AS run_id, d.customer_name, d.env_type, d.db_name,
       d.error_type, d.error_message, d.checked_at
FROM process_db_connection_result d
JOIN process_run r ON r.id = d.run_id
WHERE d.connection_status = 'FAILED'
  AND d.checked_at >= NOW() - INTERVAL 7 DAY
ORDER BY d.checked_at DESC;

-- Step-by-step audit trail for a specific run
SELECT step_no, step_name, status, customer, env_type, message, logged_at
FROM process_run_log
WHERE run_id = 42
ORDER BY id;

-- Runs that ended in FAILED status (crashes or manual stops)
SELECT id, status, execution_start_at, execution_end_at,
       TIMESTAMPDIFF(SECOND, execution_start_at, execution_end_at) AS duration_sec
FROM process_run
WHERE status = 'FAILED'
ORDER BY id DESC;
```
