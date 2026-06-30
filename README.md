"""
Common/email_manager.py
=======================
Send DB connection check report email with HTML body and Excel attachment.
"""
import logging
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger("checker.email")


class EmailManager:

    def __init__(self, email_cfg):
        self.cfg = email_cfg

    # ── Send ─────────────────────────────────────────────────────────────────

    def send_report(self, subject, html_body, excel_path=None, run_id=0):
        """Send email with HTML body and Excel attachment.
        Returns (success: bool, error_msg: str|None).
        """
        if not self.cfg.to_addrs:
            log.warning("  No TO address configured — email skipped")
            return False, "No TO address configured"

        try:
            msg            = MIMEMultipart("mixed")
            msg["Subject"] = subject
            msg["From"]    = self.cfg.from_addr
            msg["To"]      = ", ".join(self.cfg.to_addrs)
            if self.cfg.cc_addrs:
                msg["Cc"]  = ", ".join(self.cfg.cc_addrs)

            # HTML body
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            # Excel attachment
            if excel_path and os.path.exists(excel_path):
                with open(excel_path, "rb") as f:
                    part = MIMEBase(
                        "application",
                        "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(excel_path)}")
                msg.attach(part)

            all_to = self.cfg.to_addrs + self.cfg.cc_addrs + self.cfg.bcc_addrs
            with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port, timeout=30) as smtp:
                if self.cfg.use_tls:
                    smtp.starttls()
                if self.cfg.smtp_user:
                    smtp.login(self.cfg.smtp_user, self.cfg.smtp_pass)
                smtp.sendmail(self.cfg.from_addr, all_to, msg.as_string())

            log.info(f"  Email sent  |  run_id={run_id}  |  to={self.cfg.to_addrs}")
            return True, None

        except Exception as e:
            log.error(f"  Email failed: {e}")
            return False, str(e)[:500]

    # ── Subject ──────────────────────────────────────────────────────────────

    @staticmethod
    def build_subject(results, run_id, db_label="FMW"):
        customers = list(dict.fromkeys(
            r.get("customer_name", "") for r in results if r.get("customer_name")))
        cust_str = ", ".join(customers) if customers else "All Customers"
        return f"{db_label} DB Connection Check — {cust_str} — Run #{run_id}"

    # ── HTML body ─────────────────────────────────────────────────────────────

    @staticmethod
    def build_body(results, run_id, started, ended,
                   total, success, failed, skipped,
                   tcp_err=0, pwd_err=0, unk_err=0, db_label="FMW"):
        """
        Build a short, simple HTML email body:
        Line 1 — status summary (counts)
        Line 2 — overall result line
        Plus Run ID and Date/Time.
        Full details are in the attached Excel report.
        """

        if total == 0:
            result_color = "#856404"
            result_text  = "No active environments or databases were found for this run."
        elif failed == 0 and skipped == 0:
            result_color = "#155724"
            result_text  = "All database connections were successful."
        elif success == 0:
            result_color = "#721c24"
            result_text  = "No connections were successful. Please review the attached report urgently."
        else:
            result_color = "#856404"
            result_text  = "Some connections failed or were skipped. Please review the attached report."

        html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Calibri,Arial,sans-serif;font-size:14px;color:#222;
             line-height:1.6;max-width:680px;margin:0;padding:24px;">

  <p style="margin:0 0 14px 0;">Hi Team,</p>

  <p style="margin:0 0 4px 0;">
    Total: {total} &nbsp;|&nbsp; Connected: {success} &nbsp;|&nbsp;
    Not Connected: {failed} &nbsp;|&nbsp; Skipped: {skipped}
  </p>
  <p style="margin:0 0 14px 0;font-weight:600;color:{result_color};">
    {result_text}
  </p>

  <p style="margin:0 0 4px 0;color:#555;">
    Run ID: <strong>#{run_id}</strong> &nbsp;|&nbsp; Date &amp; Time: <strong>{started}</strong>
  </p>

  <p style="margin:18px 0 0 0;">
    Please refer to the attached {db_label} Excel report for full details.
  </p>

  <p style="margin:24px 0 0 0;">
    Thanks &amp; Regards,<br>
    <strong>RPA Team</strong>
  </p>

</body>
</html>"""

        return html














        ===========================
"""
ExcelCreation/excel_manager.py
==============================
Generate Excel report for Oracle DB Connection Check.

Sheet 1 — DB Connection Check (16 columns):
  # | Customer | Environment | Server Name | Server ID | Server Host |
  DB Name | DB Type | DSN | DB Connection Status |
  Duration (ms) | Error | Error Type | Checked At (IST) |
  Expired (Action Required) | Reason (if yes)

Dynamic sheets — one per Customer + Environment (FMW db_type only):
  Sheet name : "CustomerName - ENV"
  Row 2      : FMW query read from app_config
  Columns    : Server Host / DB Name / DB Type | USERNAME | ACCOUNT_STATUS |
               EXPIRY_DATE | Action Required

Expiry / Action Required rules:
  FMW + CONNECTED + query ran   + date found   → Yes / Yes | Reason: date
  FMW + CONNECTED + query ran   + no date      → No  / No  | Reason: blank
  FMW + CONNECTED + query error / not configured → blank   | Reason: blank
  FMW + NOT CONNECTED or SKIPPED               → blank     | Reason: blank
  Non-FMW db_type (any status)                 → blank     | Reason: blank
"""
import logging
import os
from datetime import datetime, timedelta

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

log      = logging.getLogger("checker.excel")
IST      = timedelta(hours=5, minutes=30)
LAST_COL = 15   # 13 original + 2 expiry columns (Server ID column removed from Sheet 1)

# ── Palette ───────────────────────────────────────────────────────────────────
HDR_DARK   = "1565C0"
HDR_MED    = "0D47A1"
HDR_GREEN  = "1B5E20"
SUMM_BG    = "E3F2FD"
GREEN_BG   = "E8F5E9";  GREEN_BG2 = "F1F8E9"
RED_BG     = "FFEBEE";  RED_BG2   = "FFF5F5"
AMBER_BG   = "FFF8E1";  AMBER_BG2 = "FFFDE7"
C_GREEN    = "2E7D32"
C_RED      = "C62828"
C_AMBER    = "E65100"
C_BLUE     = "0D47A1"
C_WHITE    = "FFFFFF"
C_DARK_RED = "B71C1C"
C_DARK_GRN = "1B5E20"

STATUS_LABEL = {
    "COMPLETED": "DB CONNECTED",
    "FAILED":    "DB NOT CONNECTED",
    "SKIPPED":   "SKIPPED",
}
STATUS_COLOR = {
    "COMPLETED": C_GREEN,
    "FAILED":    C_RED,
    "SKIPPED":   C_AMBER,
}
ERR_LABEL = {
    "TCP_TIMEOUT": "TCP Connection Timeout",
    "PASSWORD":    "Password Expired / Invalid Username or Password",
    "UNKNOWN":     "Unknown / Other Error",
    "":            "",
}
ERR_COLOR = {"TCP_TIMEOUT": C_RED, "PASSWORD": C_AMBER, "UNKNOWN": C_BLUE, "": ""}

_DEFAULT_FMW_QUERY = (
    "SELECT USERNAME, ACCOUNT_STATUS, EXPIRY_DATE "
    "FROM DBA_USERS WHERE USERNAME LIKE '%OAM%'"
)


def _border():
    s = Side(style="thin", color="BDBDBD")
    return Border(left=s, right=s, top=s, bottom=s)


def _ist(dt) -> str:
    return (dt + IST).strftime("%d-%b-%Y %I:%M:%S %p IST") if dt else ""


def _col(n):
    return get_column_letter(n)


# ── Expiry helpers ────────────────────────────────────────────────────────────

def _is_expiry_set(val) -> bool:
    # True only when EXPIRY_DATE contains an actual date — not null/none/empty
    if val is None:
        return False
    return str(val).strip().upper() not in ("", "NULL", "NONE")


def _compute_expiry_status(fmw_query_rows: list) -> tuple:
    # Called only for rows where query ran successfully — returns (expired, action, reason)
    if not fmw_query_rows:
        return "No", "No", ""  # query ran but zero OAM accounts found
    for row in fmw_query_rows:
        expiry = row.get("EXPIRY_DATE", "")
        if _is_expiry_set(expiry):
            return "Yes", "Yes", str(expiry).strip()  # first account with a date wins
    return "No", "No", ""  # all accounts have empty/null expiry


def _build_expiry_map(results: list) -> dict:
    # Builds (customer_name, env_type) → (expired, action, reason)
    # Only includes FMW entries where connection COMPLETED and query actually ran
    env_rows: dict = {}
    for r in results:
        if r.get("db_type", "").upper() != "FMW":
            continue
        if r.get("connection_status") != "COMPLETED":
            continue  # failed/skipped — expiry columns stay blank
        if not r.get("fmw_query_ran", False):
            continue  # connected but query errored or not configured — stay blank
        key = (r.get("customer_name", ""), r.get("env_type", ""))
        env_rows.setdefault(key, []).extend(r.get("fmw_query_rows", []))
    return {key: _compute_expiry_status(rows) for key, rows in env_rows.items()}


# ── Sheet 1: DB Connection Check ─────────────────────────────────────────────

def _write_connection_sheet(ws, results, run_id, timestamp, db_label, expiry_map):
    total   = len(results)
    success = sum(1 for r in results if r.get("connection_status") == "COMPLETED")
    failed  = sum(1 for r in results if r.get("connection_status") == "FAILED")
    skipped = sum(1 for r in results if r.get("connection_status") == "SKIPPED")
    tcp_err = sum(1 for r in results if r.get("error_type") == "TCP_TIMEOUT")
    pwd_err = sum(1 for r in results if r.get("error_type") == "PASSWORD")
    unk_err = sum(1 for r in results
                  if r.get("error_type") == "UNKNOWN"
                  and r.get("connection_status") == "FAILED")

    if total == 0:
        summary, summ_color = "No data processed", C_RED
    elif failed == 0 and skipped == 0:
        summary, summ_color = "ALL CONNECTED  ✅", C_GREEN
    elif success == 0:
        summary, summ_color = (
            f"NO SUCCESSFUL CONNECTIONS  ❌   "
            f"Not Connected: {failed}  |  Skipped: {skipped}"), C_RED
    else:
        summary, summ_color = (
            f"PARTIAL  ⚠    Connected: {success}  |  "
            f"Not Connected: {failed}  |  Skipped: {skipped}"), C_AMBER

    last_letter = _col(LAST_COL)

    # Row 1 — Title
    ws.merge_cells(f"A1:{last_letter}1")
    c = ws["A1"]
    c.value     = f"{db_label} Oracle DB Connection Check  |  Run #{run_id}  |  {timestamp}"
    c.font      = Font(name="Calibri", size=13, bold=True, color=C_WHITE)
    c.fill      = PatternFill("solid", fgColor=HDR_DARK)
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 26

    # Row 2 — Summary bar
    ws.merge_cells(f"A2:{last_letter}2")
    c = ws["A2"]
    err_part = (
        f"  |  Errors — TCP: {tcp_err}  |  Password: {pwd_err}  |  Unknown: {unk_err}"
        if failed else "")
    c.value = (
        f"Total: {total}   ✅ Connected: {success}   ❌ Not Connected: {failed}"
        f"   ⚠ Skipped: {skipped}{err_part}   —   {summary}")
    c.font      = Font(name="Calibri", size=10, bold=True, color=summ_color)
    c.fill      = PatternFill("solid", fgColor=SUMM_BG)
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[2].height = 20

    # Row 3 — Headers
    headers = [
        "#", "Customer", "Environment", "Server Name",
        "Server Host", "DB Name", "DB Type", "DSN (Host:Port/Service)",
        "DB Connection Status", "Duration (ms)", "Error", "Error Type",
        "Checked At (IST)",
        "Expired\n(Action Required)", "Reason\n(if yes)",
    ]
    widths = [4, 22, 13, 18, 22, 20, 11, 32, 20, 13, 45, 35, 24, 18, 45]
    # Note: "Server ID" column intentionally removed/commented out for Sheet 1 per request.
    # If needed again, re-insert "Server ID" after "Server Name" in headers/widths above
    # and add `r.get("server_id", "") or "",` back into the `values` list below.

    for col, (h, w) in enumerate(zip(headers, widths), 1):
        c           = ws.cell(row=3, column=col, value=h)
        c.font      = Font(name="Calibri", size=10, bold=True, color=C_WHITE)
        c.fill      = PatternFill("solid", fgColor=HDR_MED)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border    = _border()
        ws.column_dimensions[_col(col)].width = w
    ws.row_dimensions[3].height = 30

    ws.auto_filter.ref = f"A3:{last_letter}{3 + max(total, 1)}"

    for idx, r in enumerate(results, 1):
        row      = idx + 3
        stat     = r.get("connection_status", "SKIPPED")
        err_type = r.get("error_type", "") or ""

        bg = (GREEN_BG if idx % 2 else GREEN_BG2) if stat == "COMPLETED" else \
             (RED_BG   if idx % 2 else RED_BG2)   if stat == "FAILED"    else \
             (AMBER_BG if idx % 2 else AMBER_BG2)

        h   = r.get("db_host",    "") or ""
        p   = r.get("db_port",    1521) or 1521
        svc = r.get("db_service", "") or ""
        dsn = f"{h}:{p}/{svc}" if h else ""

        # Expiry: FMW + COMPLETED + query ran → compute value; everything else → blank
        db_type_val = r.get("db_type", "").upper()
        if db_type_val == "FMW" and stat == "COMPLETED" and r.get("fmw_query_ran", False):
            key = (r.get("customer_name", ""), r.get("env_type", ""))
            expired_str, action_str, reason_str = expiry_map.get(key, ("No", "No", ""))
            expiry_display = f"{expired_str} / {action_str}"
        else:
            # blank for: failed, skipped, non-FMW, query errored, query not configured
            expiry_display = ""
            reason_str     = ""

        values = [
            idx,
            r.get("customer_name",  "") or "",
            r.get("env_type",       "") or "",
            r.get("server_name",    "") or "",
            # "Server ID" column intentionally removed/commented out here for Sheet 1.
            r.get("server_host",    "") or "",
            r.get("db_name",        "") or "—",
            r.get("db_type",        "") or "",
            dsn,
            STATUS_LABEL.get(stat, stat),
            r.get("duration_ms",    0),
            r.get("error_message",  "") or "",
            ERR_LABEL.get(err_type, err_type),
            _ist(r.get("checked_at")),
            expiry_display,
            reason_str,
        ]

        for col, val in enumerate(values, 1):
            c           = ws.cell(row=row, column=col, value=val)
            c.font      = Font(name="Calibri", size=10)
            c.fill      = PatternFill("solid", fgColor=bg)
            c.border    = _border()
            c.alignment = Alignment(vertical="center", wrap_text=True)

            if col == 9:  # DB Connection Status — bold coloured
                c.font      = Font(name="Calibri", size=10, bold=True,
                                   color=STATUS_COLOR.get(stat, C_RED))
                c.alignment = Alignment(horizontal="center", vertical="center")

            if col == 10:  # Duration — right-align
                c.alignment = Alignment(horizontal="right", vertical="center")

            if col == 11 and val:  # Error — italic
                c.font = Font(name="Calibri", size=9, italic=True,
                              color=STATUS_COLOR.get(stat, C_RED))

            if col == 12 and val:  # Error Type — bold coloured
                c.font = Font(name="Calibri", size=9, bold=True,
                              color=ERR_COLOR.get(err_type, C_BLUE))

            if col == 14 and expiry_display:  # Expired/Action — only when value present
                c.font = Font(name="Calibri", size=10, bold=True,
                              color=C_DARK_RED if expiry_display.startswith("Yes") else C_DARK_GRN)
                c.alignment = Alignment(horizontal="center", vertical="center")

            if col == 15 and reason_str:  # Reason — italic red only when date present
                c.font = Font(name="Calibri", size=9, italic=True, color=C_DARK_RED)

        ws.row_dimensions[row].height = 20

    ws.freeze_panes = "A4"


# ── Dynamic per-Customer/Env sheets (FMW only) ───────────────────────────────

def _safe_sheet_name(name: str) -> str:
    # Sanitise to valid Excel sheet name — max 31 chars, no illegal chars
    for ch in r"\/:*?[]":
        name = name.replace(ch, " ")
    return name[:31].strip()


def _write_fmw_env_sheet(ws, cname: str, env_type: str,
                         env_results: list, run_id, timestamp, fmw_query: str):
    # One sheet per Customer+Env — FMW db_type only
    # Row 2 shows the live query from app_config
    # Action Required: blank when not connected/skipped or query errored; Yes/No when query ran
    SHEET_COLS  = 5
    last_letter = _col(SHEET_COLS)

    # Row 1 — Title with customer, env, run info
    ws.merge_cells(f"A1:{last_letter}1")
    c = ws["A1"]
    c.value     = (f"{cname}  |  {env_type}  |  "
                   f"FMW OAM Account Expiry  |  Run #{run_id}  |  {timestamp}")
    c.font      = Font(name="Calibri", size=12, bold=True, color=C_WHITE)
    c.fill      = PatternFill("solid", fgColor=HDR_GREEN)
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 24

    # Row 2 — FMW query read live from app_config
    ws.merge_cells(f"A2:{last_letter}2")
    c = ws["A2"]
    c.value     = f"FMW Query : {fmw_query}"
    c.font      = Font(name="Calibri", size=9, italic=True, color="37474F")
    c.fill      = PatternFill("solid", fgColor="E8F5E9")
    c.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[2].height = 16

    # Row 3 — Headers
    headers = [
        "Server Host / DB Name / DB Type",
        "USERNAME", "ACCOUNT_STATUS", "EXPIRY_DATE", "Action Required",
    ]
    widths = [40, 26, 20, 22, 18]

    for col, (h, w) in enumerate(zip(headers, widths), 1):
        c           = ws.cell(row=3, column=col, value=h)
        c.font      = Font(name="Calibri", size=10, bold=True, color=C_WHITE)
        c.fill      = PatternFill("solid", fgColor=HDR_GREEN)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border    = _border()
        ws.column_dimensions[_col(col)].width = w
    ws.row_dimensions[3].height = 22

    data_rows = []
    for r in env_results:
        conn_info = (f"{r.get('server_host', '') or ''} / "
                     f"{r.get('db_name',     '') or ''} / "
                     f"{r.get('db_type',     '') or ''}")
        stat          = r.get("connection_status", "FAILED")
        fmw_rows      = r.get("fmw_query_rows", [])
        fmw_query_ran = r.get("fmw_query_ran",  False)

        if stat != "COMPLETED":
            # Not connected or skipped — query never ran — all OAM columns blank
            data_rows.append({
                "conn_info": conn_info, "USERNAME": "",
                "ACCOUNT_STATUS": "", "EXPIRY_DATE": "", "action": "",
            })
        elif not fmw_query_ran:
            # Connected but query errored or not configured — leave action blank
            data_rows.append({
                "conn_info": conn_info, "USERNAME": "",
                "ACCOUNT_STATUS": "", "EXPIRY_DATE": "", "action": "",
            })
        elif fmw_rows:
            # Query ran and returned OAM accounts — one row per account
            for frow in fmw_rows:
                expiry = frow.get("EXPIRY_DATE", "") or ""
                data_rows.append({
                    "conn_info":      conn_info,
                    "USERNAME":       frow.get("USERNAME",       ""),
                    "ACCOUNT_STATUS": frow.get("ACCOUNT_STATUS", ""),
                    "EXPIRY_DATE":    expiry,
                    "action":         "Yes" if _is_expiry_set(expiry) else "No",
                })
        else:
            # Query ran successfully but zero OAM accounts returned
            data_rows.append({
                "conn_info": conn_info, "USERNAME": "",
                "ACCOUNT_STATUS": "", "EXPIRY_DATE": "", "action": "No",
            })

    ws.auto_filter.ref = f"A3:{last_letter}{3 + max(len(data_rows), 1)}"

    for idx, dr in enumerate(data_rows, 1):
        row      = idx + 3
        bg       = "E8F5E9" if idx % 2 else "F1F8E9"
        expiry   = dr["EXPIRY_DATE"]
        has_date = _is_expiry_set(expiry)
        action   = dr["action"]

        for col, val in enumerate(
            [dr["conn_info"], dr["USERNAME"], dr["ACCOUNT_STATUS"], expiry, action], 1
        ):
            c           = ws.cell(row=row, column=col, value=val)
            c.font      = Font(name="Calibri", size=10)
            c.fill      = PatternFill("solid", fgColor=bg)
            c.border    = _border()
            c.alignment = Alignment(vertical="center", wrap_text=True)

            if col == 4 and has_date:  # EXPIRY_DATE — red bold when real date
                c.font      = Font(name="Calibri", size=10, bold=True, color=C_DARK_RED)
                c.alignment = Alignment(horizontal="center", vertical="center")

            if col == 3 and val:  # ACCOUNT_STATUS — red if expired/locked, green otherwise
                up = str(val).upper()
                c.font = Font(name="Calibri", size=10, bold=True, color=C_RED) \
                    if ("EXPIRED" in up or "LOCKED" in up) \
                    else Font(name="Calibri", size=10, color=C_DARK_GRN)

            if col == 5 and action:  # Action Required — only style when not blank
                c.font = Font(name="Calibri", size=10, bold=True,
                              color=C_DARK_RED if action == "Yes" else C_DARK_GRN)
                c.alignment = Alignment(horizontal="center", vertical="center")

        ws.row_dimensions[row].height = 20

    ws.freeze_panes = "A4"

    if not data_rows:
        ws.merge_cells(f"A4:{last_letter}4")
        c = ws["A4"]
        c.value     = f"No FMW OAM accounts found for {cname} / {env_type}."
        c.font      = Font(name="Calibri", size=10, italic=True, color=C_AMBER)
        c.alignment = Alignment(horizontal="center", vertical="center")


# ── Public entry point ────────────────────────────────────────────────────────

def generate_excel(results, run_id, timestamp, excel_path,
                   db_label="FMW", fmw_query: str = "") -> str:
    # Generates report Excel — Sheet 1 (all DBs) + one sheet per FMW Customer/Env
    os.makedirs(excel_path, exist_ok=True)

    label    = (db_label or "FMW").upper().replace("/", "_").replace(" ", "_")
    date_str = datetime.now().strftime("%d-%m-%y")
    filename = f"{label}_CONNECTION_CHECKS_{date_str}-{run_id}.xlsx"
    filepath = os.path.join(excel_path, filename)

    query_text = fmw_query.strip() if fmw_query else _DEFAULT_FMW_QUERY

    # Expiry map — only from FMW COMPLETED entries where query actually ran
    expiry_map = _build_expiry_map(results)

    wb = Workbook()

    # Sheet 1 — DB Connection Check (all entries)
    ws1 = wb.active
    ws1.title = "DB Connection Check"
    _write_connection_sheet(ws1, results, run_id, timestamp, db_label, expiry_map)

    # Dynamic sheets — one per unique Customer + Environment for FMW db_type only
    env_groups: dict = {}
    for r in results:
        if r.get("db_type", "").upper() != "FMW":
            continue
        key = (r.get("customer_name", "") or "", r.get("env_type", "") or "")
        env_groups.setdefault(key, []).append(r)

    used_names: dict = {}
    for (cname, env_type), env_results in env_groups.items():
        safe_name = _safe_sheet_name(f"{cname} - {env_type}")
        if safe_name in used_names:
            used_names[safe_name] += 1
            safe_name = _safe_sheet_name(f"{safe_name} ({used_names[safe_name]})")
        else:
            used_names[safe_name] = 1
        ws = wb.create_sheet(title=safe_name)
        _write_fmw_env_sheet(ws, cname, env_type, env_results,
                             run_id, timestamp, query_text)
        log.info(f"  FMW sheet   : '{safe_name}'  ({len(env_results)} DB entry(s))")

    wb.save(filepath)
    log.info(f"  Excel saved : {filepath}")
    return filepath
