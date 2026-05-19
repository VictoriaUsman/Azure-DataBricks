# Databricks notebook source
# MAGIC %md
# MAGIC # Notifier Utility — Slack + Email Alerts
# MAGIC
# MAGIC Sends pipeline alerts to Slack (webhook) and/or email (SMTP).
# MAGIC Credentials are read from Databricks Secrets so nothing sensitive is
# MAGIC committed to the repo.
# MAGIC
# MAGIC **Setup (one-time):**
# MAGIC ```bash
# MAGIC # Create a Databricks secret scope named "retail_platform"
# MAGIC databricks secrets create-scope retail_platform
# MAGIC databricks secrets put --scope retail_platform --key slack_webhook_url
# MAGIC databricks secrets put --scope retail_platform --key smtp_password
# MAGIC ```
# MAGIC
# MAGIC **Usage:**
# MAGIC ```python
# MAGIC notifier = Notifier.from_secrets()
# MAGIC notifier.send_pipeline_alert("bronze_ingestion", level="ERROR",
# MAGIC                               message="Ingestion failed: file not found",
# MAGIC                               run_id="abc123")
# MAGIC ```

# COMMAND ----------

import smtplib
import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from typing import Literal

# COMMAND ----------

LEVEL_EMOJI = {
    "INFO":  ":white_check_mark:",
    "WARN":  ":warning:",
    "ERROR": ":red_circle:",
}

LEVEL_COLOR = {
    "INFO":  "#36a64f",
    "WARN":  "#ffae00",
    "ERROR": "#e01e5a",
}


@dataclass
class NotifierConfig:
    slack_webhook_url: str | None = None

    # SMTP config (works with Gmail, SendGrid, Office 365, etc.)
    smtp_host:        str | None = None
    smtp_port:        int        = 587
    smtp_user:        str | None = None
    smtp_password:    str | None = None
    email_from:       str | None = None
    email_to:         list[str]  = field(default_factory=list)

    environment:      str        = "databricks-community"
    project:          str        = "retail-sales-platform"


# COMMAND ----------


class Notifier:
    """
    Sends structured pipeline alerts to Slack and/or email.
    Failures in notification delivery are logged but never crash the pipeline.
    """

    def __init__(self, config: NotifierConfig):
        self.cfg = config

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_secrets(
        cls,
        scope:        str  = "retail_platform",
        environment:  str  = "production",
        email_to:     list[str] | None = None,
    ) -> "Notifier":
        """
        Build a Notifier by reading credentials from Databricks Secret Scope.
        Falls back gracefully if a secret doesn't exist (prints a warning).
        """
        def _get(key: str) -> str | None:
            try:
                return dbutils.secrets.get(scope=scope, key=key)
            except Exception:
                return None

        cfg = NotifierConfig(
            slack_webhook_url = _get("slack_webhook_url"),
            smtp_host         = _get("smtp_host"),
            smtp_port         = int(_get("smtp_port") or 587),
            smtp_user         = _get("smtp_user"),
            smtp_password     = _get("smtp_password"),
            email_from        = _get("email_from"),
            email_to          = email_to or [],
            environment       = environment,
        )

        if not cfg.slack_webhook_url and not cfg.smtp_host:
            print("[Notifier] WARNING: no notification channels configured. "
                  "Add 'slack_webhook_url' or SMTP keys to Databricks secret scope "
                  f"'{scope}'.")

        return cls(cfg)

    @classmethod
    def from_dict(cls, config: dict) -> "Notifier":
        """Build from a plain dict — useful in tests or local runs."""
        return cls(NotifierConfig(**config))

    # ── Public API ────────────────────────────────────────────────────────────

    def send_pipeline_alert(
        self,
        stage:        str,
        level:        Literal["INFO", "WARN", "ERROR"],
        message:      str,
        run_id:       str  = "",
        rows_written: int | None = None,
        rows_rejected:int | None = None,
        duration_ms:  int | None = None,
    ) -> None:
        """
        Main entry point. Dispatches to all configured channels.
        Delivery failures are swallowed so they never block the pipeline.
        """
        context = {
            "stage":         stage,
            "level":         level,
            "message":       message,
            "run_id":        run_id,
            "rows_written":  rows_written,
            "rows_rejected": rows_rejected,
            "duration_ms":   duration_ms,
            "environment":   self.cfg.environment,
            "project":       self.cfg.project,
            "timestamp":     datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

        self._dispatch_slack(context)
        self._dispatch_email(context)

    # ── Slack ─────────────────────────────────────────────────────────────────

    def _dispatch_slack(self, ctx: dict) -> None:
        if not self.cfg.slack_webhook_url:
            return

        emoji = LEVEL_EMOJI.get(ctx["level"], ":bell:")
        color = LEVEL_COLOR.get(ctx["level"], "#aaaaaa")

        fields = [
            {"title": "Stage",       "value": ctx["stage"],       "short": True},
            {"title": "Run ID",      "value": ctx["run_id"],      "short": True},
            {"title": "Environment", "value": ctx["environment"], "short": True},
            {"title": "Time",        "value": ctx["timestamp"],   "short": True},
        ]
        if ctx["rows_written"] is not None:
            fields.append({"title": "Rows Written",  "value": f"{ctx['rows_written']:,}",  "short": True})
        if ctx["rows_rejected"] is not None:
            fields.append({"title": "Rows Rejected", "value": f"{ctx['rows_rejected']:,}", "short": True})
        if ctx["duration_ms"] is not None:
            fields.append({"title": "Duration",      "value": f"{ctx['duration_ms']:,} ms", "short": True})

        payload = {
            "text": f"{emoji} *[{ctx['project']}] {ctx['level']}: {ctx['stage']}*",
            "attachments": [{
                "color":  color,
                "text":   ctx["message"],
                "fields": fields,
                "footer": "Retail Sales Analytics Platform",
            }],
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req  = urllib.request.Request(
                self.cfg.slack_webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    print(f"[Notifier] Slack returned HTTP {resp.status}")
        except urllib.error.URLError as e:
            print(f"[Notifier] Slack delivery failed: {e}")
        except Exception as e:
            print(f"[Notifier] Unexpected Slack error: {e}")

    # ── Email ─────────────────────────────────────────────────────────────────

    def _dispatch_email(self, ctx: dict) -> None:
        if not (self.cfg.smtp_host and self.cfg.smtp_user
                and self.cfg.smtp_password and self.cfg.email_to):
            return

        level   = ctx["level"]
        subject = f"[{level}] {ctx['project']} — {ctx['stage']} ({ctx['environment']})"

        metrics_rows = ""
        for key in ("rows_written", "rows_rejected", "duration_ms"):
            if ctx.get(key) is not None:
                label = key.replace("_", " ").title()
                metrics_rows += f"<tr><td><b>{label}</b></td><td>{ctx[key]:,}</td></tr>"

        html = f"""
        <html><body>
        <h2 style="color:{LEVEL_COLOR[level]}">{level}: {ctx['stage']}</h2>
        <p><b>Project:</b> {ctx['project']}<br>
           <b>Run ID:</b> {ctx['run_id']}<br>
           <b>Environment:</b> {ctx['environment']}<br>
           <b>Time:</b> {ctx['timestamp']}</p>
        <p style="font-size:16px">{ctx['message']}</p>
        {'<table border="1" cellpadding="4">' + metrics_rows + '</table>' if metrics_rows else ''}
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.cfg.email_from or self.cfg.smtp_user
        msg["To"]      = ", ".join(self.cfg.email_to)
        msg.attach(MIMEText(html, "html"))

        try:
            with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port, timeout=15) as s:
                s.ehlo()
                s.starttls()
                s.login(self.cfg.smtp_user, self.cfg.smtp_password)
                s.sendmail(
                    msg["From"],
                    self.cfg.email_to,
                    msg.as_string(),
                )
        except smtplib.SMTPException as e:
            print(f"[Notifier] Email delivery failed: {e}")
        except OSError as e:
            print(f"[Notifier] Email connection error: {e}")
