# Databricks notebook source
# MAGIC %md
# MAGIC # Notifier Utility — MS Teams + Slack + Email Alerts
# MAGIC
# MAGIC Sends pipeline alerts to MS Teams (Incoming Webhook), Slack (webhook),
# MAGIC and/or email (SMTP). Credentials are read from Databricks Secrets so
# MAGIC nothing sensitive is committed to the repo.
# MAGIC
# MAGIC **Setup (one-time):**
# MAGIC ```bash
# MAGIC # Create a Databricks secret scope named "retail_platform"
# MAGIC databricks secrets create-scope retail_platform
# MAGIC
# MAGIC # MS Teams — get the Incoming Webhook URL from Teams channel settings:
# MAGIC #   Channel → ... → Connectors → Incoming Webhook → Configure → Copy URL
# MAGIC databricks secrets put --scope retail_platform --key teams_webhook_url
# MAGIC
# MAGIC # Slack (optional, kept for backwards compatibility)
# MAGIC databricks secrets put --scope retail_platform --key slack_webhook_url
# MAGIC
# MAGIC # Email (optional)
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
import ssl
import json
import html as _html
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
    teams_webhook_url: str | None = field(default=None, repr=False)  # MS Teams Incoming Webhook
    slack_webhook_url: str | None = field(default=None, repr=False)  # Slack (optional / legacy)

    # SMTP config (works with Office 365, SendGrid, Gmail, etc.)
    smtp_host:        str | None = None
    smtp_port:        int        = 587
    smtp_user:        str | None = field(default=None, repr=False)
    smtp_password:    str | None = field(default=None, repr=False)
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
            except Exception as exc:
                msg = str(exc).lower()
                if "secret does not exist" in msg or "resource_does_not_exist" in msg:
                    return None
                print(f"[Notifier] WARNING: could not fetch secret '{key}' "
                      f"from scope '{scope}': {type(exc).__name__}")
                return None

        cfg = NotifierConfig(
            teams_webhook_url = _get("teams_webhook_url"),
            slack_webhook_url = _get("slack_webhook_url"),
            smtp_host         = _get("smtp_host"),
            smtp_port         = int(_get("smtp_port") or 587),
            smtp_user         = _get("smtp_user"),
            smtp_password     = _get("smtp_password"),
            email_from        = _get("email_from"),
            email_to          = email_to or [],
            environment       = environment,
        )

        if not any([cfg.teams_webhook_url, cfg.slack_webhook_url, cfg.smtp_host]):
            print("[Notifier] WARNING: no notification channels configured. "
                  "Add 'teams_webhook_url', 'slack_webhook_url', or SMTP keys "
                  f"to Databricks secret scope '{scope}'.")

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

        self._dispatch_teams(context)
        self._dispatch_slack(context)
        self._dispatch_email(context)

    # ── MS Teams ──────────────────────────────────────────────────────────────

    def _dispatch_teams(self, ctx: dict) -> None:
        """
        Sends an Adaptive MessageCard to an MS Teams channel via Incoming Webhook.
        Connector URL configured in Teams: Channel → ... → Connectors →
        Incoming Webhook → Configure → Copy URL → store in Databricks Secrets.
        """
        if not self.cfg.teams_webhook_url:
            return

        color = LEVEL_COLOR.get(ctx["level"], "#aaaaaa").lstrip("#")

        facts = [
            {"name": "Stage",       "value": ctx["stage"]},
            {"name": "Run ID",      "value": ctx["run_id"] or "—"},
            {"name": "Environment", "value": ctx["environment"]},
            {"name": "Time",        "value": ctx["timestamp"]},
        ]
        if ctx["rows_written"] is not None:
            facts.append({"name": "Rows Written",  "value": f"{ctx['rows_written']:,}"})
        if ctx["rows_rejected"] is not None:
            facts.append({"name": "Rows Rejected", "value": f"{ctx['rows_rejected']:,}"})
        if ctx["duration_ms"] is not None:
            facts.append({"name": "Duration",      "value": f"{ctx['duration_ms']:,} ms"})

        payload = {
            "@type":    "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": color,
            "summary":  f"[{ctx['level']}] {ctx['project']}: {ctx['stage']}",
            "sections": [{
                "activityTitle":    f"**[{ctx['level']}] {ctx['project']} — {ctx['stage']}**",
                "activitySubtitle": ctx["environment"],
                "facts":            facts,
                "text":             ctx["message"],
            }],
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req  = urllib.request.Request(
                self.cfg.teams_webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=10) as resp:
                if resp.status != 200:
                    print(f"[Notifier] Teams returned HTTP {resp.status}")
        except urllib.error.URLError as e:
            print(f"[Notifier] Teams delivery failed: {e}")
        except Exception as e:
            print(f"[Notifier] Unexpected Teams error: {e}")

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
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=10) as resp:
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

        html_body = f"""
        <html><body>
        <h2 style="color:{LEVEL_COLOR[level]}">{_html.escape(level)}: {_html.escape(ctx['stage'])}</h2>
        <p><b>Project:</b> {_html.escape(ctx['project'])}<br>
           <b>Run ID:</b> {_html.escape(ctx['run_id'])}<br>
           <b>Environment:</b> {_html.escape(ctx['environment'])}<br>
           <b>Time:</b> {_html.escape(ctx['timestamp'])}</p>
        <p style="font-size:16px">{_html.escape(ctx['message'])}</p>
        {'<table border="1" cellpadding="4">' + metrics_rows + '</table>' if metrics_rows else ''}
        </body></html>
        """

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.cfg.email_from or self.cfg.smtp_user
        msg["To"]      = ", ".join(self.cfg.email_to)
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port, timeout=15) as s:
                s.ehlo()
                s.starttls(context=ssl.create_default_context())
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
