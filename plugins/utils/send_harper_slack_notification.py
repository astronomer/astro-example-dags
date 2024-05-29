from __future__ import annotations

# import json
from typing import TYPE_CHECKING, Sequence

import pytz
from airflow.models import Variable
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.notifications.slack import SlackNotifier

from plugins.utils.truncate_exception import truncate_exception

if TYPE_CHECKING:
    import jinja2
    from slack_sdk.http_retry import RetryHandler
    from airflow.utils.context import Context


ICON_URL: str = "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static/pin_100.png"


def convert_datetime(datetime_string):

    return datetime_string.astimezone(pytz.timezone("Europe/London")).strftime("%b-%d %H:%M:%S")


ICON_URL: str = "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static/pin_100.png"


class HarperSlackSuccessNotifier(SlackNotifier):

    def __init__(
        self,
        *,
        slack_conn_id: str = SlackHook.default_conn_name,
        username: str = "Airflow",
        icon_url: str = ICON_URL,
        attachments: Sequence = (),
        base_url: str | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        retry_handlers: list[RetryHandler] | None = None,
    ):

        try:
            device_name = Variable.get("DEVICE_NAME")
        except KeyError:
            device_name = "Production"  # Default value

        try:
            channel = Variable.get("SLACK_SUCCESS_CHANNEL")
        except KeyError:
            channel = "#alerts-dev-airflow"  # Default value

        super().__init__(
            slack_conn_id=slack_conn_id,
            text="",
            channel=channel,
            username=username,
            icon_url=icon_url,
            attachments=attachments,
            blocks=[
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": ":large_green_circle: Dag Run Successful",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "{{ run_id }}",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": "Dag: {{ dag.dag_id }}"},
                        {"type": "mrkdwn", "text": "DagRun: {{ run_id }}"},
                        {"type": "mrkdwn", "text": f"System: {device_name}"},
                    ],
                },
            ],
            base_url=base_url,
            proxy=proxy,
            timeout=timeout,
            retry_handlers=retry_handlers,
        )

    # def notify(self, context):
    #     """Send a message to a Slack Channel."""
    #     api_call_params = {
    #         "channel": self.channel,
    #         "username": self.username,
    #         "text": self.text,
    #         "icon_url": self.icon_url,
    #         "attachments": json.dumps(self.attachments),
    #         "blocks": json.dumps(self.blocks),
    #     }
    #     self.hook.call("chat.postMessage", json=api_call_params)


send_harper_success_notification = HarperSlackSuccessNotifier


class HarperSlackFailureNotifier(SlackNotifier):

    def __init__(
        self,
        *,
        slack_conn_id: str = SlackHook.default_conn_name,
        username: str = "Airflow Fail",
        icon_url: str = ICON_URL,
        attachments: Sequence = (),
        base_url: str | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        retry_handlers: list[RetryHandler] | None = None,
    ):

        try:
            device_name = Variable.get("DEVICE_NAME")
        except KeyError:
            device_name = "Production"  # Default value

        try:
            channel = Variable.get("SLACK_FAILURE_CHANNEL")
        except KeyError:
            channel = "#alerts-dev-airflow"  # Default value

        super().__init__(
            slack_conn_id=slack_conn_id,
            text="",
            channel=channel,
            username=username,
            icon_url=icon_url,
            attachments=attachments,
            blocks=[
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": ":red_circle: {{ dag.dag_id }} Failed",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": "{{ run_id }}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": "{{ task.task_id }}",
                        },
                    ],
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "```{{ exception | truncate_exception(2000) }}```",
                    },
                },
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": "Dag: {{ dag.dag_id }}"},
                        {"type": "mrkdwn", "text": "DagRun: {{ run_id }}"},
                        {"type": "mrkdwn", "text": f"System: {device_name}"},
                        {
                            "type": "mrkdwn",
                            "text": "<{{ task_instance.log_url }}| View Log>",
                        },
                    ],
                },
            ],
            base_url=base_url,
            proxy=proxy,
            timeout=timeout,
            retry_handlers=retry_handlers,
        )

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """Add our custom macro

        :param context: Context dict with values to apply on content.
        :param jinja_env: Jinja environment to use for rendering.
        """
        dag = context["dag"]
        if not jinja_env:
            jinja_env = self.get_template_env(dag=dag)
        jinja_env.filters["truncate_exception"] = truncate_exception
        self._do_render_template_fields(self, self.template_fields, context, jinja_env, set())


send_harper_failure_notification = HarperSlackFailureNotifier
