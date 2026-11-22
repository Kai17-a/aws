import io
import json
import urllib.error
from datetime import date
from decimal import Decimal

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

import app

# ---------------------------------------------------------------------------
# get_date_range
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        # 月初は前月分すべて（End は当日＝排他的なので前月末日まで含む）
        (date(2026, 11, 1), (date(2026, 10, 1), date(2026, 11, 1))),
        # 年またぎ
        (date(2026, 1, 1), (date(2025, 12, 1), date(2026, 1, 1))),
        # 月の途中は当月1日から前日まで
        (date(2026, 11, 22), (date(2026, 11, 1), date(2026, 11, 22))),
        # 2日でも Start < End になる
        (date(2026, 11, 2), (date(2026, 11, 1), date(2026, 11, 2))),
    ],
)
def test_get_date_range(today, expected):
    assert app.get_date_range(today) == expected


# ---------------------------------------------------------------------------
# get_service_billings
# ---------------------------------------------------------------------------


def _group(name, amount):
    return {
        "Keys": [name],
        "Metrics": {"AmortizedCost": {"Amount": amount, "Unit": "USD"}},
    }


def _response(groups, token=None):
    res = {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-10-01", "End": "2026-11-01"},
                "Total": {},
                "Groups": groups,
                "Estimated": True,
            }
        ]
    }
    if token:
        res["NextPageToken"] = token
    return res


def test_get_service_billings_calls_api_once_per_page_and_aggregates():
    client = boto3.client(
        "ce",
        region_name="us-east-1",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )
    expected_params = {
        "TimePeriod": {"Start": "2026-10-01", "End": "2026-11-01"},
        "Granularity": "MONTHLY",
        "Metrics": ["AmortizedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    with Stubber(client) as stub:
        stub.add_response(
            "get_cost_and_usage",
            _response([_group("Amazon S3", "1.5"), _group("AWS Lambda", "0.1")], "t"),
            expected_params,
        )
        stub.add_response(
            "get_cost_and_usage",
            _response([_group("Amazon S3", "0.25")]),
            {**expected_params, "NextPageToken": "t"},
        )
        result = app.get_service_billings(client, date(2026, 10, 1), date(2026, 11, 1))
        stub.assert_no_pending_responses()

    assert result == {
        "Amazon S3": Decimal("1.75"),
        "AWS Lambda": Decimal("0.1"),
    }


def test_get_service_billings_propagates_errors():
    client = boto3.client(
        "ce",
        region_name="us-east-1",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )
    with Stubber(client) as stub:
        stub.add_client_error("get_cost_and_usage", "AccessDeniedException")
        with pytest.raises(ClientError):
            app.get_service_billings(client, date(2026, 10, 1), date(2026, 11, 1))


# ---------------------------------------------------------------------------
# get_exchange_rate
# ---------------------------------------------------------------------------


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def test_get_exchange_rate_keeps_exact_decimal(monkeypatch):
    monkeypatch.setenv("EXCHANGE_RATE_URL", "https://example.com/rate")
    captured = {}

    def fake_urlopen(req, timeout):
        captured["timeout"] = timeout
        return _FakeResponse(json.dumps({"values": [[150.13]]}).encode())

    monkeypatch.setattr(app.urllib.request, "urlopen", fake_urlopen)
    assert app.get_exchange_rate() == Decimal("150.13")
    assert str(app.get_exchange_rate()) == "150.13"
    assert captured["timeout"] > 0


def test_get_exchange_rate_returns_none_without_url(monkeypatch):
    monkeypatch.delenv("EXCHANGE_RATE_URL", raising=False)
    assert app.get_exchange_rate() is None


def test_get_exchange_rate_returns_none_on_error(monkeypatch):
    monkeypatch.setenv("EXCHANGE_RATE_URL", "https://example.com/rate")

    def fake_urlopen(req, timeout):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(app.urllib.request, "urlopen", fake_urlopen)
    assert app.get_exchange_rate() is None


# ---------------------------------------------------------------------------
# build_message
# ---------------------------------------------------------------------------


def test_build_message_with_exchange_rate():
    billings = {
        "AWS Lambda": Decimal("0.123"),
        "Amazon S3": Decimal("10.004"),
        "Tax": Decimal("1.5"),
        "AWS Cost Explorer": Decimal("0.004"),  # 0.00 に丸まるので表示しない
    }
    title, description, footer = app.build_message(
        date(2026, 10, 1),
        date(2026, 11, 1),
        billings,
        Decimal("150.5"),
        date(2026, 11, 1),
    )
    # 合計 11.631 → 11.63 USD、円は 11.631 * 150.5 = 1750.4655 → 1,750 円
    assert title == "10/01～10/31の請求額は、￥1,750 (11.63 USD)です。"
    # 金額の大きい順
    assert description.splitlines() == [
        "・Amazon S3: ￥1,506 (10.00 USD)",
        "・Tax: ￥226 (1.50 USD)",
        "・AWS Lambda: ￥19 (0.12 USD)",
    ]
    assert "150.5 円/ドル" in footer
    assert "11/01 取得" in footer
    assert "速報値" in footer


def test_build_message_without_exchange_rate():
    title, description, footer = app.build_message(
        date(2026, 11, 1),
        date(2026, 11, 22),
        {"Amazon S3": Decimal("2.345")},
        None,
        date(2026, 11, 22),
    )
    assert title == "11/01～11/21の請求額は、2.35 USDです。"
    assert description == "・Amazon S3: 2.35 USD"
    assert "為替レート" not in footer
    assert "速報値" in footer


def test_build_message_truncates_long_description():
    billings = {f"Service {i:03d} " + "x" * 80: Decimal(1000 - i) for i in range(200)}
    _, description, _ = app.build_message(
        date(2026, 10, 1), date(2026, 11, 1), billings, None, date(2026, 11, 1)
    )
    assert len(description) <= app.DISCORD_DESCRIPTION_LIMIT
    assert description.splitlines()[0].startswith("・Service 000")
    assert description.splitlines()[-1].startswith("…ほか")


# ---------------------------------------------------------------------------
# post_discord
# ---------------------------------------------------------------------------


def test_post_discord_raises_without_url(monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    with pytest.raises(RuntimeError):
        app.post_discord("t", "d", "f")


def test_post_discord_sends_embed(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    captured = {}

    def fake_urlopen(req, timeout):
        captured["req"] = req
        captured["timeout"] = timeout
        return _FakeResponse(b"")

    monkeypatch.setattr(app.urllib.request, "urlopen", fake_urlopen)
    app.post_discord("title", "desc", "foot")

    req = captured["req"]
    assert req.get_method() == "POST"
    assert req.full_url == "https://discord.example/webhook"
    assert req.get_header("Content-type") == "application/json"
    assert req.get_header("User-agent")
    body = json.loads(req.data)
    assert body["embeds"] == [
        {"title": "title", "description": "desc", "footer": {"text": "foot"}}
    ]
    assert captured["timeout"] > 0


def test_post_discord_propagates_http_error(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")

    def fake_urlopen(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 400, "Bad Request", {}, None)

    monkeypatch.setattr(app.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(urllib.error.HTTPError):
        app.post_discord("t", "d", "f")


# ---------------------------------------------------------------------------
# lambda_handler
# ---------------------------------------------------------------------------


class _FakeBoto3:
    @staticmethod
    def client(*args, **kwargs):
        return None


def _stub_boto3(monkeypatch):
    monkeypatch.setattr(app, "boto3", _FakeBoto3)


def test_lambda_handler_uses_current_date_each_invocation(monkeypatch):
    calls = []
    _stub_boto3(monkeypatch)
    monkeypatch.setattr(
        app,
        "get_service_billings",
        lambda client, start, end: calls.append((start, end)) or {},
    )
    monkeypatch.setattr(app, "get_exchange_rate", lambda: None)
    monkeypatch.setattr(app, "post_discord", lambda *a: None)

    for today in (date(2026, 11, 1), date(2026, 12, 1)):
        monkeypatch.setattr(app, "today_jst", lambda today=today: today)
        app.lambda_handler({}, None)

    assert calls == [
        (date(2026, 10, 1), date(2026, 11, 1)),
        (date(2026, 11, 1), date(2026, 12, 1)),
    ]


def test_lambda_handler_fails_when_cost_explorer_fails(monkeypatch):
    _stub_boto3(monkeypatch)

    def boom(*args):
        raise RuntimeError("ce error")

    monkeypatch.setattr(app, "get_service_billings", boom)
    posted = []
    monkeypatch.setattr(app, "post_discord", lambda *a: posted.append(a))
    with pytest.raises(RuntimeError):
        app.lambda_handler({}, None)
    assert posted == []
