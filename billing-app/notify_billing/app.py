import json
import logging
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import boto3

# Lambda の Python ランタイムはルートロガーにハンドラを設定済みのため、
# basicConfig ではなくレベルだけ設定する
logger = logging.getLogger()
logger.setLevel(logging.INFO)

JST = timezone(timedelta(hours=+9), "JST")

# 外部 HTTP 通信のタイムアウト(秒)。Lambda のタイムアウト(25秒)より十分短くする
HTTP_TIMEOUT_SECONDS = 10
USER_AGENT = "notify-billing-lambda/1.0"

# Discord の embed description の上限文字数
DISCORD_DESCRIPTION_LIMIT = 4096

CENT = Decimal("0.01")
YEN = Decimal("1")


def today_jst() -> date:
    """JST の現在日付を返す"""
    return datetime.now(JST).date()


def get_date_range(today: date) -> tuple[date, date]:
    """
    Cost Explorer から取得する期間を返す

    実行日が月初なら前月分、それ以外は当月1日から前日までを対象とする。

    Parameters
    ----------
    today : date
        実行日

    Returns
    -------
    start : date
        期初(この日を含む)
    end : date
        期末(この日を含まない。Cost Explorer の TimePeriod.End と同じ扱い)
    """
    this_month_first = today.replace(day=1)
    if today.day == 1:
        start = (this_month_first - timedelta(days=1)).replace(day=1)
    else:
        start = this_month_first
    return start, today


def get_service_billings(client, start: date, end: date) -> dict[str, Decimal]:
    """
    AWS 使用料金をサービスごとに取得する

    Parameters
    ----------
    client : boto3 の Cost Explorer クライアント
    start : date
        期初(含む)
    end : date
        期末(含まない)

    Returns
    -------
    billings : dict[str, Decimal]
        サービス名をキー、料金(USD)を値とする辞書
    """
    params = {
        "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
        "Granularity": "MONTHLY",
        "Metrics": ["AmortizedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    billings: dict[str, Decimal] = {}
    while True:
        response = client.get_cost_and_usage(**params)
        for result in response["ResultsByTime"]:
            for group in result["Groups"]:
                name = group["Keys"][0]
                amount = Decimal(group["Metrics"]["AmortizedCost"]["Amount"])
                billings[name] = billings.get(name, Decimal(0)) + amount
        token = response.get("NextPageToken")
        if not token:
            return billings
        params["NextPageToken"] = token


def get_exchange_rate() -> Decimal | None:
    """
    ドル円為替レートを取得する

    取得できない場合は None を返し、通知は USD のみで行う。

    Returns
    -------
    exchange_rate : Decimal | None
        ドル円為替レート
    """
    url = os.getenv("EXCHANGE_RATE_URL")
    if not url:
        logger.warning("EXCHANGE_RATE_URLが設定されていないため、USDのみで通知します。")
        return None

    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as res:
            data = json.load(res, parse_float=Decimal)
        rate = Decimal(str(data["values"][0][0]))
    except Exception:
        logger.exception("為替レートの取得に失敗しました。USDのみで通知します。")
        return None

    if rate <= 0:
        logger.error(f"為替レートが不正です。USDのみで通知します。rate={rate}")
        return None
    return rate


def format_amount(usd: Decimal, exchange_rate: Decimal | None) -> str:
    """金額を表示用の文字列にする(円は丸める前の USD から計算する)"""
    usd_text = usd.quantize(CENT, rounding=ROUND_HALF_UP)
    if exchange_rate is None:
        return f"{usd_text} USD"
    yen = (usd * exchange_rate).quantize(YEN, rounding=ROUND_HALF_UP)
    return f"￥{yen:,} ({usd_text} USD)"


def build_message(
    start: date,
    end: date,
    billings: dict[str, Decimal],
    exchange_rate: Decimal | None,
    run_date: date,
) -> tuple[str, str, str]:
    """
    Discord へ送信するメッセージを作成する

    Parameters
    ----------
    start : date
        期初(含む)
    end : date
        期末(含まない)
    billings : dict[str, Decimal]
        サービスごとの料金(USD)
    exchange_rate : Decimal | None
        ドル円為替レート。None なら USD のみ表示する
    run_date : date
        実行日(為替レートの取得日)

    Returns
    -------
    title : str
    description : str
    footer : str
    """
    last_day = end - timedelta(days=1)
    total = sum(billings.values(), Decimal(0))
    title = (
        f"{start:%m/%d}～{last_day:%m/%d}の請求額は、"
        f"{format_amount(total, exchange_rate)}です。"
    )

    lines = [
        f"・{name}: {format_amount(amount, exchange_rate)}"
        for name, amount in sorted(billings.items(), key=lambda x: x[1], reverse=True)
        if amount.quantize(CENT, rounding=ROUND_HALF_UP) != 0
    ]
    description = truncate_lines(lines, DISCORD_DESCRIPTION_LIMIT)

    footer_lines = []
    if exchange_rate is not None:
        footer_lines.append(
            f"※為替レート: {exchange_rate} 円/ドル ({run_date:%m/%d} 取得)"
        )
    footer_lines.append("※速報値のため、確定した請求額と異なる場合があります。")

    return title, description, "\n".join(footer_lines)


def truncate_lines(lines: list[str], limit: int) -> str:
    """行のリストを、上限文字数に収まるように連結する"""
    text = "\n".join(lines)
    if len(text) <= limit:
        return text

    for count in range(len(lines) - 1, -1, -1):
        rest = f"…ほか{len(lines) - count}件"
        text = "\n".join([*lines[:count], rest])
        if len(text) <= limit:
            return text
    return text[:limit]


def post_discord(title: str, description: str, footer: str) -> None:
    """
    Discord Webhook へ通知する

    失敗した場合は例外を送出する。

    Parameters
    ----------
    title : str
        embed のタイトル
    description : str
        embed の本文
    footer : str
        embed のフッター
    """
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        raise RuntimeError("DISCORD_WEBHOOK_URLが設定されていません。")

    embed = {"title": title, "description": description}
    if footer:
        embed["footer"] = {"text": footer}
    body = {"content": "@everyone\n", "embeds": [embed]}

    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    # 4xx/5xx は HTTPError として送出される
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS):
        pass


def lambda_handler(event, context) -> None:
    run_date = today_jst()
    start, end = get_date_range(run_date)
    logger.info(f"対象期間: {start} ～ {end}(含まない)")

    # Cost Explorer のエンドポイントは us-east-1
    client = boto3.client("ce", region_name="us-east-1")
    billings = get_service_billings(client, start, end)
    exchange_rate = get_exchange_rate()

    title, description, footer = build_message(
        start, end, billings, exchange_rate, run_date
    )
    post_discord(title, description, footer)
    logger.info("Discordへ通知しました。")
