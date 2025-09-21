import os
import boto3
import requests
import logging
from datetime import datetime, timedelta, timezone
from decimal import *

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s"
)
logger = logging.getLogger()

# boto3クライアント
ce_client = boto3.client("ce", region_name="ap-northeast-1")

# JSTタイムゾーンと現在時刻
JST = timezone(timedelta(hours=+9), "JST")
dt_now = datetime.now(JST)


def get_exchange_rate() -> Decimal:
    """
    ドル円為替レート取得

    Returns
    -------
    exchange_rate : Decimal
        ドル円為替レート
    """
    url = os.getenv("CHANGE_RATE_URL")
    if not url:
        logger.error("CHANGE_RATE_URLが設定されていません。")
        return Decimal(0)

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return Decimal(data["values"][0][0])
    except Exception as e:
        logger.error(f"為替レートの取得に失敗しました。エラー: {str(e)}")
        return Decimal(0)


def post_discord(title: str, msg: str, footer: str) -> None:
    """
    discord webhook通知

    Parameters
    ----------
    msg : 送信メッセージ
    """
    url = os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        logger.error("DISCORD_WEBHOOK_URLが設定されていません。")
        return

    body = {
        "content": "@everyone\n",
        "embeds": [{"title": title, "description": msg}],
    }

    if footer:
        body["embeds"][0]["footer"] = {"text": footer}

    try:
        requests.post(url, json=body)
    except Exception as e:
        logger.exception(f"Discordへの通知に失敗しました。エラー: {str(e)}")


def get_total_billing(client) -> dict[str, str | Decimal]:
    """
    AWS使用料金の総額取得

    Parameters
    ----------
    client : boto3.client

    Returns
    -------
    total_billing : dict
        総額情報
        start: 対象期間期初
        end: 対象期間期末
        billing: 総額
    """
    try:
        start_date, end_date = get_total_cost_date_range()
        response = client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["AmortizedCost"],
        )
        # 少数第2で切り上げ
        billing = Decimal(
            response["ResultsByTime"][0]["Total"]["AmortizedCost"]["Amount"]
        ).quantize(Decimal("0.00"), rounding=ROUND_UP)

        return {
            "start": response["ResultsByTime"][0]["TimePeriod"]["Start"],
            "end": response["ResultsByTime"][0]["TimePeriod"]["End"],
            "billing": billing,
        }
    except Exception as e:
        logger.exception(f"資料料金の取得に失敗しました。エラー: {str(e)}")
        return {}


def get_service_billings(client) -> list[dict[str, str | Decimal]]:
    """
    AWS使用料金の総額取得

    Parameters
    ----------
    client : boto3.client

    Returns
    -------
    billings : list[dict]
        各サービス料金内訳
        service_name: サービス名
        billing: サービス料金
    """
    try:
        start_date, end_date = get_total_cost_date_range()
        response = client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["AmortizedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )

        billings = [
            {
                "service_name": item["Keys"][0],
                "billing": Decimal(item["Metrics"]["AmortizedCost"]["Amount"]),
            }
            for item in response["ResultsByTime"][0]["Groups"]
        ]
        return billings
    except Exception as e:
        logger.exception(f"資料料金の取得に失敗しました。エラー: {str(e)}")
        return []


def get_message(total_billing: dict, service_billings: list) -> tuple[str, str, str]:
    """
    Discordへ送信するメッセージの内容を作成

    Parameters
    ----------
    total_billing : dict
        総額情報
    service_billings : list[dict]
        各サービス料金内訳

    Returns
    -------
    title : str
        メッセージタイトル
    details : str
        メッセージ内容
    footer : str
        フッターメッセージ
    """
    start = datetime.strptime(total_billing["start"], "%Y-%m-%d").strftime("%m/%d")
    end_today = datetime.strptime(total_billing["end"], "%Y-%m-%d")
    end_yesterday = end_today.strftime("%m/%d")

    total = total_billing["billing"]
    exchange_rate = get_exchange_rate()

    # タイトル
    if exchange_rate > 0:
        total_yen = (total * exchange_rate).quantize(Decimal("0.00"), rounding=ROUND_UP)
        title = (
            f"{start}～{end_yesterday}の請求額は、￥{total_yen} ({total:.2f} USD)です。"
        )
    else:
        title = f"{start}～{end_yesterday}の請求額は、{total:.2f} USDです。"

    # メッセージボディ
    details = []
    for item in service_billings:
        service_name = item["service_name"]
        billing = Decimal(item["billing"]).quantize(Decimal("0.00"), rounding=ROUND_UP)

        if billing == 0:
            continue

        if exchange_rate > 0:
            billing_yen = (billing * exchange_rate).quantize(
                Decimal("0.00"), rounding=ROUND_UP
            )
            details.append(f"・{service_name}: ￥{billing_yen} ({billing:.2f} USD)")
        else:
            details.append(f"・{service_name}: {billing:.2f} USD")

    # フッターメッセージ
    footer = ""
    if exchange_rate > 0:
        footer = f"※為替レート: {exchange_rate} 円/1ドル ({end_yesterday} 時点)"

    return title, "\n".join(details), footer


def get_total_cost_date_range() -> tuple[str, str]:
    """
    awsから取得する使用料金の期間を返却する
    期間はAPI実行日付の1月前

    Returns
    -------
    start_date : str
        期初
    end_date : str
        期末
    """
    end_date = (dt_now + timedelta(days=-1)).strftime("%Y-%m-%d")
    start_date = (
        get_last_month_first_day() if dt_now.day == 1 else get_this_month_first_day()
    )
    return start_date, end_date


def get_last_month_first_day() -> str:
    """
    システム日付一月前月初日付を取得

    Returns
    -------
    last_month_first_day : str
    """
    last_month = dt_now.month - 1
    last_month_year = dt_now.year
    if last_month == 0:
        last_month = 12
        last_month_year -= 1
    return dt_now.replace(year=last_month_year, month=last_month, day=1).strftime(
        "%Y-%m-%d"
    )


def get_this_month_first_day() -> str:
    """
    システム日付月初日付を取得

    Returns
    -------
    this_month_first_day : str
    """
    return dt_now.replace(day=1).strftime("%Y-%m-%d")


def lambda_handler(event, context) -> None:
    total_billing = get_total_billing(ce_client)
    if not total_billing:
        return
    service_billings = get_service_billings(ce_client)
    title, detail, footer = get_message(total_billing, service_billings)
    post_discord(title, detail, footer)
