import os
import math
import boto3
import requests
import logging
import yfinance as yf
from datetime import datetime, timedelta, timezone
from pandas_datareader import data as pdr

logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s"
)
logger = logging.getLogger()
ce_client = boto3.client("ce", region_name="ap-northeast-1")
JST = timezone(timedelta(hours=+9), "JST")
dt_now = datetime.now(JST)


def get_exchange_rate() -> int:
    """
    ドル円為替レート取得

    Returns
    -------
    exchange_rate : int
        ドル円為替レート
    """
    try:
        date_range = {
            "start": (dt_now + timedelta(days=-1)).strftime("%Y-%m-%d"),
            "end": dt_now.strftime("%Y-%m-%d"),
        }
        ticker = "JPY=X"
        yf.pdr_override()
        df = pdr.get_data_yahoo(ticker, date_range["start"], date_range["end"])
        exchange_rate = math.ceil(df.iloc[0][3])
        return exchange_rate
    except Exception as e:
        logger.error(f"為替レートの取得に失敗しました。エラー: {str(e)}")
        return 0


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

    if footer:
        body = {
            "content": f"@everyone\n",
            "embeds": [
                {"title": title, "description": msg, "footer": {"text": footer}}
            ],
        }
    else:
        body = {
            "content": f"@everyone\n",
            "embeds": [{"title": f"{title}", "description": f"{msg}"}],
        }

    try:
        requests.post(url, json=body)
    except Exception as e:
        logger.exception(f"Discordへの通知に失敗しました。エラー: {str(e)}")


def get_total_billing(client):
    """
    AWS使用料金の総額取得

    Parameters
    ----------
    client :

    Retruns
    ----------
    total_billing : dict
        総額情報
        start: 対象期間期初
        end: 対象期間期末
        billing: 総額
    """
    try:
        (start_date, end_date) = get_total_cost_date_range()
        response = client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["AmortizedCost"],
        )
        return {
            "start": response["ResultsByTime"][0]["TimePeriod"]["Start"],
            "end": response["ResultsByTime"][0]["TimePeriod"]["End"],
            "billing": response["ResultsByTime"][0]["Total"]["AmortizedCost"]["Amount"],
        }
    except Exception as e:
        logger.exception(f"資料料金の取得に失敗しました。。エラー: {str(e)}")


def get_service_billings(client):
    """
    AWS使用料金の総額取得

    Parameters
    ----------
    client :

    Retruns
    ----------
    billings : list[dict]
        各サービス料金内訳
        service_name: サービス名
        billing: サービス料金
    """
    try:
        (start_date, end_date) = get_total_cost_date_range()

        response = client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["AmortizedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        billings = []
        for item in response["ResultsByTime"][0]["Groups"]:
            billings.append(
                {
                    "service_name": item["Keys"][0],
                    "billing": item["Metrics"]["AmortizedCost"]["Amount"],
                }
            )
        return billings
    except Exception as e:
        logger.exception(f"資料料金の取得に失敗しました。。エラー: {str(e)}")


def get_message(total_billing: dict, service_billings: list) -> (str, str):
    """
    Discordへ送信するメッセージの内容を作成

    Parameters
    ----------
    total_billing : dict
        総額情報
        start: 対象期間期初
        end: 対象期間期末
        billing: 総額
    service_billings : list[dict]
        各サービス料金内訳
        service_name: サービス名
        billing: サービス料金

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

    total = round(float(total_billing["billing"]), 2)
    exchange_rate = get_exchange_rate()

    # タイトル
    if exchange_rate > 0:
        total_yen = math.ceil(total * exchange_rate)
        title = (
            f"{start}～{end_yesterday}の請求額は、￥{total_yen} ({total:.2f} USD)です。"
        )
    else:
        title = f"{start}～{end_yesterday}の請求額は、{total:.2f} USDです。"

    # メッセージボディ
    details = []
    for item in service_billings:
        service_name = item["service_name"]
        billing = round(float(item["billing"]), 2)

        if billing == 0.0:
            continue

        if exchange_rate > 0:
            billing_yen = math.ceil(billing * exchange_rate)
            details.append(f"・{service_name}: ￥{billing_yen} ({billing:.2f} USD)")
        else:
            details.append(f"・{service_name}: {billing:.2f} USD")

    # フッターメッセージ
    if exchange_rate > 0:
        footer = f"※為替レート: {exchange_rate} 円/1ドル ({end_yesterday} 時点)"

    return title, "\n".join(details), footer


def get_total_cost_date_range() -> (str, str):
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
    start_date = 0
    end_date = (dt_now + timedelta(days=-1)).strftime("%Y-%m-%d")
    if dt_now.day == 1:
        start_date = get_last_month_first_day()
    else:
        start_date = get_this_month_first_day()

    return start_date, end_date


def get_last_month_first_day() -> str:
    """
    システム日付一月前月初日付を取得

    Returns
    -------
    last_month_first_day : str
        システム日付一月前月初日付
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

    Returns
    -------
    this_month_first_day : str
        システム日付月初日付
    """
    return dt_now.replace(day=1).strftime("%Y-%m-%d")


def lambda_handler(event, context) -> None:
    total_billing = get_total_billing(ce_client)
    service_billings = get_service_billings(ce_client)
    (title, detail, footer) = get_message(total_billing, service_billings)
    post_discord(title, detail, footer)
