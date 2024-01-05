import os
import math
import boto3
import requests
import logging
import yfinance as yf 
from datetime import datetime, timedelta, date
from pandas_datareader import data as pdr

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s")
logger = logging.getLogger()
dt_now = date.today()

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
            "start": (dt_now + timedelta(days=-1)).isoformat(),
            "end": dt_now.isoformat()
        }
        ticker = "JPY=X"
        yf.pdr_override()
        df = pdr.get_data_yahoo(ticker, date_range["start"], date_range["end"])
        exchange_rate = math.ceil(df.iloc[0][3])
        return exchange_rate
    except:
        logger.error("為替レートの取得に失敗しました。")
        return 0

def post_discord(title:str, msg: str) -> None:
    """
    discord webhook通知

    Parameters
    ----------
    msg : 送信メッセージ
    """
    last_month = dt_now + timedelta(days=-1)
    last_month_year = last_month.year
    last_month_month = last_month.month
    if last_month_month == 1:
        last_month_year -= 1
        last_month_month = 12

    url = os.getenv('DISCORD_WEBHOOK_URL')
    body = {
        "content": f"@everyone\n",
        "embeds": [{
            "title": f"***{title}***",
            "description": f"{msg}"
        }]
    }
    requests.post(url, json=body)
    
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
    (start_date, end_date) = get_total_cost_date_range()
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost'
        ]
    )
    return {
        'start': response['ResultsByTime'][0]['TimePeriod']['Start'],
        'end': response['ResultsByTime'][0]['TimePeriod']['End'],
        'billing': response['ResultsByTime'][0]['Total']['AmortizedCost']['Amount'],
    }

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
    (start_date, end_date) = get_total_cost_date_range()

    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
        ]
    )
    billings = []
    for item in response['ResultsByTime'][0]['Groups']:
        billings.append({
            'service_name': item['Keys'][0],
            'billing': item['Metrics']['AmortizedCost']['Amount']
        })
    return billings

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
    """
    start = datetime.strptime(total_billing['start'], '%Y-%m-%d').strftime('%m/%d')

    end_today = datetime.strptime(total_billing['end'], '%Y-%m-%d')
    end_yesterday = (end_today - timedelta(days=1)).strftime('%m/%d')

    total = round(float(total_billing['billing']), 2)
    exchange_rate = get_exchange_rate()
    if exchange_rate > 0:
        total_yen = math.ceil(total * exchange_rate)
        title = f'{start}～{end_yesterday}の請求額は、￥{total_yen} ({total:.2f} USD)です。'
    else:
        title = f'{start}～{end_yesterday}の請求額は、{total:.2f} USDです。'

    details = []
    for item in service_billings:
        service_name = item['service_name']
        billing = round(float(item['billing']), 2)

        if billing == 0.0:
            continue

        if exchange_rate > 0:
            billing_yen = math.ceil(billing * exchange_rate)
            details.append(f'・{service_name}: ￥{billing_yen} ({billing:.2f} USD)')
        else:
            details.append(f'・{service_name}: {billing:.2f} USD')

    return title, '\n'.join(details)

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
    start_date = get_last_month_first_day()
    end_date = get_this_month_first_day()
    return start_date, end_date

def get_last_month_first_day() -> str:
    """
    システム日付一月前月初日付を取得
    
    Returns
    -------
    last_month_first_day : str
        システム日付一月前月初日付
    """
    date_now = dt_now
    last_month = date_now.month - 1
    last_month_year = date_now.year
    if last_month == 0:
        last_month = 12
        last_month_year -= 1
    last_month_first_day = date_now.replace(year=last_month_year ,month=last_month, day=1).isoformat()
    return last_month_first_day

def get_this_month_first_day() -> str:
    """
    システム日付の月初日付を取得
    
    Returns
    -------
    this_month_first_day : str
        システム日付月初日付
    """
    return dt_now.replace(day=1).isoformat()


def lambda_handler(event, context) -> None:
    ce_client = boto3.client('ce', region_name='ap-northeast-1')
    total_billing = get_total_billing(ce_client)
    service_billings = get_service_billings(ce_client)
    (title, detail) = get_message(total_billing, service_billings)
    post_discord(title, detail)
