# billing-app

AWS の使用料金を Discord に通知する Lambda です。
EventBridge Scheduler で毎月1日 21時(JST)に起動し、前月分の料金をサービスごとに通知します。

- 月初以外に手動で実行した場合は、当月1日から前日までの料金を通知します。
- 為替レートの URL を設定すると、円換算の金額もあわせて通知します。
- 料金は Cost Explorer の速報値です。確定した請求額と異なる場合があります。

## パラメータ

| パラメータ | 必須 | 内容 |
| --- | --- | --- |
| `DiscordWebhookUrl` | ○ | Discord の Webhook URL |
| `ChangeRateUrl` | | ドル円為替レートを返す API の URL。`{"values": [[<レート>]]}` 形式の JSON を返すこと |

## デプロイ・開発

リポジトリのルートで mise のタスクを使います（ルートの README を参照）。

```bash
mise run check
mise run deploy   # 初回は billing-app で `sam deploy --guided` を実行して samconfig.toml を作る
```
