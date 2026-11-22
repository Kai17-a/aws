### billing-app
- AWSの使用料金をDiscordに通知するLambda

## 開発環境

ツールのバージョンとタスクは [mise](https://mise.jdx.dev/) で管理しています（`mise.toml`）。

```bash
mise install      # uv / prek / betterleaks / aws-sam / cfn-lint を入れる
mise run setup    # 開発用の依存パッケージと Git フックを入れる
mise tasks        # タスクの一覧
```

| タスク | 内容 |
| --- | --- |
| `mise run check` | lint・フォーマット確認・テスト・テンプレート検証・秘密情報の検出をまとめて実行 |
| `mise run test` / `lint` / `format` / `validate` | 個別のチェック |
| `mise run betterleaks` | 作業ツリーの秘密情報を検出（gitignore 対象は除く） |
| `mise run betterleaks:history` | Git の全履歴の秘密情報を検出 |
| `mise run prek` | すべての Git フックを全ファイルに実行 |
| `mise run build` / `deploy` | SAM でビルド・デプロイ（`deploy` は `check` と `build` を先に実行） |

## Git フック

[prek](https://github.com/j178/prek) で管理しています（`prek.toml`）。

- pre-commit: betterleaks による秘密情報の検出、末尾の空白・改行、TOML、秘密鍵、Python の lint とフォーマット、SAM テンプレートの検証
- pre-push: billing-app のテスト

betterleaks の設定は `.betterleaks.toml` です。既定のルールに、Discord の Webhook URL を検出するルールを追加しています。
