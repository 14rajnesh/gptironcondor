# Angel One Weekly Iron Condor Paper Bot

This version is Angel One SmartAPI only. It contains no FYERS imports and no FYERS secrets.

## Required GitHub secrets

- ANGEL_API_KEY
- ANGEL_CLIENT_CODE
- ANGEL_PIN
- ANGEL_TOTP_SECRET
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

## Files

- weekly_iron_condor_bot.py — self-contained bot
- requirements.txt — Python dependencies
- .github/workflows/weekly_iron_condor_loop.yml — GitHub Actions workflow

## Logs

CSV logs are written to `data/` and uploaded as the artifact `iron-condor-csv-logs` after each run.

## Telegram commands

- /start — resume new entries
- /stop — pause new entries
- /status — show bot state
- /pnl — show active trade P&L

## Warning

This is paper trading only. It does not place orders.
