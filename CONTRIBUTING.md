# Contributing

Thanks for taking a look at this project. It started as a personal demo,
but issues and pull requests are genuinely welcome - from typo fixes to
new resilience patterns or dashboard panels.

## Getting set up

```bash
git clone https://github.com/mallajaswanthkumar10/realtime-market-pipeline.git
cd realtime-market-pipeline
python -m venv .venv && source .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Bring up the local Kafka + supporting services with `docker-compose up -d`
(see `docker-compose.yml`), then run the producer/consumer/streaming job
as described in the README.

## Before opening a PR

- Run the test suite: `pytest`
- Run the linter: `flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics`
- Both of these also run in CI on every push/PR, so a green run there
  confirms you're good.
- Keep commits focused and the message describing *why*, not just *what*.

## What's especially welcome

- Additional resilience patterns (bulkhead, timeout budgets, etc.) in `utils/`
- More anomaly-detection strategies in `streaming/anomaly_math.py`
- Dashboard improvements in `dashboard/`
- Doc fixes - if something in the README or `docs/` confused you, that's
  a real bug, please open an issue or just fix it

No change is too small. Opening an issue first is fine, but not required -
a draft PR is a perfectly good way to start a conversation.
