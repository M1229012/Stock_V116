name: Weekly Stock Rank

on:
  schedule:
    # 每週六 10:00 UTC (台灣時間 18:00) 執行一次
    - cron: '0 10 * * 6'
  workflow_dispatch: # 允許手動觸發

jobs:
  run_script:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install pandas requests selenium webdriver-manager lxml
      - name: Run Script
        run: python stock_holder_rank.py
