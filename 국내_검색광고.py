name: 전체 Supabase 파이프라인
on:
  schedule:
    - cron: '0 15 * * *'    # KST 00:00
    - cron: '0 21 * * *'    # KST 06:00
    - cron: '0 9 * * *'     # KST 18:00
  workflow_dispatch:
    inputs:
      full_refresh:
        description: '전체 기간 갱신'
        required: false
        default: 'false'
        type: choice
        options:
          - 'false'
          - 'true'
      refresh_days:
        description: '갱신 기간'
        required: false
        default: '10'
        type: choice
        options:
          - '7'
          - '10'
          - '30'
          - '180'
          - '365'
          - '730'

jobs:
  # ─── 국내 세트별 (먼저) ───
  kr-adset:
    name: 🇰🇷 국내 세트별
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas yfinance
      - name: Run 국내 세트별
        env:
          META_TOKEN_1: ${{ secrets.META_TOKEN_1 }}
          META_TOKEN_2: ${{ secrets.META_TOKEN_2 }}
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          TOSS_SECRET_KEY: ${{ secrets.TOSS_SECRET_KEY }}
          FULL_REFRESH: ${{ github.event.inputs.full_refresh || 'false' }}
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
        run: python 국내_세트별_supabase.py

  # ─── 국내 소재별 (세트별 끝나면) ───
  kr-creative:
    name: 🎨 국내 소재별
    runs-on: ubuntu-latest
    timeout-minutes: 60
    needs: kr-adset
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas
      - name: Run 국내 소재별
        env:
          META_TOKEN_1: ${{ secrets.META_TOKEN_1 }}
          META_TOKEN_2: ${{ secrets.META_TOKEN_2 }}
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          FULL_REFRESH: ${{ github.event.inputs.full_refresh || 'false' }}
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
        run: python 국내_소재별_supabase.py

  # ─── 🆕 국내 검색광고 (네이버SA + 믹스패널 세션 어트리뷰션) ───
  # ★ 기존 국내_매출.py 와 이름 충돌 방지를 위해 국내_검색광고.py 로 분리
  # ★ naver_sa_daily (+ 옵션 google_ads_daily) 테이블에 직접 upsert
  kr-naver-sa:
    name: 🔍 국내 검색광고 (네이버SA)
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas numpy supabase gspread
      - name: Run 국내 검색광고
        env:
          # ── 네이버 검색광고 API ──
          NAVER_API_KEY: ${{ secrets.NAVER_API_KEY }}
          NAVER_SECRET_KEY: ${{ secrets.NAVER_SECRET_KEY }}
          NAVER_CUSTOMER_ID: ${{ secrets.NAVER_CUSTOMER_ID }}
          # ── 믹스패널 ──
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          # ── Supabase (기존 secret 재사용, 이름만 매핑) ──
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          # ── 구글Ads 집계 동기화 (옵션) ──
          SYNC_GOOGLE_ADS: 'true'
          GCP_SERVICE_ACCOUNT_KEY: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}
          URL_NAV_GOO: ${{ secrets.URL_NAV_GOO }}
          # ── 기간 ──
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
          DRY_RUN: 'false'
        run: python 국내_검색광고.py

  # ─── 글로벌 (국내와 동시 실행) ───
  global:
    name: 🌏 글로벌 Meta+Stripe
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas yfinance stripe
      - name: Run 글로벌
        env:
          META_TOKEN_1: ${{ secrets.META_TOKEN_1 }}
          META_TOKEN_GlobalTT: ${{ secrets.META_TOKEN_GlobalTT }}
          META_TOKEN_4: ${{ secrets.META_TOKEN_4 }}
          META_TOKEN_3: ${{ secrets.META_TOKEN_3 }}
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          STRIPE_API_KEY: ${{ secrets.STRIPE_API_KEY }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          FULL_REFRESH: ${{ github.event.inputs.full_refresh || 'false' }}
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
        run: python 글로벌_세트별_supabase.py

  # ─── 밴스드 세트별 (독립 실행) ───
  vn-adset:
    name: 🎯 밴스드 세트별
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas
      - name: Run 밴스드 세트별
        env:
          META_TOKEN_VANCED: ${{ secrets.META_TOKEN_VANCED }}
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          FULL_REFRESH: ${{ github.event.inputs.full_refresh || 'false' }}
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
        run: python 밴스드_세트별_supabase.py

  # ─── 밴스드 소재별 (세트별 끝나면) ───
  vn-creative:
    name: 🎨 밴스드 소재별
    runs-on: ubuntu-latest
    timeout-minutes: 60
    needs: vn-adset
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install requests pandas
      - name: Run 밴스드 소재별
        env:
          META_TOKEN_VANCED: ${{ secrets.META_TOKEN_VANCED }}
          MIXPANEL_PROJECT_ID: ${{ secrets.MIXPANEL_PROJECT_ID }}
          MIXPANEL_USERNAME: ${{ secrets.MIXPANEL_USERNAME }}
          MIXPANEL_SECRET: ${{ secrets.MIXPANEL_SECRET }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          FULL_REFRESH: ${{ github.event.inputs.full_refresh || 'false' }}
          REFRESH_DAYS: ${{ github.event.inputs.refresh_days || '10' }}
        run: python 밴스드_소재별_supabase.py
