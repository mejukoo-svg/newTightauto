# hourly-roas — 추이차트 셀 클릭 → 세트 × 그 날짜의 시간별 ROAS

추이차트(국내·글로벌·밴스드·🇹🇼 대만·보조지표)에서 **세트 행의 날짜 셀**을 누르면 화면이
시간별 화면으로 바뀌고, 그 세트의 그날 **1시간 단위 ROAS·지출·매출**을 그린다.
이 함수가 그 화면의 데이터를 만든다.

## 왜 서버가 필요한가 · 왜 테이블이 아닌가

- 시간별 그레인은 DB에 없다. `kr_channel_revenue_4h` 는 `채널 × 4시간` 이라 세트로 쪼갤 수 없다.
- 세트-시간 테이블을 새로 적재하면 DDL + cron + 백필이 붙고, **적재 안 된 날짜는 빈 화면**이 된다.
  반대로 원천(Meta insights / Mixpanel export)은 아무 과거 날짜나 즉시 조회된다.
  → 클릭할 때 그때그때 원천을 읽는다. 저장하지 않으므로 백필도 창(window) 제한도 없다.
- Meta 토큰·Mixpanel 시크릿을 브라우저에 둘 수 없다(`index.html` 은 정적 파일 = 소스 공개).
  `apply-budget` 과 같은 이유로 서버 한 겹을 둔다.

## 데이터 기준

| 항목 | 원천 | 비고 |
|---|---|---|
| 지출 | Meta insights `{adset_id}/insights`, `breakdowns=hourly_stats_aggregated_by_advertiser_time_zone`, `time_increment=1` | 광고주 TZ=KST → 시각이 곧 KST |
| 매출 | Mixpanel export `결제완료`/`payment_complete` 중 `utm_term`=adset_id + Meta 계열 `utm_source` | 파이프라인(국내/글로벌/밴스드 세트별)과 같은 귀속 규칙 |
| dedup | `order_id`(utm_term 보유 우선 → revenue 큰 것) → `$insert_id` → `(date,distinct_id,서비스)` | export 는 `$insert_id` 중복을 안 걸러 준다 |
| 통화 | 국내·밴스드 KRW / 글로벌 USD | 추이차트 `norm()` 표시 통화와 동일 |

**추이차트 일별 셀과 다를 수 있는 지점** (응답 `notes` 로 화면에 그대로 표시된다):

- 크로스셀 UTM 백필(`distinct_id` 라스트터치 재귀속)은 일별 파이프라인에만 있다 → 여기 합계가 더 낮을 수 있다.
- 환율이 실시간(open.er-api.com)이다. 파이프라인은 그날의 일별 환율을 쓴다.
- 글로벌 `GL_META_DAYS`(메타 보고값 치환 날짜)는 셀이 Meta 기준, 이 화면은 Mixpanel 기준이다.

화면 상단에 **일별 셀 값 대비 합계 차이(%)** 를 같이 띄우므로 괴리는 눈으로 확인된다.

실측(2026-08-23, 배포 직후 종단 확인):

| 세트 | 지출 | 매출 |
|---|---|---|
| 국내 `무녀_ASC` (₩242.8만) | **+0.0%** (완전 일치) | **+1.1%** |
| 글로벌 `대만_무당_[복제]ASC` ($1,316) | +1.2% | **−26.9%** |

글로벌 매출이 낮은 건 예상된 차이다 — 글로벌 파이프라인은 오가닉 결제를 **직전 Meta 방문의
라스트터치로 세트에 상속**시키는데(`글로벌_세트별_supabase.py`의 `_src=='organic'` 처리),
이 함수는 결제 이벤트 자신의 `utm_term` 만 본다. 시간대별 **모양(패턴)** 은 유효하고,
**절대액은 일별 셀을 기준으로 볼 것**.

## 요청 / 응답

```
POST /functions/v1/hourly-roas
Authorization: Bearer <로그인 JWT>
{ "mode": "kr" | "gl" | "vn", "adset_id": "1234...", "ad_account_id": "act_...", "date": "2026-08-23" }

→ { ok, currency, hours: [{h, spend, revenue, purchases, impressions, clicks} × 24],
    totals: {...}, notes: [...] }
```

`ad_account_id` 는 토큰을 고르는 데만 쓴다. 비면 가진 토큰을 차례로 시도한다(소유 계정 토큰만 200).

## 시크릿

Meta 토큰은 `apply-budget` 이 이미 쓰는 것을 그대로 재사용한다. **Mixpanel 3개만 새로 필요**하다.

```bash
supabase secrets set --project-ref grtglwavqhvlqcocahao \
  MIXPANEL_USERNAME='<서비스 계정 사용자명>' \
  MIXPANEL_SECRET='<서비스 계정 시크릿>' \
  MIXPANEL_PROJECT_ID='3390233'
```

값은 로컬 `newTightauto/.env` 및 GitHub Secrets 에 이미 있는 것과 같다.

## 배포

```bash
cd newTightauto
supabase functions deploy hourly-roas --project-ref grtglwavqhvlqcocahao
```

JWT 검증은 함수 안에서 직접 한다(`--no-verify-jwt` 를 쓰지 말 것 — 비로그인 401).

## 비용·한계

- 클릭 1회 = Meta 1콜 + Mixpanel export 1콜(첫 클릭만). **같은 날짜**를 다른 세트로 다시 누르면
  isolate 메모리 캐시(10분)로 Mixpanel 을 재호출하지 않는다. 브라우저에도 `(세트,날짜)` 캐시가 있다.
- 응답 시간 실측 **13~25초**(그 날짜의 결제 이벤트 전량을 받아 거르는 비용). 화면은 그동안 로딩 문구를 띄운다.
- Mixpanel export 는 시간당 60쿼리 제한. 서로 다른 날짜를 60번 이상 열면 429 → 재시도 후 실패 메시지.
- `where` 절은 쓰지 않는다. 조건이 붙으면 날짜창이 조용히 무시되는 사례가 있어(memory:
  `mixpanel-export-where-gotchas`) 파이프라인처럼 전량 받아 함수 안에서 필터한다.
