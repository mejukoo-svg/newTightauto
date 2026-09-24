# current-budgets — 예산 컬럼 실시간 조회

추이차트(국내·글로벌)와 날짜탭의 **예산** 컬럼이 "지금 메타에 설정돼 있는 값"을 보이게 하는
읽기 전용 Edge Function. 아무것도 수정하지 않는다.

## 왜 필요했나

예산 컬럼은 원래 파이프라인(`ad_performance_daily.budget` / `global_ad_performance_daily.budget_usd`)이
시간당 한 번 찍는 스냅샷이었다. 그래서 두 가지 문제가 있었다.

1. **증감액을 적용한 직후 화면이 옛 값을 보여준다.** `⚡ 메타에 예산 적용`은 메타를 바로 바꾸지만
   대시보드는 다음 파이프라인 실행까지 모른다.
2. **파이프라인이 activities(예산 변경이력)로 값을 복원하는데, 메타가 최근 이벤트를 조용히
   누락하면 며칠씩 옛 값에 고착된다.** 실측(2026-09-24): 09-23·09-24 두 번 +20% 를 적용했는데
   `120247521906330231` 의 저장값은 09-20~09-24 내내 적용 전 180,000 으로 평탄했다
   (메타 실제값 259,200). activities 응답에 그 세트의 이벤트가 아예 없었다.

→ 화면을 그린 뒤 이 함수로 메타에서 직접 읽어 덮어쓴다. 실패하면 조용히 DB 스냅샷을 쓴다.

## 인터페이스

```
POST /functions/v1/current-budgets
Authorization: Bearer <user JWT>      ← 로그인 세션 필수 (apply-budget 과 동일)
{ "mode": "kr" | "gl" | "vn",
  "items": [ { "adset_id": "1202...", "ad_account_id": "act_1270..." } ] }   // 최대 1000개
```

```jsonc
{ "ok": true, "mode": "kr", "at": "2026-09-25T…Z",
  "budgets": {
    "120248678637570177": { "raw": 137341, "value": 137341, "source": "adset_lifetime" }
  },
  "missing": ["…"],        // 예산을 못 찾은 세트 (삭제됨 / 권한 없음 등)
  "noToken": ["act_…"] }   // 토큰 미설정 계정이 있으면
```

- `value` 는 대시보드 예산 컬럼과 **같은 단위** — 국내·밴스드는 ₩ 그대로, 글로벌은 `raw ÷ 100`.
  (파이프라인의 저장 규칙과 일치시켜야 화면에서 두 값이 섞이지 않는다.)
- `source` = 예산이 실제로 붙어 있던 곳:
  `adset_daily` → `adset_lifetime` → `campaign_daily` → `campaign_lifetime` 순으로 찾는다.
  이 순서는 `apply-budget` 의 `planOne`, 파이프라인의 `budget_resolve.py` 와 동일하다.

## 값 해석 규칙 (셋 다 같은 규칙을 쓴다)

| 상황 | 값 |
|---|---|
| 세트 일예산이 있음 | 그대로 |
| 일정(심야만·오전제외 등) 예약 세트 — 메타가 일예산을 금지하고 총예산만 허용 | `총예산 ÷ 일정기간(일)` |
| ASC/CBO — 예산이 캠페인에 있음 | 캠페인 일예산 (세트마다 같은 값 반복) |
| 캠페인 총예산(CBO lifetime) | `총예산 ÷ 기간`. 기간은 **세트 자신의 일정**을 먼저 쓴다 — 캠페인 `stop_time` 에는 '마지막으로 손댄 시각'이 들어오는 일이 있어 그대로 나누면 일예산이 몇 배로 튄다 |

총예산을 그대로 넣으면 안 된다. 정렬(💸 예산순)과 증감 테두리가 기간 배수만큼 부풀려진다
(실측: 5일짜리 720,000 총예산이 720,000 일예산으로 표시돼 목록 맨 위로 올라갔다).

## 배포

```bash
supabase functions deploy current-budgets --project-ref grtglwavqhvlqcocahao
```

필요한 Edge Secret (apply-budget 과 같은 것을 씁니다 — 이미 설정돼 있으면 추가 작업 없음):
`META_TOKEN_1`, `META_TOKEN_2_1`(또는 `META_TOKEN_2`), `META_TOKEN_GlobalTT`,
`META_TOKEN_ACT_9937`, `META_TOKEN_VANCED`, `SB_SECRET_KEY`(또는 런타임 기본 서비스키).

읽기 전용이라 `ads_read` 스코프만 있으면 충분하다.

## 배포 전에는

함수가 없으면 대시보드는 401/404 를 받고 **조용히 DB 스냅샷 값을 그대로 쓴다**(10분 쿨다운 후 재시도).
예산 컬럼 헤더의 ⚡ 뱃지가 안 보이면 실시간 값이 아니라는 뜻이다.
