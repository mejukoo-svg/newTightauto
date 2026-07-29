# apply-budget — 날짜탭 증감액 → 메타 예산 반영

대시보드 날짜탭에서 `⚡ 메타에 예산 적용`을 누르면 호출되는 Edge Function.
마킹(+20 / +10 / -10 / -20 / OFF)을 실제 Meta 광고세트 예산에 반영한다.

## 왜 서버가 필요한가

`index.html`은 정적 파일이라 소스가 그대로 공개된다(Supabase Auth로 **데이터**는 잠갔지만
HTML 본문 자체는 누구나 받을 수 있다). Meta 쓰기 토큰을 브라우저에 둘 수 없으므로
토큰은 Edge Secret에만 두고, 브라우저는 "어떤 세트를 어떻게" 만 보낸다.

## 동작

1. 호출자의 Supabase 로그인 JWT를 검증한다 (비로그인 = 401).
2. 브라우저가 보낸 `tag`를 하이라이트 테이블의 현재 값과 대조한다.
   불일치하면 그 행은 거부 — 낡은 화면에서 잘못 적용되는 것을 막는다.
3. **메타에서 현재 예산을 다시 읽어** ±%를 계산한다.
   날짜탭의 '예산' 컬럼은 파이프라인 스냅샷 + 통화환산을 거친 표시용 값이라 신뢰하지 않는다.
4. 예산이 실제로 붙어있는 객체를 판별한다.
   - 세트에 `daily_budget`/`lifetime_budget`이 있으면 → 세트 수정
   - 없으면 캠페인 예산(CBO) → **캠페인** 수정 (`scope: "campaign"`)
     `fetch_adset_budgets`가 CBO일 때 캠페인 예산을 세트 예산으로 폴백 표시하므로,
     그 값을 세트에 그대로 쓰면 엉뚱한 곳을 바꾼다.
5. OFF → 세트 `status=PAUSED`. 캠페인은 건드리지 않는다(다른 세트 동반 중단 방지).
   복증(watch) → 변경 없음.
6. 결과를 `budget_apply_log`에 기록한다.

`dryRun: true`(기본)면 아무것도 쓰지 않고 계획만 돌려준다. 대시보드는 먼저 dry-run으로
`현재 → 변경` 표를 띄우고, 사용자가 확인을 눌러야 `dryRun: false`로 다시 호출한다.

## 안전장치

- 세트 ID가 15자리 이상 숫자여야 함 → 같은 테이블을 쓰는 구글 디멘드젠 `ad_group_id`(11자리) 차단
- 한 번에 최대 200개
- 같은 CBO 캠페인에 **서로 다른** 증감률이 마킹되면 양쪽 다 거부(어느 쪽을 따를지 모호)
  같은 증감률이면 1회만 적용
- 변경 후 예산이 0 이하면 거부
- 대시보드는 로그 테이블을 읽기만 가능(쓰기는 service_role 전용)

## 사전 준비

### 1. 로그 테이블

`sql/budget_apply_log.sql`을 Supabase SQL Editor에서 1회 실행.

### 2. ⚠️ META_TOKEN_2 권한

현재 `META_TOKEN_2`의 스코프는 `ads_read` 뿐이라 **쓰기가 안 된다**.
이 토큰이 담당하는 `act_1808141386564262`(타이트사주3rd원화새계정) 세트는
`ads_management`가 붙은 토큰으로 교체하기 전까지 적용이 실패한다.

확인 완료된 토큰: `META_TOKEN_1`, `META_TOKEN_ACT_9937`, `META_TOKEN_SAJU_TW`,
`META_TOKEN_VANCED` → `ads_management` 보유.
`META_TOKEN_GlobalTT`(act_2677·act_1335)는 미확인 — 첫 적용 시 에러 메시지로 드러난다.

스코프 확인:

```bash
curl -s "https://graph.facebook.com/v21.0/debug_token?input_token=$TOKEN&access_token=$TOKEN"
```

시스템 사용자 토큰은 스코프뿐 아니라 해당 광고계정에 **광고주(ADVERTISE) 이상 권한**도 필요하다.

### 3. Edge Secret 등록

```bash
supabase link --project-ref grtglwavqhvlqcocahao
supabase secrets set \
  META_TOKEN_1="..." \
  META_TOKEN_2="..." \
  META_TOKEN_GlobalTT="..." \
  META_TOKEN_ACT_9937="..." \
  META_TOKEN_VANCED="..."
```

`SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY`는 런타임이 자동 주입하므로 등록하지 않는다.

### 4. 배포

```bash
supabase functions deploy apply-budget
```

JWT 검증은 기본값(켜짐) 그대로 둔다 — `--no-verify-jwt`를 쓰지 말 것.

## 계정 → 토큰 매핑

`index.ts`의 `ACC_TOKEN_ENV`는 파이프라인의 `META_TOKENS`와 **같이** 유지해야 한다.
새 광고계정을 추가할 때 아래 4곳을 함께 고친다.

| 파일 | 위치 |
|---|---|
| `국내_세트별_supabase.py` | `META_TOKENS` |
| `글로벌_세트별_supabase.py` | `META_TOKENS` |
| `밴스드_세트별_supabase.py` | `_DEFAULT_VANCED_ACCOUNTS` |
| `supabase/functions/apply-budget/index.ts` | `ACC_TOKEN_ENV` |

## 되돌리기

`budget_apply_log`에 `before_value`가 남는다.

```sql
select applied_at, actor, adset_name, scope, field, before_value, after_value, ok, error
from "new-tightauto".budget_apply_log
order by applied_at desc limit 50;
```
