# apply-budget-google

구글 디멘드젠 탭의 증감액 마킹을 Google Ads 예산에 실제로 반영하는 Edge Function.
메타용 [`apply-budget`](../apply-budget/README.md) 과 요청·응답 계약이 같고,
대시보드는 같은 모달·비밀번호 게이트·재적용 경고를 그대로 쓴다.

```
POST /functions/v1/apply-budget-google      Authorization: Bearer <로그인 JWT>
{ "mode":"gd", "dryRun":true, "items":[{"adset_id":"<ad_group_id>","tag":"up10"}] }
→ { ok, dryRun, actor, plan:[ {adset_id, adset_name, campaign_name, scope, field,
                               before, after, note, error, conflict?, redo?} ] }
```

## 메타와 다른 점

- **구글에는 광고그룹 예산이 없다.** ±% 는 항상 캠페인 예산(`campaignBudgets:mutate`)을 바꾼다.
  OFF 만 광고그룹 상태(`adGroups:mutate`)를 PAUSED 로 바꾼다 — 캠페인은 건드리지 않는다.
- 금액 단위가 **micros**(1원 = 1,000,000). 계획·로그·화면은 전부 '원' 이고 mutate 직전에만 환산한다.
- 로그는 같은 `budget_apply_log` 테이블에 `region='gd'` 로 남는다(메타는 kr/gl/vn).
- 대시보드 마킹은 메타와 같은 `adset_highlights` 테이블에 광고그룹 id 로 들어간다.

## 사전 준비

### 1. Edge Secret

```bash
supabase secrets set \
  G_ADS_DEV_TOKEN="..." G_ADS_CLIENT_ID="..." G_ADS_CLIENT_SECRET="..." \
  G_ADS_REFRESH_TOKEN="..." G_ADS_LOGIN_ID="..." G_ADS_CUSTOMER_ID="..."
```

값은 `newTightauto/.env` 에 같은 이름으로 있다(파이프라인이 쓰는 것과 동일).
`G_ADS_LOGIN_ID` 는 MCC, `G_ADS_CUSTOMER_ID` 는 운영 계정(미설정 시 `5912047700` 폴백).

### 2. 배포

Supabase CLI 가 없으면 Management API 로 올린다(멀티파트).

```
POST https://api.supabase.com/v1/projects/{ref}/functions/deploy?slug=apply-budget-google
  metadata: {"entrypoint_path":"index.ts","name":"apply-budget-google","verify_jwt":true}
  file:     index.ts
```

JWT 검증은 켠 채로 둘 것 — 미인증 POST 는 401 이어야 한다.

## 로컬 검증

```bash
node --experimental-strip-types _verify.mjs   # Node 22+
```

구글·Supabase 를 스텁해 핸들러를 직접 돌린다. **실제 예산은 건드리지 않는다.**
dry-run 쓰기 0건 / micros 환산 / OFF 는 광고그룹만 / 복증 / 같은 캠페인 충돌·1회적용 /
마킹 불일치 / 미존재 광고그룹 / 재적용(redo) / 401·자격증명 누락을 확인한다.

## 안전장치

- 브라우저가 보낸 마킹을 `adset_highlights` 와 대조 — 낡은 화면에서의 오적용 차단
- 같은 캠페인에 서로 다른 증감률이 걸리면 양쪽 거부(한쪽만 선택하면 통과),
  같은 증감률이면 캠페인 예산을 1회만 쓰고 결과를 공유
- 오늘(KST) 이미 적용된 광고그룹은 `redo` 로 표시 → 모달에서 기본 해제·전체선택 제외
- 조회 순서(화면 정렬)를 그대로 돌려준다 — 오류 행이 위로 튀지 않는다

## 되돌리기

```sql
select applied_at, actor, adset_name, scope, field, before_value, after_value, ok, error
from "new-tightauto".budget_apply_log
where region = 'gd' order by applied_at desc limit 50;
```
