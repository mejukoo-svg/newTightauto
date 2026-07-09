"""
알림톡 대시보드(https://tightsaju-alimtalk.vercel.app/)의 데이터를
파싱해서 Supabase(tightsaju-vibe)에 upsert 적재한다.

- 페이지는 HTML 안에 `const DATA = {...}` 로 데이터를 통째로 내려줌 → 그것만 파싱
- Supabase PostgREST upsert(Prefer: merge-duplicates)로 재실행 안전(idempotent)

실행:  py load_alimtalk.py
사전:  .env 에 아래 값이 있어야 함 (없으면 스크립트 상단 상수로 채워도 됨)
       ALIMTALK_USER, ALIMTALK_PASS,
       ALIMTALK_SUPABASE_URL, ALIMTALK_SUPABASE_KEY(service_role)
"""
import json
import os
import sys
import base64
import urllib.request

ALIMTALK_URL = "https://tightsaju-alimtalk.vercel.app/"
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
# 대시보드(index.html)가 읽는 스키마. public 아님 — PostgREST Content-Profile 로 지정.
DB_SCHEMA = "new-tightauto"


def load_env(path):
    """KEY=VALUE 형태 라인만 읽어 dict로. (비ASCII 키·값 지원)"""
    env = {}
    if not os.path.exists(path):
        return env
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def extract_data(html):
    """HTML에서 `const DATA = {...};` 의 JSON 오브젝트를 괄호매칭으로 추출."""
    idx = html.find("const DATA = ")
    if idx < 0:
        raise RuntimeError("HTML에서 'const DATA' 를 찾지 못함 (페이지 구조 변경?)")
    start = html.find("=", idx) + 1
    while html[start] in " \n\t":
        start += 1
    depth = 0
    i = start
    instr = False
    esc = False
    q = ""
    while i < len(html):
        c = html[i]
        if instr:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == q:
                instr = False
        else:
            if c in "\"'":
                instr = True
                q = c
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    break
        i += 1
    return json.loads(html[start:i + 1])


def fetch_html(url, user, pw):
    req = urllib.request.Request(url)
    token = base64.b64encode(f"{user}:{pw}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def flatten(data):
    """DATA → (campaigns, totals, camps, products) 4개 row 리스트로 평탄화."""
    # 발송비용 단가(원/건). 알림톡 대시보드와 동일하게 cost = sent × cost_krw 로 계산.
    cost_krw = int(data.get("cost_krw", 0) or 0)
    campaigns = [
        {"key": c["key"], "label": c.get("label"),
         "intent_label": c.get("intent_label"), "color": c.get("color")}
        for c in data.get("campaigns", [])
    ]
    totals, camps, products = [], [], []
    for date, day in data.get("daily", {}).items():
        tot = day.get("total", {})
        totals.append({"date": date, "n": tot.get("n", 0), "rev": tot.get("rev", 0)})
        for ckey, cv in day.get("camp", {}).items():
            sent = cv.get("sent", 0)
            camps.append({
                "date": date, "campaign_key": ckey,
                "n": cv.get("n", 0), "rev": cv.get("rev", 0),
                "intent_n": cv.get("intent_n", 0), "intent_rev": cv.get("intent_rev", 0),
                "sent": sent, "cost": sent * cost_krw,
            })
            for pname, pv in cv.get("products", {}).items():
                products.append({
                    "date": date, "campaign_key": ckey, "product": pname,
                    "n": pv.get("n", 0), "rev": pv.get("rev", 0),
                })
    return campaigns, totals, camps, products


def upsert(base_url, key, table, rows, conflict):
    if not rows:
        print(f"  {table}: (0 rows) skip")
        return
    url = f"{base_url}/rest/v1/{table}?on_conflict={conflict}"
    body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("apikey", key)
    req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    req.add_header("Content-Profile", DB_SCHEMA)  # public 아닌 new-tightauto 스키마로 쓰기
    req.add_header("Accept-Profile", DB_SCHEMA)
    req.add_header("Prefer", "resolution=merge-duplicates,return=minimal")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            print(f"  {table}: {len(rows)} rows upserted (HTTP {r.status})")
    except urllib.error.HTTPError as e:
        print(f"  {table}: FAILED HTTP {e.code}\n{e.read().decode('utf-8', 'replace')}")
        raise


def main():
    env = load_env(ENV_PATH)

    def cfg(name, default=""):
        return os.environ.get(name) or env.get(name) or default

    user = cfg("ALIMTALK_USER", "saju")
    pw = cfg("ALIMTALK_PASS") or env.get("비밀번호") or ""
    # Supabase 타깃: 전용 ALIMTALK_* 우선, 없으면 기존 파이프라인 공용 시크릿(SUPABASE_*) 폴백.
    sb_url = (cfg("ALIMTALK_SUPABASE_URL") or cfg("SUPABASE_URL")).rstrip("/")
    sb_key = cfg("ALIMTALK_SUPABASE_KEY") or cfg("SUPABASE_SERVICE_KEY")
    # 스키마: SUPABASE_DB_SCHEMA(기존 워크플로우와 동일) 있으면 사용, 없으면 new-tightauto.
    global DB_SCHEMA
    DB_SCHEMA = cfg("SUPABASE_DB_SCHEMA") or DB_SCHEMA

    if not sb_url or not sb_key:
        sys.exit("ERROR: ALIMTALK_SUPABASE_URL/KEY (또는 SUPABASE_URL/SUPABASE_SERVICE_KEY) 가 필요합니다.")
    if not pw:
        sys.exit("ERROR: ALIMTALK_PASS (알림톡 Basic Auth 비밀번호) 가 필요합니다.")

    print(f"[1/3] 알림톡 대시보드 fetch: {ALIMTALK_URL}")
    html = fetch_html(ALIMTALK_URL, user, pw)
    data = extract_data(html)
    print(f"      as_of={data.get('as_of')} range={data.get('range_from')}~{data.get('range_to')}")

    campaigns, totals, camps, products = flatten(data)
    print(f"[2/3] 평탄화: campaigns={len(campaigns)} totals={len(totals)} "
          f"camps={len(camps)} products={len(products)}")

    print(f"[3/3] Supabase upsert → {sb_url}")
    upsert(sb_url, sb_key, "alimtalk_campaign", campaigns, "key")
    upsert(sb_url, sb_key, "alimtalk_daily_total", totals, "date")
    upsert(sb_url, sb_key, "alimtalk_daily_campaign", camps, "date,campaign_key")
    upsert(sb_url, sb_key, "alimtalk_daily_product", products, "date,campaign_key,product")
    print("완료 ✅")


if __name__ == "__main__":
    main()
