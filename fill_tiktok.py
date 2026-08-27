# -*- coding: utf-8 -*-
"""
틱톡 캠페인 추이차트 채우기 (시트 1PYGM70Gse... "시트1" gid=0)

사용법:
  python fill_tiktok.py --spend 2026-08-04=C:\\path\\a.xlsx --spend 2026-08-05=C:\\path\\b.xlsx
  python fill_tiktok.py --spend ... --apply
  python fill_tiktok.py --budget-only --budget C:\\path\\adgroup.xlsx --apply   # 예산 컬럼만 갱신

  매출/판매수 = Mixpanel export API ($insert_id+order_id dedup, KST)
  지출        = 틱톡 광고관리자 리포트 xlsx (지출 컬럼 필수)

시트 그레인 = **광고그룹(Ad Group)**. B열 = 광고그룹 ID. C열 = 예산(설정값).

C열 예산은 광고관리자 리포트의 설정 컬럼에서 읽는다(지출과 달리 날짜별 값이 아니라
'지금 이렇게 설정돼 있다'는 스냅샷이다 — 돌릴 때마다 덮어쓴다).
  · 광고그룹 리포트의 'Ad Group Budget' 이 0 보다 크면 그 값(ABO)
  · 0 이면 그 캠페인이 CBO 라는 뜻이라 캠페인 리포트의 '캠페인 예산' 을
    '(캠페인)' 꼬리표와 함께 넣는다 — 같은 캠페인의 광고그룹 행에 같은 값이
    반복되므로 세로로 더하면 안 된다.
  ⚠ 리포트가 이 예산이 일예산인지 총예산인지 구분해 주지 않는다. 광고관리자에서
    일예산으로 운영 중이라는 전제로 대시보드는 '일예산' 컬럼에 그대로 보여준다.
2026-08-14 까지는 캠페인:광고그룹이 전부 1:1 이었으므로 과거 수치는 그레인 이관과 무관하다.
지출은 광고그룹 레벨 리포트에서 광고그룹 ID 로 직접 읽는다(캠페인 레벨 파일도 받지만,
그 캠페인에 광고그룹이 2개 이상이면 배분 근거가 없어 중단한다).
매출은 MP 에 광고그룹 ID 가 없어(utm_id=캠페인 / utm_content=소재) 캠페인 단위로 받아
광고그룹으로 배분한다 — 그날 지출 광고그룹이 1개면 유일하게 정해지고, 2개 이상인데
매출이 0 이 아니면 소재→광고그룹 매핑(광고 레벨 리포트)이 필요하므로 중단한다.

--apply 없으면 dry-run(계산·검증만).
"""
import argparse, json, sys, datetime, collections, os
import requests, openpyxl
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

KST = datetime.timezone(datetime.timedelta(hours=9))
SA = r"C:\Users\gram\meta_scraper\service_account.json"
SID = "1PYGM70GseCggr6oSxFDgOdw5TUe1IiU0VzKayxM1A4A"
GID = 0
TAB = "시트1"
ENV = r"C:\Users\gram\newTightauto\.env"

# (이름, 광고그룹 ID, 캠페인 ID). 행 순서 = 시트 5행부터.
# 시트에 아직 없는 광고그룹은 실행 시 자동으로 행이 삽입된다(목록 맨 뒤에 추가만 하면 된다).
# 순서는 캠페인 ID 오름차순(생성순) → 그 안에서 광고그룹 ID 오름차순 = 캠페인별로 묶인 상태다.
# 새 광고그룹은 맨 뒤에 붙으므로 묶음이 깨진다. 정렬을 되살리려면 _regroup_tiktok.py 를
# 돌린 뒤 이 목록도 같은 순서로 다시 맞출 것(시트 행 순서와 ROWS 순서가 다르면 실행이 중단된다).
ROWS = [
    ("전체", None, None),
    ("무녀_헤드뱅잉_headbang_2600714", "1870671711953025", "1870671597111538"),
    ("무당_빙수범산_bingsu_260714", "1870672615784066", "1870672615781810"),
    ("무당_애니메이션모음_ASC", "1872037864274802", "1872037759235186"),
    ("무당_aiUGC_ASC", "1872128852227409", "1872128852227393"),
    ("무당_애니메이션모음_ASC(2)", "1872955572107265", "1872955572106417"),
    ("무녀_피안애니메이션모음_ASC", "1873119493389858", "1873119493389842"),
    ("무당_aiUGC_ASC(2-1)", "1873303138939010", "1873303138937266"),
    ("무당_aiUGC_ASC(2-2)", "1873574070480033", "1873303138937266"),
    ("무당_aiUGC_ASC(2-3)", "1873845018996865", "1873303138937266"),
    ("무당_애니메이션모음_ASC(3-2)", "1873303422916706", "1873303422916690"),
    ("무당_애니메이션모음_ASC(3-1)", "1873650543987842", "1873303422916690"),
    ("1%_0812_1%전환소재들", "1873308114121729", "1873308019491889"),
    ("무녀_피안애니메이션모음_ASC(1-1)", "1873573699433905", "1873573699433889"),
    ("무녀_피안애니메이션모음_ASC(1-2)", "1873935803512994", "1873573699433889"),
    # --- 2026-08-20 신규 (08/20 광고그룹 리포트 실측) ---
    # 기존 캠페인의 복사 광고그룹 — 캠페인 ID 는 형제 광고그룹에서 그대로 물려받는다.
    ("무당_애니메이션모음_ASC(3-3)", "1874027254261266", "1873303422916690"),
    ("무당_애니메이션모음_ASC(3-4)", "1874027254261858", "1873303422916690"),
    ("무당_애니메이션모음_ASC(3-5)", "1874027254262914", "1873303422916690"),
    ("무당_aiUGC_ASC(2-4)", "1874028853097138", "1873303138937266"),
    ("무당_aiUGC_ASC(2-5)", "1874028853098498", "1873303138937266"),
    ("무녀_피안애니메이션모음_ASC(1-3)", "1874029022819346", "1873573699433889"),
    ("무녀_피안애니메이션모음_ASC(1-4)", "1874029022820466", "1873573699433889"),
    ("무녀_피안애니메이션모음_ASC(1-5)", "1874029022823762", "1873573699433889"),
    # 신규 캠페인 4개 — 2026-08-24 캠페인 레벨 리포트로 진짜 캠페인 ID 를 확인해
    # 대용키(첫 광고그룹 ID)를 교체했다. 대용키였던 동안은 캠페인 레벨 리포트의 캠페인 ID 와
    # 안 맞아 load_spend 의 `cid not in CIDS` 에서 이 4개 캠페인 지출이 조용히 버려졌다.
    ("구미호_0819_ASC", "1873936928573601", "1873936871185570"),
    ("외모정병_0819_ASC", "1873938593488098", "1873938558940257"),
    ("무당_0820_신규전환모음(1-1)", "1874026288642113", "1874026254620802"),
    ("무당_0820_신규전환모음(1-2)", "1874028716506385", "1874026254620802"),
    ("무당_0820_신규전환모음의(1-3)", "1874028716508545", "1874026254620802"),
    ("무당_0820_신규전환모음의(1-4)", "1874028716509297", "1874026254620802"),
    ("무당_0820_신규전환모음의(1-5)", "1874028716512545", "1874026254620802"),
    ("무녀_0820_피안2차(1-1)", "1874027641849442", "1874027568799361"),
    ("무녀_0820_피안2차(1-2)", "1874028446470626", "1874027568799361"),
    ("무녀_0820_피안2차의(1-3)", "1874028446472594", "1874027568799361"),
    ("무녀_0820_피안2차의(1-4)", "1874028446474818", "1874027568799361"),
    ("무녀_0820_피안2차의(1-5)", "1874028446476002", "1874027568799361"),
    # --- 2026-08-26 신규 캠페인 2개 (08/26 첫 지출 · 광고그룹 리포트 실측) ---
    ("무당_소재수십개ASC_세트A", "1874574395129122", "1874574320593378"),
    ("무녀_소재여러개_ASC_a", "1874577359594753", "1874577345364225"),
]

# 캠페인 ID -> 캠페인 이름. 시트 A열은 캠페인명과 광고그룹명이 다를 때 두 줄로 병기한다
# (광고그룹을 복사하면 이름이 (2-1)/(2-2) 로 갈려 그룹명만으론 소속 캠페인을 알 수 없다).
# 이름은 광고관리자 리포트 기준 2026-08-19 실측값 — 틱톡에서 이름을 바꾸면 여기도 갱신할 것.
CAMP_NAME = {
    "1870671597111538": "무녀_헤드뱅잉_2600714",
    "1870672615781810": "무당_빙수범산_260714",
    "1872037759235186": "무당_애니메이션모음_ASC",
    "1872128852227393": "무당_aiUGC_ASC",
    "1872955572106417": "무당_애니메이션모음_ASC(2)",
    "1873119493389842": "무녀_피안애니메이션모음_ASC",
    "1873303138937266": "무당_aiUGC_ASC(2)",
    "1873303422916690": "무당_애니메이션모음_ASC(3)",
    "1873308019491889": "1%_0812_1%전환소재들",
    "1873573699433889": "무녀_피안애니메이션모음_ASC(1-1)",
    # 아래 4개는 2026-08-24 캠페인 레벨 리포트로 진짜 캠페인 ID 를 확인해 대용키를 교체했다.
    "1873936871185570": "구미호_0819_ASC",
    "1873938558940257": "외모정병_0819_ASC",
    "1874026254620802": "무당_0820_신규전환모음",
    "1874027568799361": "무녀_0820_피안2차",
    "1874574320593378": "무당_소재수십개ASC",
    "1874577345364225": "무녀_소재여러개_ASC",
}


def sheet_label(name, cid):
    """시트 A열 라벨 = **광고그룹명 단독**(2026-08-20~, 사용자 요청).

    시트 그레인이 광고그룹이고 B열이 광고그룹 ID 라 캠페인은 시트에서 뺐다.
    광고그룹명 자체가 (2-1)/(3-2) 식으로 계보를 담고 있어 이름만으로 서로 구분된다
    (14개 전수 확인 · 중복 없음). CAMP_NAME 은 CBO 예산을 캠페인명으로 찾을 때
    계속 쓰이므로 지우지 말 것 — 시트 표시에서만 빠진 것이다.
    """
    return name
# 시트 컬럼(0-based). 2026-08-18 예산 컬럼을 C 에 끼워 넣으며 합계·날짜가 한 칸씩 밀렸다.
COL_ID = 1                # B: 광고그룹 ID
COL_BUD = 2               # C: 예산(설정 스냅샷)
COL_TOT = 3               # D: 최근 7일 평균(2026-08-27~ · 그전엔 전기간 합계)
COL_D0 = 4                # E~: 날짜(최신 좌측)
BUD_HDR = "예산"
AVG_DAYS = 7              # D열 평균 창(최근 N개 날짜 열)
AVG_HDR = f"최근 {AVG_DAYS}일 평균"

NROW = len(ROWS)          # 헤더(4행) 아래 데이터 행 수
LASTROW = 4 + NROW        # 마지막 데이터 행 번호(1-based)
AIDS = [a for _, a, _ in ROWS if a]                       # 광고그룹 ID (시트 키)
CIDS = sorted({c for _, _, c in ROWS if c})               # 캠페인 ID
NAME_BY_AID = {a: n for n, a, _ in ROWS if a}
CAMP_BY_AID = {a: c for _, a, c in ROWS if a}
AIDS_BY_CAMP = collections.defaultdict(list)
for _n, _a, _c in ROWS:
    if _a:
        AIDS_BY_CAMP[_c].append(_a)
SRC = "tiktok"            # utm_source 가 이 값인 결제만 인정
WD = ["월", "화", "수", "목", "금", "토", "일"]

BG_CYAN = {"red": 0.0, "green": 1.0, "blue": 1.0}
BG_GREEN = {"red": 0.7019608, "green": 1.0, "blue": 0.7019608}
BG_YELLOW = {"red": 1.0, "green": 1.0, "blue": 0.6}
BG_LIGHTRED = {"red": 1.0, "green": 0.8, "blue": 0.8}
BG_DEEPRED = {"red": 1.0, "green": 0.6, "blue": 0.6}
BLACK = {"red": 0.0, "green": 0.0, "blue": 0.0}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
HDR_BLUE = {"red": 0.101960786, "green": 0.4509804, "blue": 0.9098039}


def hdr_cell(text):
    """4행 헤더 셀. updateCells 가 서식 필드를 덮어쓰므로 반드시 서식을 함께 준다
    (안 주면 파란 배경·흰 굵은 글씨가 초기화된다)."""
    return {"userEnteredValue": {"stringValue": text},
            "userEnteredFormat": {"backgroundColorStyle": {"rgbColor": HDR_BLUE},
                                  "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                                  "textFormat": {"fontSize": 10, "bold": True,
                                                 "foregroundColorStyle": {"rgbColor": WHITE}}}}

GREEN_TXT = {"red": 0.101960786, "green": 0.41960785, "blue": 0.101960786}
RED_TXT = {"red": 0.8666667, "green": 0.0, "blue": 0.0}
BLUE_TXT = {"red": 0.0, "green": 0.0, "blue": 0.8666667}


def roas_bg(roas):
    if roas >= 300: return BG_CYAN
    if roas >= 200: return BG_GREEN
    if roas >= 100: return BG_YELLOW
    if roas > 0:    return BG_LIGHTRED
    return BG_DEEPRED


def cell_text(spend, revenue, cnt):
    """일자 셀은 정수, D열(최근 7일 평균) 셀은 소수가 섞여 들어온다.

    금액은 원 단위로 반올림해 쓰고, 판매수는 float 이면 소수 1자리로 쓴다
    (평균을 정수로 반올림하면 주 1~2건짜리 광고그룹이 '0건' 으로 사라진다).
    순이익은 반올림한 지출·매출의 차로 계산해야 화면상 세 줄이 서로 맞는다.
    """
    roas = round(revenue / spend * 100) if spend else 0
    s_i, r_i = round(spend), round(revenue)
    profit = r_i - s_i
    cs = f"{cnt:,.1f}" if isinstance(cnt, float) else f"{cnt:,}"
    return (f"{roas}\n {profit:+,}\n {-s_i:,}\n {r_i:,}\n {cs}건"), roas, profit


def avg_cells(cells):
    """최근 AVG_DAYS 개 날짜 열(왼쪽=최신)의 **일평균** -> (지출, 매출, 판매수).

    분모는 항상 AVG_DAYS 고정이다(2026-08-27 사용자 결정). 빈칸(그날 지출 0)도 0 으로
    세서 나눈다 — 집행일 수로 나누면 행마다 분모가 달라 '전체' 행과 세로합이 안 맞고
    행끼리 '하루당 얼마' 비교가 성립하지 않는다. 어제 시작한 광고그룹이 1/7 로 희석돼
    보이는 건 의도된 성질이다(run-rate).
    ROAS 는 7일 지출·매출 **합**의 비율 = 평균끼리의 비율이라 자동으로 가중평균이 된다.
    반환값을 여기서 미리 반올림한다 — 시트에 쓰는 값과 검증에서 비교하는 값이
    글자 단위로 같아야 재실행 때 '불일치' 로 멈추지 않는다.
    """
    win = cells[:AVG_DAYS]
    sp = sum(x[0] for x in win if x)
    rv = sum(x[1] for x in win if x)
    cn = sum(x[2] for x in win if x)
    return round(sp / AVG_DAYS), round(rv / AVG_DAYS), round(cn / AVG_DAYS, 1)


def cell_data(spend, revenue, cnt):
    """CellData(값+줄별 서식+배경) 생성"""
    txt, roas, profit = cell_text(spend, revenue, cnt)
    L = txt.split("\n")
    idx, acc = [], 0
    for s in L:
        idx.append(acc)
        acc += len(s) + 1
    runs = [
        {"startIndex": idx[0], "format": {"fontSize": 11, "bold": True, "foregroundColorStyle": {"rgbColor": BLACK}}},
        {"startIndex": idx[1], "format": {"fontSize": 11, "bold": False,
                                          "foregroundColorStyle": {"rgbColor": GREEN_TXT if profit >= 0 else RED_TXT}}},
        {"startIndex": idx[2], "format": {"fontSize": 9, "bold": False, "foregroundColorStyle": {"rgbColor": RED_TXT}}},
        {"startIndex": idx[3], "format": {"fontSize": 9, "bold": False, "foregroundColorStyle": {"rgbColor": BLUE_TXT}}},
        {"startIndex": idx[4], "format": {"fontSize": 9, "bold": False, "foregroundColorStyle": {"rgbColor": BLACK}}},
    ]
    runs[0].pop("startIndex")
    return {
        "userEnteredValue": {"stringValue": txt},
        "userEnteredFormat": {"backgroundColorStyle": {"rgbColor": roas_bg(roas)},
                              "horizontalAlignment": "LEFT", "verticalAlignment": "MIDDLE",
                              "textFormat": {"fontSize": 11, "bold": False},
                              "wrapStrategy": "CLIP"},
        "textFormatRuns": runs,
    }


def parse_cell(s):
    """'292\\n +137,703\\n -71,697\\n 209,400\\n 4건' -> (spend, revenue, cnt) / 빈칸이면 None"""
    if not s or not s.strip():
        return None
    L = [x.strip() for x in s.split("\n")]
    if len(L) != 5:
        raise ValueError(f"예상 밖 셀 포맷: {s!r}")
    spend = abs(int(L[2].replace(",", "").replace("+", "")))
    revenue = int(L[3].replace(",", ""))
    t = L[4].replace("건", "").replace(",", "")
    cnt = float(t) if "." in t else int(t)   # D열 평균 셀은 소수 1자리
    return spend, revenue, cnt


# ---------------- 예산: 설정 컬럼 읽기 ----------------
ADG_BUD_KEYS = ["ad group budget", "광고 그룹 예산", "광고그룹 예산", "adgroup budget"]
CAMP_BUD_KEYS = ["캠페인 예산", "campaign budget"]
CAMP_NAME_KEYS = ["캠페인 이름", "campaign name"]


def _budnum(v):
    if v is None:
        return None
    t = str(v).replace(",", "").replace("₩", "").strip()
    if t in ("", "-", "None"):
        return None
    try:
        return int(round(float(t)))
    except ValueError:
        return None


def load_budgets(paths):
    """설정 xlsx(광고그룹/캠페인 리포트) → {광고그룹ID: 시트에 넣을 문자열}.

    광고그룹 리포트와 캠페인 리포트를 같이 주면 CBO 캠페인까지 채워진다.
    광고그룹 리포트만 주면 CBO 광고그룹은 예산 0 이라 빈칸으로 남는다.
    """
    adg, camp_by_name = {}, {}
    for path in paths:
        wb = openpyxl.load_workbook(path)
        ws = wb[wb.sheetnames[0]]
        rows = list(ws.iter_rows(values_only=True))
        hdr = [str(h).strip() if h is not None else "" for h in rows[0]]
        low = [h.lower() for h in hdr]

        def find(keys):
            for exact in (True, False):
                for i, h in enumerate(low):
                    if (any(k == h for k in keys) if exact else any(k in h for k in keys)):
                        return i
            return None

        i_adg, i_ab = find(ADG_KEYS), find(ADG_BUD_KEYS)
        i_cn, i_cb = find(CAMP_NAME_KEYS), find(CAMP_BUD_KEYS)
        got = 0
        for r in rows[1:]:
            if i_adg is not None and i_ab is not None and r[i_adg] is not None:
                aid = str(r[i_adg]).strip()
                v = _budnum(r[i_ab])
                if aid.isdigit() and v is not None:
                    adg[aid] = v
                    got += 1
            if i_cn is not None and i_cb is not None and r[i_cn] is not None:
                v = _budnum(r[i_cb])
                if v is not None:
                    camp_by_name[str(r[i_cn]).strip()] = v
                    got += 1
        kind = ("광고그룹" if i_ab is not None else "") + ("/캠페인" if i_cb is not None else "")
        print(f"💰 {os.path.basename(path)}: {kind or '예산 컬럼 없음'} {got}건")
        if not kind:
            print(f"   실제 헤더: {hdr}")

    out = {}
    for _n, aid, cid in ROWS:
        if not aid:
            continue
        v = adg.get(aid)
        cv = camp_by_name.get(CAMP_NAME.get(cid, ""))
        if v:
            out[aid] = f"{v:,}"
        elif v == 0 and cv:
            out[aid] = f"{cv:,} (캠페인)"          # CBO — 같은 캠페인 행끼리 중복이라 합산 금지
        # v == 0 인데 캠페인 예산을 못 찾은 경우(= 광고그룹 리포트만 준 CBO 광고그룹)는
        # 시트 값을 그대로 둔다. 예전엔 "0" 을 써서 이미 들어가 있던 "300,000 (캠페인)" 을
        # 덮어 지웠다 — docstring 이 말하는 '빈칸으로 남는다' 와도 어긋났다.
        # v is None(리포트에 없는 광고그룹) → 시트 값 유지
    return out


# ---------------- 지출: Campaign / Ad group Report xlsx ----------------
COST_KEYS = ["지출", "비용", "총 비용", "cost", "spend", "amount spent"]
CID_KEYS = ["캠페인 id", "campaign id", "campaign_id"]
ADG_KEYS = ["광고 그룹 id", "광고그룹 id", "adgroup id", "ad group id", "adgroup_id"]
DATE_KEYS = ["날짜", "date", "일자", "stat time", "time"]


def load_spend(path, forced_date):
    """지출 xlsx → {날짜: {광고그룹ID: 지출}}.

    광고그룹 레벨 리포트면 광고그룹 ID 로 직접 읽는다(권장).
    캠페인 레벨 리포트면 그 캠페인의 광고그룹이 ROWS 에 1개뿐일 때만 그 광고그룹에 넣고,
    2개 이상이면 배분 근거가 없으므로 중단한다(조용히 한쪽에 몰면 ROAS 가 왜곡된다).
    """
    wb = openpyxl.load_workbook(path)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    hdr = [str(h).strip() if h is not None else "" for h in rows[0]]
    low = [h.lower() for h in hdr]

    def find(keys, skip=()):
        # 정확일치를 부분일치보다 우선한다. 광고그룹 리포트엔 '지출' 앞에 '최소 지출 타겟'이
        # 있어서 부분일치로 먼저 잡으면 전 캠페인 지출이 0 으로 읽힌다(2026-08-16 실측).
        for exact in (True, False):
            for i, h in enumerate(low):
                if any(s in h for s in skip):
                    continue
                if any(k == h for k in keys) if exact else any(k in h for k in keys):
                    return i
        return None

    ci_cost, ci_cid = find(COST_KEYS), find(CID_KEYS)
    ci_adg = find(ADG_KEYS)
    # 'Date Created'(캠페인 생성일)를 일자 컬럼으로 오인하면 안 된다 — 일별 브레이크다운 컬럼만 인정.
    ci_date = find(DATE_KEYS, skip=("created", "생성", "일정"))
    # 광고그룹 레벨 리포트엔 '캠페인 ID' 컬럼이 아예 없는 포맷도 있다(2026-08-16 실측).
    # 광고그룹 ID 만 있으면 ROWS 로 캠페인을 역인할 수 있으므로 그 경우는 통과시킨다.
    if ci_cost is None or (ci_cid is None and ci_adg is None):
        raise SystemExit(
            f"\n❌ 지출 컬럼을 못 찾음: {os.path.basename(path)}\n"
            f"   실제 헤더: {hdr}\n"
            f"   → 이 파일은 캠페인 '설정' 내보내기입니다. 광고관리자에서 '지출' 지표가 포함된\n"
            f"      Campaign Report 를 날짜별로 다시 내보내 주세요.\n")
    print(f"📄 {os.path.basename(path)}: 지출='{hdr[ci_cost]}' 캠페인='{hdr[ci_cid] if ci_cid is not None else '(없음)'}'"
          + (f" 광고그룹='{hdr[ci_adg]}'" if ci_adg is not None else " (광고그룹 컬럼 없음)"))

    out = collections.defaultdict(dict)          # 날짜 → 광고그룹ID → 지출
    unknown = collections.Counter()
    for r in rows[1:]:
        cid = str(r[ci_cid]).strip() if (ci_cid is not None and r[ci_cid] is not None) else ""
        try:
            cost = int(round(float(str(r[ci_cost]).replace(",", "").replace("₩", "").strip())))
        except (TypeError, ValueError):
            continue
        d = forced_date
        if ci_date is not None and r[ci_date]:
            v = r[ci_date]
            d = (v.strftime("%Y-%m-%d") if isinstance(v, datetime.datetime) else str(v)[:10])

        if ci_adg is not None:
            aid = str(r[ci_adg]).strip()
            if not aid.isdigit():
                continue                 # '총 N개 결과' 합계 행 등 (ID 가 '-')
            if aid not in AIDS:
                if cost > 0:
                    unknown[(aid, cid or "?", cost)] += 1
                continue
            cid = cid or CAMP_BY_AID[aid]
        else:
            if cid not in CIDS:
                continue
            cand = AIDS_BY_CAMP[cid]
            if len(cand) > 1:
                raise SystemExit(
                    f"\n❌ 캠페인 레벨 리포트인데 캠페인 {cid} 의 광고그룹이 {len(cand)}개 — 중단\n"
                    f"   ({', '.join(cand)})\n"
                    f"   지출을 광고그룹별로 나눌 근거가 없습니다.\n"
                    f"   → 광고그룹 레벨 리포트로 다시 내보내 주세요.\n")
            aid = cand[0]
        out[d][aid] = out[d].get(aid, 0) + cost

    if unknown:
        print("   ⚠️ ROWS 에 없는 광고그룹인데 지출이 있음 → ROWS 에 추가해야 합니다:")
        for (aid, cid, cost) in sorted(unknown):
            print(f"      광고그룹 {aid} (캠페인 {cid}) 지출 {cost:,}")
        raise SystemExit("❌ 누락 광고그룹 있음 — 중단")

    for d in sorted(out):
        print(f"   [{d}] 광고그룹 지출:")
        for aid, c in sorted(out[d].items(), key=lambda x: -x[1]):
            if c:
                print(f"      {aid}  {NAME_BY_AID[aid]:28} {c:>9,}  (캠페인 {CAMP_BY_AID[aid]})")
    return out


# ---------------- 매출: Mixpanel export API ----------------
def mp_fetch(cfg, lo, hi, tries=3):
    """MP export 스트리밍 수신. 한 번에 여러 날을 받으면 read timeout 이 잦아 3일씩 쪼갠다."""
    lines = []
    cur, end_d = datetime.date.fromisoformat(lo), datetime.date.fromisoformat(hi)
    while cur <= end_d:
        chunk_hi = min(cur + datetime.timedelta(days=2), end_d)
        for t in range(tries):
            try:
                r = requests.get("https://data.mixpanel.com/api/2.0/export",
                                 params={"from_date": cur.isoformat(), "to_date": chunk_hi.isoformat(),
                                         "event": json.dumps(["결제완료", "payment_complete"]),
                                         "project_id": cfg.get("MIXPANEL_PROJECT_ID", "3390233")},
                                 auth=(cfg["MIXPANEL_USERNAME"], cfg["MIXPANEL_SECRET"]),
                                 timeout=(30, 600), stream=True)
                r.raise_for_status()
                got = [l for l in r.iter_lines(decode_unicode=True) if l and l.strip()]
                print(f"   MP {cur}~{chunk_hi}: {len(got):,}줄", flush=True)
                lines += got
                break
            except Exception as e:
                print(f"   ⚠️ MP {cur}~{chunk_hi} 실패({t+1}/{tries}): {type(e).__name__}", flush=True)
                if t == tries - 1:
                    raise
        cur = chunk_hi + datetime.timedelta(days=1)
    return lines


def ad_to_adgroup(cid, ad, d):
    """소재(Ad) ID -> 광고그룹 ID.

    틱톡 ID 는 생성 시각순으로 증가한다(스노플레이크형). 광고는 자기 광고그룹보다 나중에
    만들어지므로 **그 소재 ID 이하인 광고그룹 중 가장 큰 것**이 소속 광고그룹이다.
    광고그룹을 복사하면 소재도 새 ID 로 다시 만들어져 이 규칙이 성립한다
    (2026-08-16 검증: 캠페인 1873303422916690 의 랜딩 이벤트 소재 ID 가
     18733035xx(08/12 광고그룹) 과 1873650xxx(08/16 복사 광고그룹) 로 갈린다).
    """
    cands = sorted(AIDS_BY_CAMP[cid], key=int)
    if not ad.isdigit():
        print(f"      [{d}] 소재 ID 가 숫자가 아님({ad!r}) -> 최초 광고그룹 {cands[0]} 로 귀속")
        return cands[0]
    pick = None
    for a in cands:
        if int(ad) >= int(a):
            pick = a
    if pick is None:
        print(f"      [{d}] 소재 {ad} 가 모든 광고그룹보다 작음 -> 최초 광고그룹 {cands[0]} 로 귀속")
        pick = cands[0]
    print(f"      소재 {ad} -> 광고그룹 {pick} ({NAME_BY_AID[pick]}) "
          f"[캠페인 {cid} 후보 {', '.join(cands)}]")
    return pick


def load_revenue(dates, spend):
    """{날짜: {광고그룹ID: [매출, 건수]}}.

    MP 에는 광고그룹 ID 가 없다(utm_id=캠페인 / utm_content=소재) → 캠페인 단위로 모은 뒤
    그날 **지출이 있는 광고그룹**으로 배분한다. 1개면 유일하게 정해지고, 2개 이상인데 매출이
    0 이 아니면 소재→광고그룹 매핑(광고 레벨 리포트)이 없이는 나눌 수 없으므로 중단한다.
    """
    cfg = {}
    for line in open(ENV, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    d0 = min(dates)
    lo = (datetime.date.fromisoformat(d0) - datetime.timedelta(days=2)).isoformat()  # KST 경계 버퍼
    hi = max(dates)
    seen_ins, seen_ord = set(), set()
    direct = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    by_cid = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    by_ad = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    dropped = []
    unmatched = collections.defaultdict(lambda: [0, 0])
    for l in mp_fetch(cfg, lo, hi):
        p = json.loads(l)["properties"]
        ins = p.get("$insert_id")
        if ins:
            if ins in seen_ins:
                continue
            seen_ins.add(ins)
        # utm_id 는 2026-08-17 부터 **캠페인 ID 와 광고그룹 ID 가 섞여** 온다(틱톡 태깅 변경).
        # 광고그룹 ID 로 오면 소재 추정 없이 그대로 귀속하고, 캠페인 ID 면 종전대로 배분한다.
        # 캠페인 ID 만 대조하면 갈라진 캠페인 매출이 통째로 사라진다(08/18 447,600원 11건 실측).
        uid = str(p.get("utm_id") or "")
        aid_direct = uid if uid in AIDS else None
        cid = CAMP_BY_AID[uid] if aid_direct else uid
        if not aid_direct and cid not in CIDS:
            # utm_source 가 틱톡인데 ROWS 의 광고그룹·캠페인 어느 쪽과도 안 맞는 ID.
            # 대개 ROWS 에 아직 없는 신규 광고그룹이거나, 대용키를 쓴 캠페인의 진짜
            # 캠페인 ID 다. 조용히 버리면 매출 누락을 못 잡으므로 모아서 출력한다.
            if str(p.get("utm_source") or "").lower() == SRC and p.get("order_id") is not None:
                du = datetime.datetime.fromtimestamp(float(p["time"]), KST).strftime("%Y-%m-%d")
                t = unmatched[(du, uid or "(없음)")]
                t[0] += int(float(p.get("amount") or 0)); t[1] += 1
            continue
        # utm_id 는 틱톡 캠페인인데 utm_source 가 다른 결제가 있다(크로스셀·라스트터치 오귀속).
        # 과거에도 utm_source=google 건을 손으로 제외해 왔으므로 여기서 일괄 차단하고, 무엇이
        # 빠졌는지 반드시 출력한다(조용히 버리면 매출 누락을 못 잡는다).
        src = str(p.get("utm_source") or "").lower()
        if src != SRC:
            dropped.append((datetime.datetime.fromtimestamp(float(p["time"]), KST), cid,
                            p.get("amount"), src or "(없음)"))
            continue
        # order_id 없는 이벤트 = 클라이언트측 중복 발화(amount 가 문자열·기기 프로퍼티 보유).
        # 전부 order_id 보유 결제 1~4초 뒤에 뜨고, 일부는 1~2시간 뒤 재발화. 결제 원장이 아니므로 제외.
        oid = p.get("order_id")
        if oid is None:
            continue
        if oid in seen_ord:
            continue
        seen_ord.add(oid)
        d = datetime.datetime.fromtimestamp(float(p["time"]), KST).strftime("%Y-%m-%d")
        if d not in dates:
            continue
        amt = int(float(p.get("amount") or 0))       # 매출 = MP amount
        if aid_direct:
            t = direct[d][aid_direct]
            t[0] += amt; t[1] += 1
            continue
        a = by_cid[d][cid]
        a[0] += amt; a[1] += 1
        b = by_ad[d][(cid, str(p.get("utm_content") or "(없음)"))]
        b[0] += amt; b[1] += 1
    hit = {k: v for k, v in unmatched.items() if k[0] in dates}
    if hit:
        print(f"\n⚠️  ROWS 와 매칭 안 된 틱톡 utm_id (매출에서 빠졌습니다 — ROWS 확인 필요):")
        for (d, uid), (amt, cn) in sorted(hit.items()):
            print(f"   [{d}] utm_id={uid} {amt:,}원 {cn}건")
    if dropped:
        print(f"\n⚠️  utm_source≠{SRC} 라 제외한 결제 {len(dropped)}건:")
        for t, cid, amt, src in sorted(dropped):
            if t.strftime("%Y-%m-%d") in dates:
                print(f"   {t:%m-%d %H:%M} {cid} {amt} (utm_source={src})")

    # 캠페인 매출 → 광고그룹 배분
    out = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    for d in dates:
        if direct[d]:
            print(f"\n   [{d}] utm_id 가 광고그룹 ID 로 온 매출(추정 없이 직접 귀속):")
            for aid, (rv, cn) in sorted(direct[d].items(), key=lambda x: -x[1][0]):
                print(f"      광고그룹 {aid} {NAME_BY_AID[aid]:26} {rv:>9,}원 {cn}건")
                t = out[d][aid]
                t[0] += rv; t[1] += cn
        print(f"\n   [{d}] 소재(utm_content)별 매출:")
        for (cid, ad), (rv, cn) in sorted(by_ad[d].items(), key=lambda x: -x[1][0]):
            print(f"      소재 {ad:<20} {rv:>9,}원 {cn}건 (캠페인 {cid})")
        for cid, (rv, cn) in by_cid[d].items():
            live = [a for a in AIDS_BY_CAMP[cid] if spend[d].get(a, 0) > 0]
            if len(live) == 1:
                t = out[d][live[0]]
                t[0] += rv; t[1] += cn
                continue
            if rv == 0:
                continue                              # 나눌 매출이 없으니 무해
            # 광고그룹이 갈라진 캠페인 -> 소재(utm_content) 단위로 광고그룹에 귀속
            # (사용자 결정 2026-08-16: 08/15 부터 소재 집계 후 광고그룹 합산).
            for (c2, ad), (arv, acn) in sorted(by_ad[d].items()):
                if c2 != cid or arv == 0:
                    continue
                aid = ad_to_adgroup(cid, ad, d)
                if spend[d].get(aid, 0) == 0:
                    print(f"      [{d}] 소재 {ad} 의 광고그룹 {aid} 는 그날 지출 0 "
                          f"-> 매출 {arv:,}원 {acn}건 은 빈칸 규칙으로 제외됨")
                t = out[d][aid]
                t[0] += arv; t[1] += acn
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spend", action="append", default=[],
                    metavar="YYYY-MM-DD=path.xlsx", help="날짜별 Campaign Report xlsx")
    ap.add_argument("--budget", action="append", default=[], metavar="path.xlsx",
                    help="예산 설정이 든 리포트 xlsx(광고그룹/캠페인). C열 예산을 갱신한다")
    ap.add_argument("--budget-only", action="store_true",
                    help="날짜 값은 건드리지 않고 C열 예산만 갱신")
    ap.add_argument("--note", default="", help="2행 주석에 이어붙일 문장")
    ap.add_argument("--recalc", action="store_true",
                    help="날짜·예산은 그대로 두고 D열(최근 7일 평균)만 다시 계산")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    if not a.spend and not a.budget_only and not a.recalc:
        sys.exit("❌ --spend 가 없습니다 (예산만 갱신하려면 --budget-only --budget <xlsx>,\n                 D열만 다시 계산하려면 --recalc)")
    budgets = load_budgets(a.budget) if a.budget else {}
    if a.budget_only and not budgets:
        sys.exit("❌ --budget-only 인데 --budget 에서 읽은 예산이 없습니다")

    spend = {}
    for item in a.spend:
        d, path = item.split("=", 1)
        for dd, m in load_spend(path, d).items():
            spend.setdefault(dd, {}).update(m)
    dates = sorted(spend)
    print(f"📅 대상 날짜: {dates}")

    rev = load_revenue(dates, spend) if dates else {}
    for d in dates:
        print(f"\n--- {d} ---")
        for name, aid, _c in ROWS[1:]:
            s = spend[d].get(aid, 0)
            r_, c_ = rev[d].get(aid, [0, 0])
            if s == 0:
                # 지출 0 = 그날 미집행(일시중지·종료) 캠페인. 매출이 잡혀도 과거 클릭의
                # 라스트터치 잔여 귀속이라 ROAS 가 성립하지 않는다 → 빈칸, 전체에서도 제외.
                extra = f" — 매출 {r_:,}원 {c_}건 있으나 지출 0이라 제외" if r_ else ""
                print(f"  {name:26} (빈칸 — 지출 0{extra})")
            else:
                print(f"  {name:26} 지출 {s:>9,} | 매출 {r_:>9,} | {c_}건 | ROAS {round(r_/s*100)}%")

    cr = Credentials.from_service_account_file(SA, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets", "v4", credentials=cr)
    vals = svc.spreadsheets().values().get(
        spreadsheetId=SID, range=f"{TAB}!A4:AZ{LASTROW}").execute().get("values", [])
    hdr = vals[0]
    # ⚠️ 이미 채운 날짜를 또 주면 열이 '삽입'돼 같은 날짜 열이 두 개가 된다.
    # 2026-08-27 실제로 08/25 가 중복 생성됐다(전 세션이 이미 채워 둔 걸 모르고 재실행).
    # D열이 전기간 합계였을 땐 중복분이 합계에 그대로 더해져도 검증을 통과해 버려서
    # 조용히 과대계상됐다. 다시 채우려면 _undo_dates.py 로 먼저 지울 것.
    dup = [d for d in dates
           if f"{datetime.date.fromisoformat(d):%m/%d}" in
              " ".join(str(h) for h in hdr[COL_D0:])]
    if dup:
        sys.exit(f"\n❌ 이미 시트에 있는 날짜입니다 — 중단: {', '.join(dup)}\n"
                 f"   그대로 두면 같은 날짜 열이 두 개 생깁니다(열은 삽입만 됩니다).\n"
                 f"   다시 채우려면 먼저: python _undo_dates.py "
                 f"{' '.join(f'{datetime.date.fromisoformat(d):%m/%d}' for d in dup)} --apply")

    # C4 가 '예산'이면 이관 완료된 시트. 아니면 이번 실행에서 C 에 컬럼을 끼워 넣는다.
    hasbud = len(hdr) > COL_BUD and str(hdr[COL_BUD]).strip() == BUD_HDR
    if not hasbud:
        print(f"\n➕ 예산 컬럼(C) 신규 삽입 — 합계·날짜가 한 칸씩 오른쪽으로 밀립니다")

    # 시트에 실제로 존재하는 데이터 행(A/B 열이 채워진 행). ROWS 에 새 캠페인을 추가하면
    # 여기서 부족분을 감지해 아래에 행을 삽입한다.
    # 데이터 블록 아래에는 빈 행 + 범례 행이 있다. ROWS 가 늘어 스캔 범위(LASTROW)가
    # 그 아래까지 내려가면 범례가 캠페인 행으로 오인되므로 첫 빈 행에서 끊는다.
    sheet_rows = []
    for r in vals[1:]:
        if not any(str(c).strip() for c in r[:2]):
            break
        sheet_rows.append(r)
    nhave = len(sheet_rows)
    have = [str(r[1]).strip() if len(r) > 1 else "" for r in sheet_rows][1:]  # 전체행 제외
    want = [a for _, a, _ in ROWS[1:]]
    # 구 시트는 B열이 캠페인 ID 였다. 08/14 까지 캠페인:광고그룹이 전부 1:1 이므로 값은 그대로
    # 두고 키만 광고그룹 ID 로 바꿔 이관한다(위치 기준 — 순서가 맞는지 캠페인 ID 로 먼저 검증).
    want_camp = [c for _, _, c in ROWS[1:]]
    migrate = None
    if have and have == want_camp[:len(have)]:
        migrate = want[:len(have)]
        print("\n🔀 B열 이관: 캠페인 ID → 광고그룹 ID (값 변화 없음, 08/14 까지 1:1)")
        for i, (old, new) in enumerate(zip(have, migrate)):
            print(f"   {ROWS[1+i][0]:28} {old} → {new}")
    elif have != want[:len(have)]:
        sys.exit(f"❌ 시트 행 순서가 ROWS 와 다름 — 중단\n   시트: {have}\n"
                 f"   ROWS(광고그룹): {want}\n   ROWS(캠페인): {want_camp}")
    missing = ROWS[1 + len(have):]
    if missing:
        print("\n➕ 시트에 없는 신규 광고그룹 → 행 추가: " + ", ".join(n for n, _, _ in missing))

    # 기존 일자 셀 파싱 + D열 검증 (새로 삽입할 행은 검증 대상 아님).
    # D열은 2026-08-27 부터 '전기간 합계' 가 아니라 '최근 7일 평균' 이다. 전환 첫 실행에서는
    # 시트에 아직 구 합계가 들어 있으므로, 두 규칙 중 어느 쪽과 맞는지 먼저 판정한 뒤
    # 전 행을 같은 규칙으로 검증한다(행마다 규칙이 갈리면 그건 진짜 오염이다).
    existing, chk = [], []
    for ri in range(1, NROW + 1):
        if ri > nhave:
            existing.append([])
            chk.append(None)
            continue
        row = vals[ri] + [""] * (max(len(hdr), COL_D0) - len(vals[ri]))
        # 예산 컬럼이 아직 없는 시트(이관 전)는 D열·날짜가 한 칸 왼쪽에 있다.
        cells = [parse_cell(c) for c in row[(COL_D0 if hasbud else COL_D0 - 1):]]
        cur = parse_cell(row[COL_TOT if hasbud else COL_TOT - 1])
        tot = (sum(x[0] for x in cells if x), sum(x[1] for x in cells if x),
               sum(x[2] for x in cells if x))
        chk.append((ROWS[ri - 1][0], cur, avg_cells(cells), tot))
        existing.append(cells)

    seen = [c for c in chk if c and c[1] is not None]
    legacy = bool(seen) and all(cur == tot for _n, cur, _av, tot in seen)
    if legacy:
        print(f"\n🔀 D열 전환: '전기간 합계' → '{AVG_HDR}'(분모 {AVG_DAYS} 고정) — 이번 실행에서 갱신됩니다")
    for c in chk:
        if not c or c[1] is None:
            continue
        name, cur, av, tot = c
        exp = tot if legacy else av
        ok = cur == exp
        print(f"검증 {name:26} D열={cur} 기대={exp} {'✅' if ok else '❌'}")
        if not ok:
            sys.exit("❌ D열 불일치 — 중단 (열이 어긋났거나 손으로 고친 셀이 있는지 확인)")

    # 새 날짜별 값
    new_cells = {}
    for d in dates:
        col = []
        tot_s = tot_r = tot_c = 0
        for name, aid, _c in ROWS[1:]:
            s = spend[d].get(aid, 0)
            r_, c_ = rev[d].get(aid, [0, 0])
            if s == 0:                      # 미집행 광고그룹 — 빈칸 & 전체 합계에서도 제외
                col.append(None)
                continue
            tot_s += s; tot_r += r_; tot_c += c_
            col.append((s, r_, c_))
        new_cells[d] = [(tot_s, tot_r, tot_c)] + col

    if not a.apply:
        print("\n(dry-run) --apply 를 붙이면 시트에 반영합니다.")
        return

    reqs = []
    # 예산 컬럼 삽입은 맨 앞에 — 뒤따르는 요청들이 전부 '밀린 뒤' 인덱스를 쓴다.
    if not hasbud:
        reqs.append({"insertDimension": {
            "range": {"sheetId": GID, "dimension": "COLUMNS",
                      "startIndex": COL_BUD, "endIndex": COL_BUD + 1},
            "inheritFromBefore": False}})
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 3, "columnIndex": COL_BUD},
            "rows": [{"values": [hdr_cell(BUD_HDR)]}],
            "fields": "userEnteredValue,userEnteredFormat.backgroundColorStyle,"
                      "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,"
                      "userEnteredFormat.textFormat"}})

    # B열 이관(캠페인 ID → 광고그룹 ID). 행 삽입보다 먼저 — 기존 행 위치 기준이다.
    if migrate:
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 5, "columnIndex": 1},
            "rows": [{"values": [{"userEnteredValue": {"stringValue": aid}}]} for aid in migrate],
            "fields": "userEnteredValue"}})
        # 4행 헤더 라벨도 같이 바꾼다(안 바꾸면 '캠페인 ID' 아래 광고그룹 ID 가 들어가 오해를 부른다).
        # ⚠️ A4 는 '광고그룹' 또는 '캠페인' 으로 시작해야 한다 — 대시보드 _ttParseSheet 가
        #    /^(광고그룹|캠페인)/ 로 헤더 행을 찾는다(2026-08-20 이전엔 '캠페인' 만 인식했다).
        #    안 지키면 시트 파싱이 null 이 되어 하드코딩 스냅샷(08/09까지)으로 조용히 폴백한다.
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 3, "columnIndex": 0},
            "rows": [{"values": [hdr_cell("광고그룹 ＼ 날짜"), hdr_cell("광고그룹 ID")]}],
            "fields": "userEnteredValue,userEnteredFormat.backgroundColorStyle,"
                      "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,"
                      "userEnteredFormat.textFormat"}})

    # 신규 광고그룹 행을 삽입한다(열 삽입보다 앞서야 이후 요청이 늘어난 행을 본다).
    # 0-based: 마지막 데이터 행(4+nhave, 1-based)의 바로 아래 = 4+nhave
    if missing:
        start = 4 + nhave
        reqs.append({"insertDimension": {
            "range": {"sheetId": GID, "dimension": "ROWS",
                      "startIndex": start, "endIndex": start + len(missing)},
            "inheritFromBefore": True}})
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": start, "columnIndex": 0},
            "rows": [{"values": [{"userEnteredValue": {"stringValue": sheet_label(n, c)}},
                                 {"userEnteredValue": {"stringValue": aid}}]}
                     for n, aid, c in missing],
            "fields": "userEnteredValue"}})

    # 오래된 날짜부터 E 에 삽입 → 최종적으로 최신이 맨 왼쪽
    for d in dates:
        reqs.append({"insertDimension": {"range": {"sheetId": GID, "dimension": "COLUMNS",
                                                   "startIndex": COL_D0, "endIndex": COL_D0 + 1},
                                         "inheritFromBefore": False}})
        reqs.append({"copyPaste": {
            "source": {"sheetId": GID, "startRowIndex": 3, "endRowIndex": LASTROW, "startColumnIndex": COL_D0 + 1, "endColumnIndex": COL_D0 + 2},
            "destination": {"sheetId": GID, "startRowIndex": 3, "endRowIndex": LASTROW, "startColumnIndex": COL_D0, "endColumnIndex": COL_D0 + 1},
            "pasteType": "PASTE_FORMAT"}})
        dt = datetime.date.fromisoformat(d)
        rowdata = [{"values": [hdr_cell(f"{dt:%m/%d} ({WD[dt.weekday()]})")]}]
        for v in new_cells[d]:
            rowdata.append({"values": [cell_data(*v) if v else
                                       {"userEnteredValue": {"stringValue": ""}}]})
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 3, "columnIndex": COL_D0},
            "rows": rowdata,
            "fields": "userEnteredValue,userEnteredFormat.backgroundColorStyle,"
                      "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,"
                      "userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,textFormatRuns"}})

    # D열 = 최근 AVG_DAYS 일 평균 갱신(2026-08-27~ · 그전엔 전기간 합계였다).
    # 새 날짜 열은 오래된 날부터 E 에 끼워 넣으므로 반영 후 순서는 [최신 → 과거] 다.
    # 여기서도 같은 순서로 이어 붙여야 '최근 7개 열' 창이 실제 시트와 일치한다.
    totrows = [{"values": [hdr_cell(AVG_HDR)]}]
    for ri in range(NROW):
        series = [new_cells[d][ri] for d in reversed(dates)] + existing[ri]
        totrows.append({"values": [cell_data(*avg_cells(series))]})
    win = [f"{datetime.date.fromisoformat(d):%m/%d}" for d in reversed(dates)]
    win += [str(h) for h in hdr[COL_D0 if hasbud else COL_D0 - 1:]]
    print(f"\n📊 D열 = {AVG_HDR} (분모 {AVG_DAYS} 고정) · 창: {', '.join(win[:AVG_DAYS])}")
    reqs.append({"updateCells": {
        "start": {"sheetId": GID, "rowIndex": 3, "columnIndex": COL_TOT},
        "rows": totrows,
        "fields": "userEnteredValue,userEnteredFormat.backgroundColorStyle,"
                  "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,"
                  "userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy,textFormatRuns"}})

    # C열 예산 — 리포트에 있는 광고그룹만 덮어쓴다(없는 행은 시트 값 유지).
    if budgets:
        # 컬럼을 끼워 넣으면 오른쪽(합계) 컬럼의 ROAS 배경색을 그대로 물려받는다.
        # 예산은 ROAS 와 무관하니 데이터 구간을 흰색으로 되돌린다.
        reqs.append({"repeatCell": {
            "range": {"sheetId": GID, "startRowIndex": 4, "endRowIndex": LASTROW,
                      "startColumnIndex": COL_BUD, "endColumnIndex": COL_BUD + 1},
            "cell": {"userEnteredFormat": {"backgroundColorStyle":
                     {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}}},
            "fields": "userEnteredFormat.backgroundColorStyle"}})
        for ri, (_n, aid, _c) in enumerate(ROWS):
            if not aid or aid not in budgets:
                continue
            reqs.append({"updateCells": {
                "start": {"sheetId": GID, "rowIndex": 4 + ri, "columnIndex": COL_BUD},
                "rows": [{"values": [{"userEnteredValue": {"stringValue": budgets[aid]}}]}],
                "fields": "userEnteredValue"}})
        print(f"\n💰 예산 기록: {len(budgets)}개 광고그룹")

    # 제목 / 주석
    last = datetime.date.fromisoformat(max(dates)) if dates else None
    if last:
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 0, "columnIndex": 0},
            "rows": [{"values": [{"userEnteredValue": {"stringValue": f"🎵 틱톡 캠페인 추이차트 2026-07-14 ~ {last:%m-%d} (KST)"}}]}],
            "fields": "userEnteredValue"}})
    if a.note:
        old = svc.spreadsheets().values().get(spreadsheetId=SID, range=f"{TAB}!A2").execute().get("values", [[""]])[0][0]
        reqs.append({"updateCells": {
            "start": {"sheetId": GID, "rowIndex": 1, "columnIndex": 0},
            "rows": [{"values": [{"userEnteredValue": {"stringValue": old + " · " + a.note}}]}],
            "fields": "userEnteredValue"}})

    svc.spreadsheets().batchUpdate(spreadsheetId=SID, body={"requests": reqs}).execute()
    print(f"\n✅ 반영 완료: {dates or '(날짜 변경 없음)'} · 합계 {ndays}일")


if __name__ == "__main__":
    main()
