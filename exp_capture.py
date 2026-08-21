# -*- coding: utf-8 -*-
"""실험현황 카드 캡처 — 대시보드 화면 그대로를 PNG 로 떠서 퍼포먼스봇이 슬랙에 올린다.

왜 캡처인가: 실험현황 카드는 표(기간 평균)가 본체라, 슬랙 코드블록으로 옮기면
줄바꿈·정렬이 깨지고 색(ROAS 배지)이 사라진다. 화면을 그대로 찍는 편이 읽기 쉽다.
텍스트 요약은 없애지 않고 캡처의 initial_comment 로 함께 보낸다(모바일 알림·검색용).

동작
  · 로컬에 index.html 을 띄우고(대시보드 코드 그대로) 서비스 키로 읽기 인증만 주입 → 로그인 화면 우회.
  · '🧪 실험' 모드 → '실험 현황' 탭 → 세트 ID 로 필터(#esFilter 는 이름+ID 부분일치) → 카드 1장 캡처.
  · 카드에서 아래쪽 블록(추이표·퍼널·그래프)은 잠시 숨긴다. 슬랙에 붙는 그림은
    SECTIONS 개의 .es-wrap 까지(기본 1 = 📊 기간 평균)만 남긴다.

브라우저는 한 번만 띄우고(데이터 로딩이 느리다) 국내·글로벌 캡처를 이어서 찍는다.
자체 실행도 가능: py exp_capture.py kr 6712345,6712346 14 2026-08-20
"""
import functools, http.server, socketserver, sys, threading
from pathlib import Path

BASE = Path(__file__).parent
OUT = BASE / "_expstat_shots"
PORT = 8899
SECTIONS = 1          # 캡처에 남길 .es-wrap 개수 (1=기간 평균 / 2=+추이표)
LOAD_TIMEOUT = 300_000  # 대시보드 데이터 로딩 대기(ms) — 코어 fetch 가 느린 날이 있다


class ExpShooter:
    """대시보드를 한 번 띄워두고 가족 카드를 여러 장 찍는다. 반드시 close() 로 정리."""

    def __init__(self, key, sections=SECTIONS, out=OUT, port=PORT, log=print):
        self.key, self.sections, self.out, self.port, self.log = key, sections, Path(out), port, log
        self._pw = self._br = self._pg = self._srv = None
        self._n = 0
        self._cut = None      # 화면을 잘라 맞춘 기준일(중복 적용 방지)

    # ── 준비: 로컬 서버 + 브라우저 + 인증 주입 + 데이터 로딩 대기 ──
    def _start(self):
        if self._pg:
            return
        from playwright.sync_api import sync_playwright
        self.out.mkdir(exist_ok=True)
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(BASE))
        socketserver.TCPServer.allow_reuse_address = True
        self._srv = socketserver.TCPServer(("127.0.0.1", self.port), handler)
        threading.Thread(target=self._srv.serve_forever, daemon=True).start()
        self._pw = sync_playwright().start()
        self._br = self._pw.chromium.launch()
        self._pg = self._br.new_page(viewport={"width": 1500, "height": 1200}, device_scale_factor=2)
        self._pg.goto(f"http://127.0.0.1:{self.port}/index.html", wait_until="networkidle")
        # 로그인 대신 읽기 인증만 주입 (service_role → RLS 우회, 읽기만 사용)
        self._pg.evaluate("""(key)=>{ SBH.apikey=key; SBH.Authorization='Bearer '+key; showApp(); }""", self.key)
        # 국내·글로벌 배열이 모두 찬 뒤에 찍는다(한 번 띄운 브라우저로 양쪽을 다 캡처하므로)
        self._pg.wait_for_function("typeof KR_AD!=='undefined' && KR_AD.length>0 && "
                                   "typeof GL_AD!=='undefined' && GL_AD.length>0", timeout=LOAD_TIMEOUT)
        self._pg.wait_for_timeout(3000)
        self._pg.evaluate("switchMode('exp')")   # 실험현황 탭은 '🧪 실험' 모드에서만 보인다
        self._pg.wait_for_timeout(800)
        self._pg.click('.tab[data-t="expstat"]')
        self._pg.wait_for_timeout(1500)
        self.log("  [캡처] 대시보드 로딩 완료")

    # ── 캡처 창을 봇의 기준일(dc)에 맞춘다 ──
    def align(self, dc):
        """화면이 쓰는 배열에서 dc 이후 날짜를 잘라낸다.

        대시보드 실험현황 탭은 '최근 N일'을 최신 날짜부터 센다. 봇은 완결일(어제)까지를
        보므로, 자르지 않으면 그림은 오늘(부분일)까지 포함한 값이 되어 함께 올라가는
        텍스트 요약과 숫자가 어긋난다(같은 카드인데 지출·ROAS가 달라 보인다).
        DATES 도 함께 잘라야 기간 컷(_esCollect)이 같은 시작일에서 잡힌다."""
        if not dc or self._cut == dc:
            return
        self._start()
        self._pg.evaluate("""(dc)=>{
            const cut=a=>Array.isArray(a)?a.filter(x=>!x.date||x.date<=dc):a;
            KR_AD=cut(KR_AD); GL_AD=cut(GL_AD); VN_AD=cut(VN_AD);
            DATES=DATES.filter(d=>d<=dc);
        }""", dc)
        self._cut = dc
        self.log(f"  [캡처] 기준일 {dc} 까지로 화면 데이터 정렬")

    # ── 가족 하나 = 카드 한 장 ──
    def shot(self, region, adset_id, days, title=""):
        """세트 ID 로 카드를 찾아 PNG 로 저장. 반환 (Path, title) / 못 찾으면 None."""
        self._start()
        pg = self._pg
        pg.evaluate("""(o)=>{
            const set=(id,v)=>{const e=document.getElementById(id); if(e){e.value=v}};
            set('esSrc',o.src); set('esDays',String(o.days)); set('esSort','spend');
            set('esLimit','0'); set('esMin','0'); set('esMetric','roas');
            const f=document.getElementById('esFilter'); if(f){f.value=o.id}
        }""", {"src": region, "days": days, "id": str(adset_id)})
        pg.evaluate("renderExpStatus()")
        pg.wait_for_timeout(1200)
        n = pg.eval_on_selector_all(".es-card", "els=>els.length")
        if not n:
            self.log(f"  [캡처] 카드 없음 (id={adset_id})")
            return None
        # 카드 상단만 남기고(SECTIONS 개의 .es-wrap 까지) 나머지는 잠시 숨긴 뒤 요소 캡처.
        # (clip 좌표는 뷰포트 기준이라 스크롤된 카드에서 어긋난다 — 요소 캡처가 안전)
        pg.evaluate("""(keep)=>{
            const c=document.querySelector('.es-card');
            c.style.width='max-content'; c.style.maxWidth='none';   // 오른쪽 여백 제거
            // seen = 지금 자식까지 세어 본 .es-wrap 개수. keep 번째 wrap 까지만 남기고
            // 그 뒤의 소제목·표·그래프는 전부 숨긴다(wrap 자신은 seen>keep 일 때부터 숨김).
            let seen=0;
            for(const el of c.children){
                const isWrap=el.classList.contains('es-wrap');
                if(isWrap) seen++;
                if(isWrap ? seen>keep : seen>=keep){ el.dataset.hid='1'; el.style.display='none' }
            }
        }""", self.sections)
        card = pg.query_selector(".es-card")
        card.scroll_into_view_if_needed()
        pg.wait_for_timeout(400)
        self._n += 1
        fn = self.out / f"expstat_{region}_{self._n:02d}.png"
        card.screenshot(path=str(fn))
        pg.evaluate("""()=>{
            const c=document.querySelector('.es-card'); if(!c)return;
            c.style.width=''; c.style.maxWidth='';
            c.querySelectorAll('[data-hid]').forEach(e=>{e.style.display=''; delete e.dataset.hid});
        }""")
        self.log(f"  [캡처] {fn.name}  {title}")
        return (fn, title or str(adset_id))

    def shots(self, region, targets, days, cutoff=None):
        """targets = [{'id':세트ID, 'title':캡션}] → [(Path, title)] (실패분은 조용히 제외).
        cutoff=기준일(YYYY-MM-DD) 을 주면 그날까지의 데이터로 카드를 그린다."""
        out = []
        if cutoff:
            self.align(cutoff)
        for t in targets:
            try:
                r = self.shot(region, t["id"], days, t.get("title", ""))
            except Exception as e:
                self.log(f"  [캡처] 실패 (id={t.get('id')}): {e}")
                r = None
            if r:
                out.append(r)
        return out

    def close(self):
        for fn in (lambda: self._br.close(), lambda: self._pw.stop(), lambda: self._srv.shutdown()):
            try:
                fn()
            except Exception:
                pass
        self._pw = self._br = self._pg = self._srv = None


if __name__ == "__main__":
    env = {}
    for l in (BASE / ".env").read_text(encoding="utf-8").splitlines():
        l = l.strip()
        if l and not l.startswith("#") and "=" in l:
            k, v = l.split("=", 1)
            env.setdefault(k.strip(), v.strip())
    region = sys.argv[1] if len(sys.argv) > 1 else "kr"
    ids = (sys.argv[2] if len(sys.argv) > 2 else "").split(",")
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 14
    cutoff = sys.argv[4] if len(sys.argv) > 4 else None
    sh = ExpShooter(env["SUPABASE_SERVICE_KEY"], log=lambda s: print(s, file=sys.stderr))
    try:
        for f, t in sh.shots(region, [{"id": i} for i in ids if i], days, cutoff):
            print(f)
    finally:
        sh.close()
