// ===== CONFIG =====
const SB_URL='https://grtglwavqhvlqcocahao.supabase.co';
const SB_KEY='sb_publishable_EwuoG13apMFGKp973Rv8vA_3yNVpHwm';
const SBH={'apikey':SB_KEY,'Authorization':'Bearer '+SB_KEY,'Accept-Profile':'new-tightauto','Content-Profile':'new-tightauto'};
// ===== Supabase Auth (대시보드 로그인) =====
// 공유 계정 1개로 게이트. Supabase Auth에 아래 이메일로 사용자를 만들고, 그 비밀번호가 새 대시보드 암호가 됨.
const DASH_EMAIL='dashboard@newtightauto.app';
const SBC=supabase.createClient(SB_URL,SB_KEY,{auth:{persistSession:true,autoRefreshToken:true}});
// 로그인 세션의 access token으로 모든 읽기/쓰기를 인증 → 공개 anon 키만으론 RLS가 차단.
// onAuthStateChange는 로그인/토큰자동갱신/로그아웃 시 SBH 헤더를 항상 최신으로 유지.
SBC.auth.onAuthStateChange((_e,session)=>{SBH.Authorization='Bearer '+(session?session.access_token:SB_KEY);});
// 세션이 죽으면(만료·갱신실패) 헤더가 공개키로 폴백되고, 공개키엔 RLS 권한이 없어 '코어 테이블 전부 401' 이 된다.
//   그동안은 아무 조치 없이 빨간 배너만 떠서 새로고침을 몇 번 해도 같은 화면이 반복됐다(2026-08-23 실제 사고).
//   · authRefresh(): 401 시점에 토큰 갱신 1회 시도 (동시 401 이 9개여도 갱신 요청은 한 번만)
//   · authLost():    갱신마저 실패하면 조용히 두지 말고 로그인 화면으로 되돌린다
let _authRefreshP=null,_AUTH_LOST=false;
function authRefresh(){
  if(!_authRefreshP)_authRefreshP=SBC.auth.refreshSession().then(({data,error})=>{
    _authRefreshP=null;
    if(error||!data||!data.session)return false;
    SBH.Authorization='Bearer '+data.session.access_token;return true;
  }).catch(()=>{_authRefreshP=null;return false});
  return _authRefreshP;
}
function authLost(msg){
  if(_AUTH_LOST)return;_AUTH_LOST=true;
  SBH.Authorization='Bearer '+SB_KEY;
  try{SBC.auth.signOut()}catch(e){}
  const ac=document.getElementById('appContent');if(ac)ac.classList.remove('ready');
  const ls=document.getElementById('loginScreen');if(ls)ls.style.display='flex';
  const er=document.getElementById('loginErr');if(er)er.textContent=msg||'세션이 만료되었습니다. 다시 로그인해 주세요.';
  const pw=document.getElementById('loginPw');if(pw){pw.value='';pw.focus()}
  const w=document.getElementById('coreWarn');if(w)w.remove();
}

// ===== 매월 매출 목표 (대시보드 배너용) — 매달 새로 추가/수정 =====
//   국내(kr) = KRW,  글로벌(gl) = USD. 글로벌 탭은 지표가 전부 달러라 목표도 달러로 둔다.
//   기존 KRW 목표는 그 달 실효환율(실적KRW÷실적USD)로 환산해 달성률이 바뀌지 않게 옮겼다:
//     4.5억÷1479.6=$304,128 → $304,000 · 6억÷1530.6=$392,007 → $392,000
//     16억÷1495.7=$1,069,733 → $1,070,000  (반올림 오차 0.05%p 미만)
const MONTHLY_REVENUE_GOAL={
  kr:{
    '2026-05': 1500000000,  // 국내: 15억
    '2026-06': 1700000000,  // 국내: 17억
    '2026-07': 1700000000,  // 국내: 17억
    '2026-08': 2200000000,  // 국내: 22억
  },
  gl:{
    '2026-05': 304000,      // 글로벌: $304K (구 4.5억)
    '2026-06': 392000,      // 글로벌: $392K (구 6억)
    '2026-07': 1070000,     // 글로벌: $1.07M (구 16억)
    '2026-08': 1100000,     // 글로벌: $1.1M
  },
};

const HL_CONFIG={
  up20:{cls:'hl-up20',pct:20,label:'+20%',bg:'#66eecc'},up10:{cls:'hl-up10',pct:10,label:'+10%',bg:'#88dd88'},
  down10:{cls:'hl-down10',pct:-10,label:'-10%',bg:'#ffcccc'},down20:{cls:'hl-down20',pct:-20,label:'-20%',bg:'#ff7777'},
  // 연한 회색 = 예산 -50% (감액 중 가장 강한 단계 — OFF 직전). 2026-08-23 추가.
  down50:{cls:'hl-down50',pct:-50,label:'-50%',bg:'#d0d0d0'},
  off:{cls:'hl-off',pct:null,label:'OFF',bg:'#000000'},watch:{cls:'hl-watch',pct:0,label:'복증',bg:'#ff9900'},
};

// ===== STATE =====
let MODE='kr'; // 'kr', 'gl', or 'cr'
console.log('[DBG] script loaded at',new Date().toISOString(),'commit=e8ecd40+');
let KR_AD=[],GL_AD=[],CR_AD=[],GL_CR=[],VN_AD=[],STRIPE_DATA=[],TOSS_DATA=[];
let CORE_FAIL={};   // 이번 로드에서 실패한 코어 테이블 {라벨: 사유} — 상단 경고 배너로 노출
let COUNTRY='ALL';  // 국가 필터 (vn/gl 전용). ALL=전체(국가행을 adset당 합산)
let NSA_DAILY=[],GOOGLE_ADS=[],GOOGLE_DG=[],NAVER_MP=[],NAVER_PL=[],NAVER_KW=[],NSA_KW=[],TOSS_DAILY=[],GGDG_CT=[],GGDG_SP=[],GGDG_TIGHT=[];
// 구글 전 캠페인(google_campaign_daily) — 매출탭 구글 채널 분할(국내/대만 × 검색/디멘드젠/PMAX)용.
//   지출=Ads API, 매출=MP utm_campaign 귀속, country=캠페인명 TW 태그, owner=[Tight]/그 외.
let GCAMP=[];
let ALIMTALK=[];  // CRM(알림톡) 채널 — alimtalk_daily_campaign (일자×캠페인, rev=귀속매출·cost=발송비용)
// 구글 디멘드젠 증감액 테두리용 — budget_apply_log(region='gd') = '⚡ 구글에 예산 적용'으로 실제 반영된 기록.
//   메타 추이차트는 ad_performance_daily.budget 의 전일 대비 변화로 테두리를 그리는데,
//   google_demandgen_campaign_daily 엔 budget 컬럼이 없어 같은 방식이 불가 → 실제 적용 로그를 쓴다.
let GGDG_CHG=[],_GGDG_CHG_LOADED=false;
let KR_HL={},GL_HL={},CR_HL={},VN_HL={};
// 하이라이트 출처: 'ai'(오늘의퍼포먼스봇 조언 자동표기)면 네모에 테두리(.hl-ai), 'user'(사람 클릭)면 테두리 없음
let KR_SRC={},GL_SRC={},CR_SRC={},VN_SRC={};
// 추이차트 전용 메모 (하이라이트 테이블 memo 컬럼 ↔ 하이라이트와 함께 전체삭제)
let KR_HM={},GL_HM={},CR_HM={},VN_HM={};
let AD=[],DATES=[],DAILY={},PRODS=[],HIGHLIGHTS={},HL_MEMO={},HL_SRC={};
// 날짜탭↔추이차트 메모 durable 저장소(daily_memos) 맵: key=region|date|entity_id → memo
let DMEMO={};
const _dmKey=(region,date,id)=>region+'|'+date+'|'+id;
// 추이차트 하이라이트·메모는 '오늘' 마킹만 유효 — 0시 지나면 자동 삭제(날짜탭 perfTbl 기록은 보존)
function _hlDayStr(){const d=new Date();return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0')}
function _hlMidnightISO(){const d=new Date();d.setHours(0,0,0,0);return d.toISOString()}
function _hlIsToday(iso){return !!iso&&new Date(iso).getTime()>=new Date(new Date().setHours(0,0,0,0)).getTime()}
let HL_DAY=_hlDayStr();

// ===== HELPERS =====
// 실패를 조용히 삼키지 않는다 — 에러는 throw, 네트워크 오류·5xx·429 는 짧은 백오프로 2회 재시도.
//   (구버전은 실패해도 에러 JSON '객체'를 그대로 반환 → 호출부가 빈 데이터로 오인해 지출/매출이
//    조용히 0으로 그려졌고, sbAll 은 length 가 undefined 라 루프 종료 판정도 못 했다. 2026-08-11)
const _sbWait=n=>new Promise(res=>setTimeout(res,400*(n+1)));
async function sbQ(t,q='',_try=0){
  let r;
  try{ r=await fetch(SB_URL+'/rest/v1/'+t+'?'+q,{headers:SBH}); }
  catch(e){ if(_try<2){await _sbWait(_try);return sbQ(t,q,_try+1)} throw new Error(t+' 네트워크 실패: '+(e&&e.message||e)) }
  if(!r.ok){
    if((r.status>=500||r.status===429)&&_try<2){await _sbWait(_try);return sbQ(t,q,_try+1)}
    // 401 = 토큰 만료(JWT expired) 또는 세션 폴백(permission denied). 부팅 직후엔 supabase-js 의
    //   백그라운드 갱신과 첫 fetch 가 경합해 '만료 토큰으로 전 테이블 401' 이 되기 쉽다.
    //   → 갱신 1회 후 재시도, 그래도 안 되면 로그인 화면으로. (조용히 0 으로 그리지 않는다)
    if(r.status===401&&_try<2){
      if(await authRefresh())return sbQ(t,q,_try+1);
      authLost('세션이 만료되었습니다. 비밀번호를 다시 입력해 주세요.');
    }
    let m='';try{m=(await r.json()).message||''}catch(e){}
    throw new Error(t+' HTTP '+r.status+(m?' — '+m:''));
  }
  const d=await r.json();
  if(!Array.isArray(d))throw new Error(t+' 응답이 배열이 아님');
  return d;
}
async function sbAll(t,orderCol,extra){let p=0,a=[];const ex=extra||'';while(true){const r=await sbQ(t,'select=*&order=date.desc,'+orderCol+'.desc&limit=1000&offset='+p*1000+ex);a=a.concat(r);if(r.length<1000)break;p++}return a}
// 날짜 컷오프: 모든 추이/주간 탭 max 범위(210일=30주)를 커버하면서 불필요한 과거 데이터는 제외 (5일 버퍼)
function _dateCutoff(days){const d=new Date();d.setDate(d.getDate()-days);return d.toISOString().slice(0,10)}

// ===== IndexedDB 캐시 (stale-while-revalidate) =====
const _IDB_NAME='tightauto_dash_cache_v1', _IDB_STORE='tables';
let _idbP;
function _idb(){
  if(!_idbP) _idbP = new Promise(res=>{
    try{
      const r=indexedDB.open(_IDB_NAME,1);
      r.onupgradeneeded=e=>{e.target.result.createObjectStore(_IDB_STORE)};
      r.onsuccess=e=>res(e.target.result);
      r.onerror=()=>res(null);
    }catch(e){res(null)}
  });
  return _idbP;
}
async function cacheGet(k){
  const db=await _idb(); if(!db) return null;
  return new Promise(res=>{
    try{
      const tx=db.transaction(_IDB_STORE,'readonly');
      const r=tx.objectStore(_IDB_STORE).get(k);
      r.onsuccess=()=>res(r.result||null);
      r.onerror=()=>res(null);
    }catch(e){res(null)}
  });
}
async function cacheSet(k,v){
  const db=await _idb(); if(!db) return;
  return new Promise(res=>{
    try{
      const tx=db.transaction(_IDB_STORE,'readwrite');
      tx.objectStore(_IDB_STORE).put(v,k);
      tx.oncomplete=res; tx.onerror=res;
    }catch(e){res()}
  });
}
// 캐시 전체 삭제 후 새로고침 — 경고 배너의 '🧹 캐시 비우고 다시 불러오기' 버튼용.
//   캐시가 있으면 그걸로 먼저 그리고 fresh fetch 는 백그라운드라, fetch 가 계속 실패/지연되면
//   옛 데이터가 무기한 남는다. 캐시를 지우면 fresh fetch 를 await 하는 경로로 들어가 원인이 바로 드러난다.
function purgeCacheAndReload(){
  try{ indexedDB.deleteDatabase(_IDB_NAME) }catch(e){}
  setTimeout(()=>location.reload(),300);
}

function money(n){
  if(n==null||n===0)return'';
  return MODE==='gl'?'$'+Math.round(n).toLocaleString('en-US'):'₩'+Math.round(n).toLocaleString('ko-KR');
}
const F=n=>n==null?'':Math.round(n).toLocaleString('ko-KR');
const P=(n,d)=>n==null?'':n.toFixed(d||1)+'%';
const WD=d=>['일','월','화','수','목','금','토'][new Date(d).getDay()];
const WM=d=>{const x=new Date(d),dd=x.getDay(),df=x.getDate()-dd+(dd===0?-6:1);return new Date(x.setDate(df)).toISOString().split('T')[0]};
const DK=d=>{const p=d.split('-');return p[0].slice(2)+'/'+p[1]+'/'+p[2]};
function RC(r){if(r>=300)return'bg-r300';if(r>=200)return'bg-r200';if(r>=100)return'bg-r100';if(r>0)return'bg-r50';return'bg-r0'}
function MC(roas,profit,spend,revenue,cvr,cpm,ctr,cpa){
  if(!spend)return'';const pc=profit>=0?'p':'p neg';
  const cvTxt=P(cvr)+(ctr!=null?'('+P(ctr)+')':'');
  let html='<div class="r">'+roas.toFixed(0)+'</div><div class="'+pc+'">'+money(profit)+'</div><div class="s">-'+money(spend)+'</div><div class="rv">'+money(revenue)+'</div><div class="cv">'+cvTxt+'</div>';
  if(cpm)html+='<div class="cm">'+money(cpm)+'</div>';
  if(cpa)html+='<div class="cpa">'+money(cpa)+'</div>';  // 구매당비용(=지출/MP구매수), CPM 아래
  return html;
}

// ===== 🧪 실험탭 — 세트 A/B 비교 (각자 세트ID·기간 지정) =====
// 셀 순서=추이차트 MC: ROAS / 순이익 / 지출 / 매출 / 전환율(클릭률) / CPM. 통화는 세트 소속(국내·밴스드=₩, 글로벌=$).
let expChart=null;
// 실험탭 하단 추이 선그래프 — 위 A/B 추이표(range={date:{s,r,uc,mp,imp}})를 날짜축 선그래프로.
//   A=파랑·B=빨강, 지표 선택(#expChartMetric). 금액계열은 통화 혼용 주의(ROAS·CVR은 %).
function _expRenderChart(A,B){
  const cv=document.getElementById('expChart');if(!cv)return;
  try{if(expChart)expChart.destroy()}catch(e){}
  expChart=null;
  if(typeof Chart==='undefined')return;
  const metric=(document.getElementById('expChartMetric')||{}).value||'roas';
  // 지정 기간 평균 표와 동일 항목. kind: pct(%)/ratio(빈도)/money(그 외, 통화표기)
  const kind=(metric==='roas'||metric==='cvr'||metric==='ctr')?'pct':(metric==='freq'?'ratio':'money');
  const mval=o=>{if(!o)return null;const s=o.s||0,r=o.r||0,uc=o.uc||0,mp=o.mp||0,imp=o.imp||0,rch=o.rch||0;
    switch(metric){
      case 'roas':return s>0?+(r/s*100).toFixed(1):null;
      case 'profit':return Math.round(r-s);
      case 'spend':return Math.round(s);
      case 'revenue':return Math.round(r);
      case 'cvr':return (uc>0&&mp>0)?+(mp/uc*100).toFixed(2):null;
      case 'ctr':return imp>0?+(uc/imp*100).toFixed(2):null;
      case 'freq':return rch>0?+(imp/rch).toFixed(2):null;
      case 'cpa':return mp>0?Math.round(s/mp):null;
      case 'cpm':return imp>0?Math.round(s/imp*1000):null;
    }return null};
  // 왼쪽=최신: 날짜 내림차순(표의 '◀최신'과 동일 방향)
  const dates=[...new Set([...Object.keys(A.range||{}),...Object.keys(B.range||{})])].sort().reverse();
  if(!dates.length)return;
  const labels=dates.map(d=>DK(d).slice(3)+'('+WD(d)+')');
  const dataA=dates.map(d=>mval((A.range||{})[d]));
  const dataB=dates.map(d=>mval((B.range||{})[d]));
  const nmA='A'+(A.name?(' · '+A.name.slice(0,18)):'');
  const nmB='B'+(B.name?(' · '+B.name.slice(0,18)):'');
  const cur=A.cur||B.cur||'₩';
  const moneyAxis=v=>{const a=Math.abs(v);if(cur==='$')return '$'+(a>=1000?(v/1000).toFixed(1)+'k':Math.round(v));return '₩'+(a>=10000?Math.round(v/10000)+'만':Math.round(v).toLocaleString('ko-KR'))};
  const fmt=v=>{if(v==null)return'';if(kind==='pct')return v+'%';if(kind==='ratio')return v.toFixed(2);return cur+Math.round(v).toLocaleString(cur==='$'?'en-US':'ko-KR')};
  const axisCb=v=>kind==='pct'?v+'%':(kind==='ratio'?v:moneyAxis(v));
  expChart=new Chart(cv,{type:'line',data:{labels,datasets:[
    {label:nmA,data:dataA,borderColor:'#1a73e8',backgroundColor:'transparent',tension:0.3,borderWidth:2,pointRadius:2,spanGaps:true},
    {label:nmB,data:dataB,borderColor:'#d81b60',backgroundColor:'transparent',tension:0.3,borderWidth:2,pointRadius:2,spanGaps:true}
  ]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{font:{size:11}}},tooltip:{callbacks:{label:c=>c.dataset.label+': '+fmt(c.parsed.y)}}},scales:{x:{ticks:{font:{size:9}}},y:{beginAtZero:kind!=='pct',ticks:{font:{size:9},callback:axisCb}}}}});
}
function _expMoney(n,cur){if(n==null||n===0)return'';return cur+Math.round(n).toLocaleString(cur==='$'?'en-US':'ko-KR')}
function _expCell(o,cur){        // o={s,r,uc,mp,imp}
  if(!o||!o.s)return'';
  const roas=o.s>0?o.r/o.s*100:0,profit=o.r-o.s;
  const cvr=o.uc>0&&o.mp>0?o.mp/o.uc*100:0,ctr=o.imp>0?o.uc/o.imp*100:0,cpm=o.imp>0?o.s/o.imp*1000:0;
  const pc=profit>=0?'p':'p neg';
  let h='<div class="r">'+roas.toFixed(0)+'</div><div class="'+pc+'">'+_expMoney(profit,cur)+'</div><div class="s">-'+_expMoney(o.s,cur)+'</div><div class="rv">'+_expMoney(o.r,cur)+'</div><div class="cv">'+P(cvr)+'('+P(ctr)+')</div>';
  if(cpm)h+='<div class="cm">'+_expMoney(cpm,cur)+'</div>';
  return h;
}
// ===== 📋 실험 현황 — 원본 vs 파생(복제·tROAS 등) 자동 나열 =====
//   실험탭(A/B)에 '원본 + 그 변형들'을 직접 넣었을 때 나오는 화면(추이표·평균표·퍼널·그래프)을
//   가족마다 자동으로 만들어 세로로 쭉 보여준다. 손으로 ID 를 넣지 않아도 되는 버전.
//   가족 판정 = dvClassify(추이차트·🧬복제·변형 탭과 같은 계보키) → 파생이 하나라도 있으면 '실험'.
const ES_COLORS=['#1a73e8','#d81b60','#00897b','#f57c00','#6d4c41','#7b1fa2','#455a64','#ad1457'];
let ES_CHARTS=[];            // 카드별 Chart 인스턴스(재렌더 시 파기)
let ES_OBS=null;             // 화면에 들어온 카드만 그래프를 그리는 관찰자
const ES_DATA={};            // canvas id → sides (지연 그래프용)

function _esVal(id,dflt){const e=document.getElementById(id);return e?e.value:dflt}

// 소스 행 → 세트 단위 집계(기간 컷 적용). 통화·필드 규칙은 실험탭(_expFindSet)과 동일.
function _esCollect(src,days){
  const cut=DATES.length?DATES[Math.min(days,DATES.length)-1]:'';
  const srcs=[];
  if(src==='kr'||src==='all')srcs.push({rows:KR_AD,cur:'₩',usd:false,tag:'국내'});
  if(src==='gl'||src==='all')srcs.push({rows:GL_AD,cur:'$',usd:true,tag:'글로벌'});
  if(src==='vn'||src==='all')srcs.push({rows:VN_AD,cur:'₩',usd:false,tag:'밴스드'});
  const sets={};
  srcs.forEach(S=>{
    (S.rows||[]).forEach(r=>{
      if(cut&&r.date<cut)return;
      const id=String(r.adset_id||'');if(!id)return;
      const k=S.tag+'|'+id;
      let o=sets[k];
      if(!o)o=sets[k]={id:id,name:r.adset_name||'',product:r.product||'기타',cur:S.cur,tag:S.tag,byDate:{},s:0,r:0,first:r.date};
      if(r.adset_name)o.name=r.adset_name;
      if(r.date<o.first)o.first=r.date;
      const sp=S.usd?(+r.spend_usd||+r.spend||0):(+r.spend||0);
      const rv=S.usd?(+r.revenue_usd||+r.revenue||0):(+r.revenue||0);
      let b=o.byDate[r.date];if(!b)b=o.byDate[r.date]={s:0,r:0,uc:0,mp:0,imp:0,rch:0};
      b.s+=sp;b.r+=rv;b.uc+=(+r.unique_clicks||0);b.mp+=(+r.results_mp||0);b.imp+=(+r.impressions||0);b.rch+=(+r.reach||0);
      o.s+=sp;o.r+=rv;
    });
  });
  return Object.keys(sets).map(k=>sets[k]);
}
// 계보로 묶어 '파생이 있는' 가족만 남긴다(=실험이 걸린 원본).
function _esFamilies(sets){
  const fam={};
  sets.forEach(o=>{
    const c=dvClassify(o.name||'');o.kind=c.kind;o.tags=c.tags;
    const key=o.tag+'::'+(c.key||o.id);
    let f=fam[key];
    if(!f)f=fam[key]={key:key,tag:o.tag,product:o.product,mem:[],s:0,r:0,varFirst:'9999-99-99'};
    f.mem.push(o);f.s+=o.s;f.r+=o.r;
    if(o.kind==='orig'&&o.product)f.product=o.product;
    if(o.kind!=='orig'&&o.first<f.varFirst)f.varFirst=o.first;
  });
  return Object.keys(fam).map(k=>fam[k]).filter(f=>f.mem.length>1&&f.mem.some(m=>m.kind!=='orig'));
}
// 세트 하나 → 실험탭 사이드와 같은 모양 {name,id,cur,range,tot,calDays,lab,role,col}
function _esSide(m,i){
  const range=m.byDate,tot={s:0,r:0,uc:0,mp:0,imp:0,rch:0};
  const ks=Object.keys(range).sort();
  ks.forEach(d=>{const o=range[d];tot.s+=o.s;tot.r+=o.r;tot.uc+=o.uc;tot.mp+=o.mp;tot.imp+=o.imp;tot.rch+=o.rch});
  let calDays=ks.length?Math.round((new Date(ks[ks.length-1])-new Date(ks[0]))/864e5)+1:0;
  if(calDays<1)calDays=ks.length;
  const role=m.kind==='orig'?'원본':((m.tags&&m.tags.length)?m.tags.join('/'):'변형');
  return {id:m.id,name:m.name,cur:m.cur,range:range,tot:tot,calDays:calDays,kind:m.kind,
          lab:String.fromCharCode(65+i),role:role,col:ES_COLORS[i%ES_COLORS.length]};
}
// 추이표 — 실험탭 expTbl 과 같은 포맷(사이드마다 '날짜 row' + '데이터 row', 왼쪽=각자 최신일)
function _esTrendTable(sides){
  const per=sides.map(S=>Object.keys(S.range).sort().reverse());
  let maxLen=0;per.forEach(a=>{if(a.length>maxLen)maxLen=a.length});
  if(!maxLen)return '';
  let ths='';
  for(let i=0;i<maxLen;i++)ths+='<th style="min-width:var(--cw);font-size:9px;color:#aaa;font-weight:400">'+(i===0?'◀최신':(i+1))+'</th>';
  let h='<table><thead><tr><th style="min-width:230px;max-width:230px;text-align:left">세트'
    +'<div style="font-size:8px;font-weight:400;color:#999;line-height:1.3">셀: ROAS / 순이익 / 지출 / 매출 / 전환율(클릭률) / CPM</div></th>'
    +'<th style="min-width:112px;background:#eef3f9">전체</th>'+ths+'</tr></thead><tbody>';
  sides.forEach((S,si)=>{
    const dts=per[si];
    h+='<tr><td style="background:#eef1f6;font-size:9px;font-weight:700;color:'+S.col+'">'+S.lab+' 날짜</td><td style="background:#eef1f6"></td>';
    for(let i=0;i<maxLen;i++){const d=dts[i];h+='<td style="background:#f4f6fa;font-size:9px;color:#555;font-weight:600">'+(d?DK(d).slice(3)+'('+WD(d)+')':'')+'</td>'}
    h+='</tr>';
    const tot=S.tot,roasT=tot.s>0?tot.r/tot.s*100:0;
    h+='<tr><td style="text-align:left;font-size:10px;background:#fff;min-width:230px;max-width:230px;white-space:normal;word-break:break-all;line-height:1.35">'
      +'<b style="color:'+S.col+';font-size:12px">'+S.lab+'</b> <span style="font-size:9px;color:#fff;background:'+S.col+';border-radius:7px;padding:0 5px">'+abEsc(S.role)+'</span> <b>'+abEsc(S.name||'-')+'</b>'
      +'<br><span style="color:#999;font-size:9px">'+abEsc(S.id)+' · '+S.cur+'</span></td>'
      +'<td class="mc '+(tot.s?RC(roasT):'')+'" style="background:#eef3f9">'+_expCell(tot,S.cur)+'</td>';
    for(let i=0;i<maxLen;i++){
      const d=dts[i];
      if(!d){h+='<td></td>';continue}
      const o=S.range[d],roas=o.s>0?o.r/o.s*100:0;
      h+='<td class="mc '+RC(roas)+'">'+_expCell(o,S.cur)+'</td>';
    }
    h+='</tr>';
  });
  return h+'</tbody></table>';
}
// 기간 평균표 — 실험탭 expAvgTbl 과 같은 항목(지출·매출·순이익=하루 평균, 나머지=기간 비율)
//   ROAS 칸 아래에 '원본 대비 몇 %p' 를 덧붙여 실험 결과를 바로 읽게 한다.
function _esAvgTable(sides){
  let h='<table><thead><tr><th style="min-width:230px;max-width:230px;text-align:left">세트</th><th style="min-width:52px">일수</th>'
    +'<th>ROAS</th><th title="일별 ROAS 표준편차 — 변동성">편차</th><th>순이익/일</th><th>지출/일</th><th>매출/일</th>'
    +'<th>CVR</th><th>CTR</th><th>빈도</th><th>구매당비용</th><th>CPM</th></tr></thead><tbody>';
  const base=sides[0];
  const bR=(base&&base.tot&&base.tot.s>0)?base.tot.r/base.tot.s*100:0;
  sides.forEach(S=>{
    const days=S.calDays||0,t=S.tot||{s:0};
    const nm='<td style="text-align:left;font-size:11px;min-width:230px;max-width:230px;white-space:normal;word-break:break-all;line-height:1.35">'
      +'<b style="color:'+S.col+'">'+S.lab+'</b> <span style="font-size:9px;color:'+S.col+'">['+abEsc(S.role)+']</span> '+abEsc(S.name||'-')+'</td>';
    if(!days||!t.s){h+='<tr>'+nm+'<td style="text-align:center;color:#999">0</td><td colspan="10" style="color:#999">데이터 없음</td></tr>';return}
    const roas=t.s>0?t.r/t.s*100:0,pf=(t.r-t.s)/days,cvr=t.uc>0&&t.mp>0?t.mp/t.uc*100:0,ctr=t.imp>0?t.uc/t.imp*100:0,
      cpm=t.imp>0?t.s/t.imp*1000:0,freq=t.rch>0?t.imp/t.rch:0,cpa=t.mp>0?t.s/t.mp:0,cur=S.cur;
    const rd=Object.keys(S.range).map(d=>S.range[d]).filter(o=>o.s>0).map(o=>o.r/o.s*100);
    let sd=null;
    if(rd.length>=2){const mn=rd.reduce((a,x)=>a+x,0)/rd.length;sd=Math.sqrt(rd.reduce((a,x)=>a+(x-mn)*(x-mn),0)/rd.length)}
    else if(rd.length===1)sd=0;
    const dlt=(S!==base&&bR>0)?(roas-bR):null;
    h+='<tr>'+nm
      +'<td style="text-align:center">'+days+'일</td>'
      +'<td class="'+RC(roas)+'" style="text-align:right;font-weight:700">'+roas.toFixed(0)+'%'
      +(dlt==null?'':'<div style="font-size:9px;font-weight:600;color:'+(dlt>=0?'#1a6b1a':'#d00')+'">'+(dlt>=0?'+':'')+dlt.toFixed(0)+'p</div>')+'</td>'
      +'<td style="text-align:right;color:#666" title="지출 있는 날의 일별 ROAS 표준편차('+rd.length+'일)">'+(sd==null?'':'±'+sd.toFixed(0))+'</td>'
      +'<td style="text-align:right;color:'+(pf>=0?'#1a6b1a':'#d00')+'">'+_expMoney(pf,cur)+'</td>'
      +'<td style="text-align:right;color:#d00">'+_expMoney(t.s/days,cur)+'</td>'
      +'<td style="text-align:right;color:#0000dd">'+_expMoney(t.r/days,cur)+'</td>'
      +'<td style="text-align:right">'+P(cvr)+'</td><td style="text-align:right">'+P(ctr)+'</td>'
      +'<td style="text-align:right">'+(freq?freq.toFixed(2):'')+'</td>'
      +'<td style="text-align:right">'+(cpa?_expMoney(cpa,cur):'')+'</td>'
      +'<td style="text-align:right">'+_expMoney(cpm,cur)+'</td></tr>';
  });
  return h+'</tbody></table>';
}
// 퍼널 — 실험탭과 동일(노출→도달→클릭→구매, 전환=바로 윗단계 대비). 사이드마다 값·전환 2열.
function _esFunnelTable(sides){
  const stages=S=>{
    const t=S.tot||{imp:0,rch:0,uc:0,mp:0};
    const imp=t.imp||0,rch=t.rch||0,clk=t.uc||0,buy=t.mp||0,base=rch>0?rch:imp;
    return [{n:'노출',v:imp,rate:null},{n:'도달',v:rch>0?rch:null,rate:(rch>0&&imp>0)?rch/imp*100:null},
            {n:'클릭',v:clk,rate:base>0?clk/base*100:null},{n:'구매',v:buy,rate:clk>0?buy/clk*100:null}];
  };
  const st=sides.map(stages),ok=sides.map(S=>!!(S.tot&&S.tot.imp));
  let h='<table><thead><tr><th style="min-width:70px;text-align:left">단계</th>';
  sides.forEach(S=>{h+='<th style="color:'+S.col+'">'+S.lab+' 값</th><th style="color:'+S.col+'">'+S.lab+' 전환</th>'});
  h+='</tr></thead><tbody>';
  for(let i=0;i<4;i++){
    h+='<tr><td style="text-align:left;font-weight:600">'+st[0][i].n+'</td>';
    sides.forEach((S,si)=>{
      const x=st[si][i];
      h+='<td style="text-align:right">'+(ok[si]&&x.v!=null?F(x.v):'')+'</td>'
       +'<td style="text-align:right;color:#888">'+(ok[si]&&x.rate!=null?x.rate.toFixed(1)+'%':(i===0&&ok[si]?'-':''))+'</td>';
    });
    h+='</tr>';
  }
  return h+'</tbody></table>';
}
// 그래프 — 실험탭과 같은 지표 선택, 사이드마다 선 1개.
//   카드가 많아 한 번에 다 그리면 무거우므로 화면에 들어올 때 그린다(IntersectionObserver).
function _esDrawChart(cv){
  const d=ES_DATA[cv.id];
  if(!d||typeof Chart==='undefined')return;
  const sides=d.sides,metric=d.metric;
  const kind=(metric==='roas'||metric==='cvr'||metric==='ctr')?'pct':(metric==='freq'?'ratio':'money');
  const mval=o=>{
    if(!o)return null;
    const s=o.s||0,r=o.r||0,uc=o.uc||0,mp=o.mp||0,imp=o.imp||0,rch=o.rch||0;
    switch(metric){
      case 'roas':return s>0?+(r/s*100).toFixed(1):null;
      case 'profit':return Math.round(r-s);
      case 'spend':return Math.round(s);
      case 'revenue':return Math.round(r);
      case 'cvr':return (uc>0&&mp>0)?+(mp/uc*100).toFixed(2):null;
      case 'ctr':return imp>0?+(uc/imp*100).toFixed(2):null;
      case 'freq':return rch>0?+(imp/rch).toFixed(2):null;
      case 'cpa':return mp>0?Math.round(s/mp):null;
      case 'cpm':return imp>0?Math.round(s/imp*1000):null;
    }
    return null;
  };
  const all={};sides.forEach(S=>Object.keys(S.range).forEach(k=>{all[k]=1}));
  const dates=Object.keys(all).sort().reverse();     // 왼쪽=최신(표와 같은 방향)
  if(!dates.length)return;
  const labels=dates.map(k=>DK(k).slice(3)+'('+WD(k)+')');
  const cur=sides[0]?sides[0].cur:'₩';
  const moneyAxis=v=>{const a=Math.abs(v);if(cur==='$')return '$'+(a>=1000?(v/1000).toFixed(1)+'k':Math.round(v));return '₩'+(a>=10000?Math.round(v/10000)+'만':Math.round(v).toLocaleString('ko-KR'))};
  const fmt=v=>{if(v==null)return'';if(kind==='pct')return v+'%';if(kind==='ratio')return v.toFixed(2);return cur+Math.round(v).toLocaleString(cur==='$'?'en-US':'ko-KR')};
  const ds=sides.map(S=>({label:S.lab+' '+S.role,data:dates.map(k=>mval(S.range[k])),borderColor:S.col,
    backgroundColor:'transparent',tension:0.3,borderWidth:2,pointRadius:2,spanGaps:true}));
  const ch=new Chart(cv,{type:'line',data:{labels:labels,datasets:ds},options:{responsive:true,maintainAspectRatio:false,
    interaction:{mode:'index',intersect:false},
    plugins:{legend:{labels:{font:{size:10},boxWidth:12}},tooltip:{callbacks:{label:c=>c.dataset.label+': '+fmt(c.parsed.y)}}},
    scales:{x:{ticks:{font:{size:9}}},y:{beginAtZero:kind!=='pct',ticks:{font:{size:9},callback:v=>kind==='pct'?v+'%':(kind==='ratio'?v:moneyAxis(v))}}}}});
  ES_CHARTS.push(ch);
}
// 가족 카드 1개 — 헤더 + 평균표 + 추이표 + 퍼널 + 그래프 (실험탭 화면 그대로)
function _esCard(f,idx,metric){
  const mem=f.mem.slice().sort((x,y)=>{
    const ox=x.kind==='orig'?0:1,oy=y.kind==='orig'?0:1;
    if(ox!==oy)return ox-oy;                 // 원본이 항상 A
    return (y.r-x.r)||(y.s-x.s);             // 그다음 매출 큰 순
  });
  const sides=mem.map((m,i)=>_esSide(m,i));
  const cid='esCh'+idx;
  ES_DATA[cid]={sides:sides,metric:metric};
  const orig=sides[0],cur=orig?orig.cur:'₩',roas=f.s>0?f.r/f.s*100:0;
  const varN=mem.filter(m=>m.kind!=='orig').length;
  const head='<div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;margin-bottom:6px">'
    +'<span style="background:#1a2744;color:#fff;font-size:9.5px;font-weight:700;padding:1px 7px;border-radius:9px">'+abEsc(f.product||'-')+'</span>'
    +'<span style="background:#eef2ff;color:#3730a3;font-size:9px;font-weight:700;padding:1px 6px;border-radius:9px">'+abEsc(f.tag)+'</span>'
    +'<span style="font-size:12.5px;font-weight:700;color:#111">'+abEsc(orig?orig.name:f.key)+'</span>'
    +'<span style="font-size:10px;color:#6b7280">실험 '+varN+'개 · 가족 지출 '+_expMoney(f.s,cur)+' · 매출 '+_expMoney(f.r,cur)+'</span>'
    +'<span class="'+RC(roas)+'" style="font-size:10px;padding:0 5px;border-radius:3px;font-weight:700">ROAS '+roas.toFixed(0)+'%</span></div>';
  return '<div class="es-card">'+head
    +'<div class="es-sub">📊 기간 평균 <span>· 지출·매출·순이익=하루 평균 / ROAS 아래 값=원본(A) 대비 차이(%p)</span></div>'
    +'<div class="es-wrap">'+_esAvgTable(sides)+'</div>'
    +'<div class="es-sub">📅 추이 <span>· 세트마다 자기 날짜 기준(왼쪽=최신) · 셀=ROAS/순이익/지출/매출/전환율(클릭률)/CPM</span></div>'
    +'<div class="es-wrap">'+_esTrendTable(sides)+'</div>'
    +'<div class="es-sub">🔻 퍼널 <span>· 전환=바로 윗단계 대비 · 구매=Mixpanel 귀속</span></div>'
    +'<div class="es-wrap">'+_esFunnelTable(sides)+'</div>'
    +'<div class="es-sub">📈 추이 그래프</div>'
    +'<div style="height:230px;padding:0 4px 6px"><canvas class="es-chart" id="'+cid+'"></canvas></div>'
    +'</div>';
}
function renderExpStatus(){
  const box=document.getElementById('esBox');
  if(!box)return;
  ES_CHARTS.forEach(c=>{try{c.destroy()}catch(e){}});ES_CHARTS=[];
  if(ES_OBS){try{ES_OBS.disconnect()}catch(e){}ES_OBS=null}
  Object.keys(ES_DATA).forEach(k=>{delete ES_DATA[k]});
  const src=_esVal('esSrc','kr'),days=parseInt(_esVal('esDays','14'))||14;
  const minSp=parseFloat(_esVal('esMin','0'))||0,sort=_esVal('esSort','spend');
  const limit=parseInt(_esVal('esLimit','20'))||0,metric=_esVal('esMetric','roas');
  const kw=(_esVal('esFilter','')||'').trim().toLowerCase();
  let fams=_esFamilies(_esCollect(src,days));
  const total=fams.length;
  if(kw)fams=fams.filter(f=>f.mem.some(m=>((m.name||'')+' '+m.id).toLowerCase().indexOf(kw)>=0));
  if(minSp>0)fams=fams.filter(f=>f.s>=minSp);
  fams.sort((a,b)=>{
    if(sort==='rev')return b.r-a.r;
    if(sort==='new')return a.varFirst<b.varFirst?1:(a.varFirst>b.varFirst?-1:0);
    return b.s-a.s;
  });
  const shown=limit>0?fams.slice(0,limit):fams;
  const info=document.getElementById('esInfo');
  if(info)info.textContent='실험 '+total+'건 중 조건 통과 '+fams.length+'건 · 표시 '+shown.length+'건'
    +(fams.length>shown.length?(' (나머지 '+(fams.length-shown.length)+'건은 표시 개수 제한으로 생략)'):'');
  if(!shown.length){box.innerHTML='<div style="padding:20px;color:#888;font-size:12px">조건에 맞는 실험(원본+파생)이 없습니다. 기간을 늘리거나 소스를 바꿔보세요.</div>';return}
  box.innerHTML=shown.map((f,i)=>_esCard(f,i,metric)).join('');
  if(typeof IntersectionObserver!=='undefined'&&typeof Chart!=='undefined'){
    ES_OBS=new IntersectionObserver(function(ents){
      ents.forEach(function(e){if(!e.isIntersecting)return;ES_OBS.unobserve(e.target);_esDrawChart(e.target)});
    },{rootMargin:'250px'});
    box.querySelectorAll('canvas.es-chart').forEach(c=>ES_OBS.observe(c));
  }else{
    box.querySelectorAll('canvas.es-chart').forEach(c=>_esDrawChart(c));
  }
}
// 입력 ID 를 KR/밴스드/글로벌(세트) → 구글 디멘드젠(광고그룹) → 소재(ad) 순으로 검색해 정규화.
//   반환 {name,cur,byDate:{date:{s,r,uc,mp,imp,rch}}}
function _expFindSet(id){
  id=String(id||'').trim();if(!id)return null;
  const scan=(rows,idf,namef,cur,usd)=>{
    if(!rows||!rows.length)return null;
    const rr=rows.filter(r=>String(r[idf]||'')===id);if(!rr.length)return null;
    const byDate={};let name='';
    rr.forEach(r=>{const s=usd?(+r.spend_usd||0):(+r.spend||0),rv=usd?(+r.revenue_usd||0):(+r.revenue||0);
      if(!byDate[r.date])byDate[r.date]={s:0,r:0,uc:0,mp:0,imp:0,rch:0};const o=byDate[r.date];
      o.s+=s;o.r+=rv;o.uc+=(+r.unique_clicks||0);o.mp+=(+r.results_mp||0);o.imp+=(+r.impressions||0);o.rch+=(+r.reach||0);name=r[namef]||name});
    return {name,cur,byDate};
  };
  return scan(KR_AD,'adset_id','adset_name','₩',false)||scan(VN_AD,'adset_id','adset_name','₩',false)
       ||scan(GL_AD,'adset_id','adset_name','$',true)||_expFindGgdg(id)||scan(CR_AD,'ad_id','ad_name','₩',false);
}
// 구글 디멘드젠 [Tight] 세트(=광고그룹 ad_group_id) — google_demandgen_campaign_daily(GGDG_TIGHT).
//   필드명이 메타와 다름: spend/revenue/clicks/purchase_count/impressions. reach 컬럼 없음 → 도달·빈도는 공란.
//   ⚠ 클릭은 구글 총클릭(메타는 unique_clicks) → 소스 간 CVR·CTR 직접 비교는 주의. 통화=₩.
function _expFindGgdg(id){
  if(!GGDG_TIGHT||!GGDG_TIGHT.length)return null;
  const rr=GGDG_TIGHT.filter(r=>String(r.ad_group_id||'')===id);if(!rr.length)return null;
  const byDate={};let name='';
  rr.forEach(r=>{if(!byDate[r.date])byDate[r.date]={s:0,r:0,uc:0,mp:0,imp:0,rch:0};const o=byDate[r.date];
    o.s+=(+r.spend||0);o.r+=(+r.revenue||0);o.uc+=(+r.clicks||0);o.mp+=(+r.purchase_count||0);o.imp+=(+r.impressions||0);
    name=r.ad_group_name||name});
  return {name:'🟢 '+(name||id),cur:'₩',byDate};
}
// 실험탭이 쓰는 지연로드 테이블(CR_AD=소재, GGDG_TIGHT=구글 디멘드젠 세트) 1회 요청 후 콜백.
//   ensureBigTable 은 실패해도 재시도하지 않으므로 재귀 루프 방지를 위해 one-shot 플래그 사용.
let _expSrcRequested=false;
function _expEnsureSrc(cb){
  if(_expSrcRequested)return false;
  const L=window._BIG_LOADED||{};
  const need=[];
  if(!L.cr&&!CR_AD.length)need.push('cr');
  if(!L.ggdgkr)need.push('ggdgkr');   // IndexedDB 캐시본만 있으면 최신 날짜가 비므로 세션당 1회는 fresh fetch
  if(!need.length)return false;
  _expSrcRequested=true;
  Promise.all(need.map(n=>ensureBigTable(n))).then(()=>{try{cb&&cb()}catch(e){}}).catch(()=>{});
  return true;
}
// 사이드 k 의 모든 ID(여러 개) 를 합산 — 날짜 필터 전. {ids,names,cur,byDate}
function _expAgg(k){
  const ids=[...document.querySelectorAll('#expIds'+k+' input.exp-id')].map(el=>(el.value||'').trim()).filter(Boolean);
  const byDate={},names=[];let cur=null;
  ids.forEach(id=>{const set=_expFindSet(id);if(!set)return;if(cur==null)cur=set.cur;names.push(set.name||id);
    Object.keys(set.byDate).forEach(d=>{const o=set.byDate[d];const b=byDate[d]||(byDate[d]={s:0,r:0,uc:0,mp:0,imp:0,rch:0});
      b.s+=o.s;b.r+=o.r;b.uc+=o.uc;b.mp+=o.mp;b.imp+=o.imp;b.rch+=(o.rch||0)})});
  return {ids,names,cur:cur||'₩',byDate};
}
function _expSide(k){
  const agg=_expAgg(k);
  const s=document.getElementById('expStart'+k)?.value||'',e=document.getElementById('expEnd'+k)?.value||'';
  if(!agg.names.length)return {id:agg.ids.join(','),name:'',cur:'₩',range:{},tot:null,calDays:0,count:0};
  const range={},tot={s:0,r:0,uc:0,mp:0,imp:0,rch:0};
  Object.keys(agg.byDate).forEach(d=>{if((!s||d>=s)&&(!e||d<=e)){const o=agg.byDate[d];range[d]=o;tot.s+=o.s;tot.r+=o.r;tot.uc+=o.uc;tot.mp+=o.mp;tot.imp+=o.imp;tot.rch+=o.rch}});
  // 평균 분모 = 설정한 기간의 달력 일수(start~end 포함). 기간 미지정 시 데이터 범위(min~max).
  let calDays=0;
  if(s&&e)calDays=Math.round((new Date(e)-new Date(s))/864e5)+1;
  else{const ks=Object.keys(range).sort();if(ks.length)calDays=Math.round((new Date(ks[ks.length-1])-new Date(ks[0]))/864e5)+1}
  if(calDays<1)calDays=Object.keys(range).length;   // 폴백
  const name=agg.names.length===1?agg.names[0]:(agg.names.length+'개 합산: '+agg.names.join(' + '));
  return {id:agg.ids.join(', '),name,cur:agg.cur,range,tot,calDays,count:agg.names.length};
}
function expAddId(k){
  const c=document.getElementById('expIds'+k);
  const inp=document.createElement('input');
  inp.className='exp-id';inp.placeholder='세트 ID (메타·구글)';
  inp.title='메타 세트 ID(국내·밴스드·글로벌) / 구글 디멘드젠 세트=광고그룹 ID / 메타 소재 ID';
  inp.style.cssText='width:150px;padding:3px 6px;border:1px solid #ccc;border-radius:3px;font-size:11px';
  inp.addEventListener('change',()=>expLookup(k));
  c.appendChild(inp);inp.focus();
}
function expDelId(k){
  const c=document.getElementById('expIds'+k);
  const inputs=c.querySelectorAll('input.exp-id');
  if(inputs.length>1)c.removeChild(inputs[inputs.length-1]);   // 마지막 입력창 제거
  else inputs[0].value='';                                     // 하나만 남으면 값만 비움
  expLookup(k);
}
function expLookup(k){
  _expEnsureSrc(()=>expLookup(k));   // 소재·구글 디멘드젠 테이블 로드 전에 ID 입력한 경우 로드 후 재조회
  const agg=_expAgg(k);
  const anyInput=[...document.querySelectorAll('#expIds'+k+' input.exp-id')].some(el=>(el.value||'').trim());
  document.getElementById('expName'+k).textContent=agg.names.length?(agg.names.length===1?agg.names[0]:agg.names.length+'개 세트'):(anyInput?'(세트 없음)':'');
  // ID 입력 시 합산 세트의 '데이터 있는 기간'으로 자동 설정 → 항상 데이터가 보이게(이후 좁히기 가능)
  const dts=Object.keys(agg.byDate).sort();
  if(dts.length){document.getElementById('expStart'+k).value=dts[0];document.getElementById('expEnd'+k).value=dts[dts.length-1]}
  renderExperiment();
}
function renderExperiment(){
  const A=_expSide('A'),B=_expSide('B');
  const _nm=(k,S)=>{const any=[...document.querySelectorAll('#expIds'+k+' input.exp-id')].some(el=>(el.value||'').trim());
    document.getElementById('expName'+k).textContent=S.count?(S.count===1?S.name:S.count+'개 세트'):(any?'(세트 없음)':'')};
  _nm('A',A);_nm('B',B);
  // A·B 각자 자기 날짜(최신순, 왼쪽=최신). 세트마다 '날짜 row' + '데이터 row' 를 따로 둠(기간 달라도 각자 표시).
  const aDates=Object.keys(A.range).sort().reverse();
  const bDates=Object.keys(B.range).sort().reverse();
  const maxLen=Math.max(aDates.length,bDates.length);
  let ths='';for(let i=0;i<maxLen;i++)ths+='<th style="min-width:var(--cw);font-size:9px;color:#aaa;font-weight:400">'+(i===0?'◀최신':(i+1))+'</th>';
  let h='<thead><tr><th class="fx fx0" style="min-width:230px;max-width:230px;text-align:left">세트'+
    '<div style="font-size:8px;font-weight:400;color:#999;line-height:1.3">셀: ROAS / 순이익 / 지출 / 매출 / 전환율(클릭률) / CPM</div></th>'+
    '<th style="min-width:112px;background:#eef3f9">전체</th>'+ths+'</tr></thead><tbody>';
  [['A',A,aDates,'#1a73e8'],['B',B,bDates,'#d81b60']].forEach(([lab,S,dts,col])=>{
    // 날짜 row (A/B 각자) — 왼쪽부터 최신일
    h+='<tr><td class="fx fx0" style="background:#eef1f6;font-size:9px;font-weight:700;color:'+col+'">'+lab+' 날짜</td><td style="background:#eef1f6"></td>';
    for(let i=0;i<maxLen;i++){const d=dts[i];h+='<td style="background:#f4f6fa;font-size:9px;color:#555;font-weight:600">'+(d?DK(d).slice(3)+'('+WD(d)+')':'')+'</td>'}
    h+='</tr>';
    // 데이터 row
    const tot=S.tot||{s:0};const roasT=tot.s>0?tot.r/tot.s*100:0;
    h+='<tr><td class="fx fx0" style="text-align:left;font-size:10px;background:#fff;min-width:230px;max-width:230px;white-space:normal;overflow:visible;text-overflow:clip;word-break:break-all;line-height:1.35"><b style="color:'+col+';font-size:12px">'+lab+'</b> <b>'+(S.name||'-')+'</b>'+
       '<br><span style="color:#999;font-size:9px">'+(S.id||'')+(S.cur?' · '+S.cur:'')+'</span></td>'+
       '<td class="mc '+(tot.s?RC(roasT):'')+'" style="background:#eef3f9">'+_expCell(tot,S.cur)+'</td>';
    for(let i=0;i<maxLen;i++){const d=dts[i];if(!d){h+='<td></td>';continue}const o=S.range[d];const roas=o.s>0?o.r/o.s*100:0;h+='<td class="mc '+RC(roas)+'">'+_expCell(o,S.cur)+'</td>'}
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('expTbl').innerHTML=h;
  _expRenderAvg(A,B);
  _expRenderFunnel(A,B);
  _expRenderChart(A,B);
}
// 퍼널: 노출→도달→클릭→구매. 전환=바로 윗단계 대비. A vs B 나란히. 값은 선택 기간 합.
function _expRenderFunnel(A,B){
  // 구글 디멘드젠 세트는 reach 컬럼이 없음(rch=0) → 도달 단계 공란, 클릭 전환은 노출 대비(=CTR)로 계산
  const stages=S=>{const t=S.tot||{imp:0,rch:0,uc:0,mp:0};const imp=t.imp||0,rch=t.rch||0,clk=t.uc||0,buy=t.mp||0;
    const base=rch>0?rch:imp;
    return [{n:'노출',v:imp,rate:null},{n:'도달',v:rch>0?rch:null,rate:(rch>0&&imp>0)?rch/imp*100:null},
            {n:'클릭',v:clk,rate:base>0?clk/base*100:null},{n:'구매',v:buy,rate:clk>0?buy/clk*100:null}];};
  const aOK=A.tot&&A.tot.imp,bOK=B.tot&&B.tot.imp;
  const aS=stages(A),bS=stages(B);
  let h='<thead><tr><th style="min-width:70px;text-align:left">단계</th>'+
    '<th style="color:#1a73e8">A 값</th><th style="color:#1a73e8">A 전환</th>'+
    '<th style="color:#d81b60">B 값</th><th style="color:#d81b60">B 전환</th></tr></thead><tbody>';
  for(let i=0;i<4;i++){
    const a=aS[i],b=bS[i];
    h+='<tr><td style="text-align:left;font-weight:600">'+a.n+'</td>'+
       '<td style="text-align:right">'+(aOK&&a.v!=null?F(a.v):'')+'</td>'+
       '<td style="text-align:right;color:#888">'+(aOK&&a.rate!=null?a.rate.toFixed(1)+'%':(i===0&&aOK?'-':''))+'</td>'+
       '<td style="text-align:right">'+(bOK&&b.v!=null?F(b.v):'')+'</td>'+
       '<td style="text-align:right;color:#888">'+(bOK&&b.rate!=null?b.rate.toFixed(1)+'%':(i===0&&bOK?'-':''))+'</td></tr>';
  }
  h+='</tbody>';document.getElementById('expFunnelTbl').innerHTML=h;
}
// 지정 기간 평균 표 — 지출·매출·순이익=하루 평균(설정 기간 일수), 나머지=기간 비율.
//   CVR=구매/클릭, CTR=클릭/노출, 빈도=노출/도달, 구매당비용=지출/구매(CPA).
function _expRenderAvg(A,B){
  const row=(lab,S,col)=>{
    const days=S.calDays||0,t=S.tot||{s:0,r:0,uc:0,mp:0,imp:0,rch:0};
    const nm='<td class="fx fx0" style="text-align:left;font-size:11px;min-width:230px;max-width:230px;white-space:normal;overflow:visible;text-overflow:clip;word-break:break-all;line-height:1.35"><b style="color:'+col+'">'+lab+'</b> '+(S.name||'-')+(S.cur?' <span style="color:#999">'+S.cur+'</span>':'')+'</td>';
    if(!days||!t.s)return '<tr>'+nm+'<td style="text-align:center;color:#999">0</td><td colspan="10" style="color:#999">데이터 없음</td></tr>';
    const roas=t.s>0?t.r/t.s*100:0,pf=(t.r-t.s)/days,cvr=t.uc>0&&t.mp>0?t.mp/t.uc*100:0,ctr=t.imp>0?t.uc/t.imp*100:0,
      cpm=t.imp>0?t.s/t.imp*1000:0,freq=t.rch>0?t.imp/t.rch:0,cpa=t.mp>0?t.s/t.mp:0,cur=S.cur;
    // ROAS 편차 = 지출 있는 날의 일별 ROAS 표준편차(모집단). 변동성이 클수록 값이 큼.
    const rd=Object.values(S.range||{}).filter(o=>o.s>0).map(o=>o.r/o.s*100);
    let sd=null;if(rd.length>=2){const mn=rd.reduce((a,x)=>a+x,0)/rd.length;sd=Math.sqrt(rd.reduce((a,x)=>a+(x-mn)*(x-mn),0)/rd.length)}else if(rd.length===1)sd=0;
    return '<tr>'+nm
      +'<td style="text-align:center">'+days+'일</td>'
      +'<td class="'+RC(roas)+'" style="text-align:right;font-weight:700">'+roas.toFixed(0)+'%</td>'
      +'<td style="text-align:right;color:#666" title="지출 있는 날의 일별 ROAS 표준편차('+rd.length+'일)">'+(sd==null?'':'±'+sd.toFixed(0))+'</td>'
      +'<td style="text-align:right;color:'+(pf>=0?'#1a6b1a':'#d00')+'">'+_expMoney(pf,cur)+'</td>'
      +'<td style="text-align:right;color:#d00">'+_expMoney(t.s/days,cur)+'</td>'
      +'<td style="text-align:right;color:#0000dd">'+_expMoney(t.r/days,cur)+'</td>'
      +'<td style="text-align:right">'+P(cvr)+'</td>'
      +'<td style="text-align:right">'+P(ctr)+'</td>'
      +'<td style="text-align:right">'+(freq?freq.toFixed(2):'')+'</td>'
      +'<td style="text-align:right">'+(cpa?_expMoney(cpa,cur):'')+'</td>'
      +'<td style="text-align:right">'+_expMoney(cpm,cur)+'</td></tr>';
  };
  const h='<thead><tr><th class="fx fx0" style="min-width:230px;max-width:230px;text-align:left">세트</th><th style="min-width:52px">일수</th>'+
    '<th>ROAS</th><th title="일별 ROAS 표준편차 — 변동성">편차</th><th>순이익/일</th><th>지출/일</th><th>매출/일</th><th>CVR</th><th>CTR</th><th>빈도</th><th>구매당비용</th><th>CPM</th></tr></thead>'+
    '<tbody>'+row('A',A,'#1a73e8')+row('B',B,'#d81b60')+'</tbody>';
  document.getElementById('expAvgTbl').innerHTML=h;
}
// 보조지표 셀 — 추이차트 MC와 동일 레이아웃, 데이터만 다름: CTR / CVR / CPM / 구매당비용
//   ctr=클릭/노출, cvr=구매(mp)/클릭, cpm=지출/노출×1000, 구매당비용=지출/구매(=CPP)
function MCAUX(spend,revenue,uc,mp,imp){
  if(!spend)return'';
  const ctr=imp>0?uc/imp*100:0, cvr=(uc>0&&mp>0)?mp/uc*100:0, cpm=imp>0?spend/imp*1000:0;
  const cpp=mp>0?spend/mp:0;  // 구매당비용
  return '<div class="r">'+P(ctr)+'</div>'
        +'<div class="cv">'+P(cvr)+'</div>'
        +'<div class="cm">'+money(cpm)+'</div>'
        +'<div class="s">'+(cpp?money(cpp):'')+'</div>';
}
// Normalize row: global→common, creative→common (ad_id as key)
const VANCED_PRODUCT_MAP={'MZMUDANG':'Shaman'};
// 밴스드 대만 광고계정 — 추이차트를 별도 탭(🇹🇼 대만 추이차트)으로 분리. 기존 밴스드 추이차트에선 제외.
const VN_TW_ACC='act_1286632473622244';
// 광고 계정 ID → 계정 이름 (Meta Graph API `name` 그대로). 추이차트(국내·글로벌) 좌측 '광고 계정' 컬럼용.
// 새 계정 추가 시 여기에 한 줄만 추가하면 됨. 미등록 ID 는 계정번호(act_ 제거)로 폴백 표시.
const ACC_NAMES={
  'act_1270614404675034':'1분꿀잼썰',
  'act_707835224206178':'2비즈니스계정 일6-7만 hksong',
  'act_1808141386564262':'타이트사주3rd원화새계정',
  'act_1054081590008088':'Sajutight_tw',
  'act_1021437716898605':'글로벌계정',
  'act_1335040608536838':'GlobalSaju',
  'act_2677707262628563':'TTsaju',
  'act_993712016404855':'Saju Taiwan',
  'act_25183853061243175':'타이트사주 (밴스드)',
  'act_1560037899174007':'타이트사주 2 (밴스드)',
  'act_1286632473622244':'타이트사주(밴스드_대만)'};
const accName=id=>{const k=String(id||'');if(!k)return'';return ACC_NAMES[k]||k.replace(/^act_/,'')};
// 글로벌 상품명 통합: 한국어명·영문 변형을 하나의 canonical 로 합쳐서 같은 상품으로 집계.
// (예: 솔로→solo, 무당·mudang→shaman, 무녀→mzpian) 키는 소문자, 조회 시 trim+소문자화하여 대소문자·표기변형 무시.
const GL_PRODUCT_CANON={'솔로':'solo','solo':'solo',
  '무당':'shaman','mudang':'shaman','shaman':'shaman',
  '무녀':'mzpian','mzpian':'mzpian',
  '집착':'possessive','possessive':'possessive',
  '커리어':'job','job':'job'};
const canonGLRows=rows=>(rows||[]).map(r=>{
  if(!r||r.product==null)return r;
  const c=GL_PRODUCT_CANON[String(r.product).trim().toLowerCase()];
  return c?{...r,product:c}:r;
});
// 밴스드 행의 상품명 보정 (테이블 product 는 대부분 'etc' 라 캠페인명에서 복원한다).
//   norm() 의 vn 분기와 주간종합의 '밴스드 포함' 합산이 같은 규칙을 쓰도록 분리해둔다.
function vnProduct(r){
  // 캠페인명에 MZMUDANG 포함 → Shaman (언더바/공백/대소문자·HK/TW 변형 모두 포함)
  if(/mzmudang/i.test(r.campaign_name||''))return 'Shaman';
  if(!r.product||r.product==='etc'){
    // 구분자 언더바·공백 모두 허용 (예: '[Vanced]_Career_' , '[Vanced] Possessive_')
    const m=(r.campaign_name||'').match(/^\[Vanced\][_ ]([^_ ]+)[_ ]/i);
    if(m){const raw=m[1];return VANCED_PRODUCT_MAP[raw.toUpperCase()]||VANCED_PRODUCT_MAP[raw]||raw}
  }
  return r.product;
}
function norm(r){
  if(MODE==='gl')return{...r,spend:r.spend_usd,revenue:r.revenue_usd,profit:r.profit_usd,budget:r.budget_usd};
  if(MODE==='vn'){
    const p=vnProduct(r);
    if(p!==r.product)return{...r,product:p};
  }
  return r;
}
// ===== 글로벌 추이차트: 특정 날짜만 Meta 자체 보고값으로 =====
// Mixpanel 귀속 매출은 UTM 이 붙은 실주문만 세므로 Meta 보고값보다 늘 낮다(최근 실측 58~79%).
// 아래 날짜는 사용자 요청으로 추이차트에서만 Meta 기준으로 본다 — 매출 = 지출 × ROAS(메타),
// 구매수 = results_meta, 순이익·ROAS·CVR 은 그 값으로 재계산된다.
//   · 적용 범위: 글로벌(gl) 모드의 추이차트뿐. 날짜탭·매출탭 등 나머지는 Mixpanel 그대로.
//   · 되돌리려면 이 Set 을 비우면 된다(코드 수정 불필요).
// ★ 2026-08-04 부터는 글로벌 매출을 Mixpanel 기준으로 확정한다(사용자 결정).
//   GL_MP_FROM 이후 날짜는 Set 에 뭐가 들어있든 Meta 치환을 하지 않는다 → 규칙이 뒤로 밀릴 일 없음.
const GL_MP_FROM='2026-08-04';
const GL_META_DAYS=new Set(['2026-07-31','2026-08-01','2026-08-02','2026-08-03']);
function isGlMetaDay(d){return !!d&&d<GL_MP_FROM&&GL_META_DAYS.has(d)}
function glMetaRow(r){
  if(!isGlMetaDay(r.date))return r;
  const sp=+r.spend||0;
  const cnt=Math.round(+r.results_meta||0);
  // 메타 ROAS(배수)가 있으면 그걸로 매출을 만들고, 없으면(값 추적이 없는 세트) 구매수만 살린다.
  //   메타는 구매수는 잡아도 purchase_roas 를 0 으로 주는 경우가 흔하다 → 매출 0 은 그대로 두되
  //   구매수·CVR·구매당비용은 메타 기준으로 보이게 한다.
  const rev=sp*(+r.purchase_roas_meta||0);
  const uc=+r.unique_clicks||0;
  // ★ 셀은 저장된 r.roas / r.cvr 을 그대로 쓴다(재계산하지 않는다) → 여기서 같이 갈아끼워야
  //   ROAS·CVR 만 Mixpanel 기준으로 남아 0 으로 찍히는 일이 없다.
  return {...r,revenue:rev,profit:rev-sp,results_mp:cnt,
          roas:sp>0?rev/sp*100:0,
          cvr:(uc>0&&cnt>0)?cnt/uc*100:0,
          cost_per_result:cnt>0?sp/cnt:0,
          _glMeta:1};
}

// Row ID: adset_id for kr/gl, ad_id for creative
function rowId(r){return MODE==='cr'?(r.ad_id||''):(r.adset_id||'')}
function rowName1(r){return MODE==='cr'?(r.ad_name||'').slice(0,25):(r.adset_name||'').slice(0,25)}
function rowIdLabel(){return MODE==='cr'?'소재 ID':'세트 ID'}
function rowNameLabel(){return MODE==='cr'?'소재':'세트'}

// ===== 국가 필터 (vn/gl: country grain 데이터를 adset당 1행으로) =====
const _COUNTRY_MODES={vn:1,gl:1};   // country 컬럼이 있는 모드
function _modeSrc(m){return m==='kr'?KR_AD:m==='gl'?GL_AD:m==='cr'?CR_AD:VN_AD;}
// 국가 코드 정규화 — 한글명/풀네임/소문자 등 변형 표기를 단일 ISO 코드로 통합(드롭다운 중복 방지).
//   예: '홍콩','hongkong','hk' → 'HK'.  미상('','none','unknown','xx') → 'XX'.
const _CC_CANON={'홍콩':'HK','香港':'HK','HONGKONG':'HK','HONG KONG':'HK',
  '대만':'TW','台灣':'TW','台湾':'TW','TAIWAN':'TW','일본':'JP','日本':'JP','JAPAN':'JP',
  '한국':'KR','대한민국':'KR','KOREA':'KR','SOUTH KOREA':'KR','미국':'US','USA':'US','UNITED STATES':'US',
  '태국':'TH','THAILAND':'TH','베트남':'VN','VIETNAM':'VN','싱가포르':'SG','SINGAPORE':'SG',
  '마카오':'MO','MACAU':'MO','MACAO':'MO','영국':'GB','UK':'GB','UNITED KINGDOM':'GB',
  '호주':'AU','AUSTRALIA':'AU','말레이시아':'MY','MALAYSIA':'MY','인도네시아':'ID','INDONESIA':'ID',
  '독일':'DE','GERMANY':'DE','프랑스':'FR','FRANCE':'FR'};
function canonCountry(c){
  const s=String(c==null?'':c).trim(); if(!s) return 'XX';
  const u=s.toUpperCase();
  if(u==='ALL') return 'ALL';
  if(['NONE','NULL','UNDEFINED','UNKNOWN','XX','-'].includes(u)) return 'XX';
  return _CC_CANON[s]||_CC_CANON[u]||u;
}
// 선택 국가로 필터 + (전체일 땐) (date,adset_id) 합산 → 기존 뷰가 1행/adset 전제이므로 호환 유지
function _applyCountry(rows){
  if(COUNTRY!=='ALL') return rows.filter(r=>canonCountry(r.country)===COUNTRY);
  const ADDF=['spend','revenue','profit','budget','impressions','reach','results_mp','results_meta','results_meta_click','unique_clicks'];
  const by={};
  rows.forEach(r=>{
    const k=r.date+'|'+(r.adset_id||'');
    let o=by[k];
    if(!o){o=by[k]=Object.assign({},r);ADDF.forEach(f=>o[f]=0);}
    ADDF.forEach(f=>{const v=+r[f];if(!isNaN(v))o[f]+=v;});
  });
  return Object.keys(by).map(k=>{
    const o=by[k],sp=o.spend||0,rv=o.revenue||0,im=o.impressions||0,uc=o.unique_clicks||0,rm=o.results_meta||0,re=o.reach||0,mp=o.results_mp||0;
    o.roas=sp>0?rv/sp*100:0; o.cvr=(uc>0&&mp>0)?mp/uc*100:0; o.cpm=im>0?sp/im*1000:0;
    o.cost_per_click=uc>0?sp/uc:0; o.cost_per_result=rm>0?sp/rm:0;
    o.unique_ctr=im>0?uc/im*100:0; o.frequency=re>0?im/re:0;
    o.country='ALL'; return o;
  });
}
// 현재 모드의 작업용 AD 배열 생성 (norm 후 국가필터/합산)
function buildAD(m){
  const rows=_modeSrc(m).map(norm);
  return _COUNTRY_MODES[m]?_applyCountry(rows):rows;
}
// 국가 드롭다운: 해당 모드 데이터의 실제 country 목록으로 채움
function _populateCountrySel(m){
  const sel=document.getElementById('countrySel'); if(!sel) return;
  if(!_COUNTRY_MODES[m]){sel.style.display='none'; COUNTRY='ALL'; return;}
  const set={};
  _modeSrc(m).forEach(r=>{const c=canonCountry(r.country); if(c&&c!=='ALL'&&c!=='XX')set[c]=(set[c]||0)+(+ (r.spend!=null?r.spend:r.spend_usd)||0);});
  const codes=Object.keys(set).sort((a,b)=>set[b]-set[a]);   // 지출 큰 순
  if(!codes.includes(COUNTRY)) COUNTRY='ALL';
  sel.innerHTML='<option value="ALL">🌐 전체</option>'+codes.map(c=>'<option value="'+c+'">'+c+'</option>').join('');
  sel.value=COUNTRY; sel.style.display='';
}
function onCountryChange(){
  COUNTRY=document.getElementById('countrySel').value||'ALL';
  AD=buildAD(MODE); rebuildLookups();
  const t=document.querySelector('.tab.active'); if(t)renderTab(t.dataset.t);
  navPush();   // 국가 필터 변경도 히스토리에 기록
}

// 주간종합 '소스' 셀렉트를 모드에 맞춘다 (2026-08-18).
//   본문 데이터(ROWS=AD)는 이미 모드별로 재빌드되지만(buildAD) 이 셀렉트는 정적 마크업이라
//   글로벌·밴스드·국내소재 모드에서도 '국내 메타'로 보였다. 더 나쁜 건 dg/both 옵션이 읽는
//   GGDG_TIGHT(google_demandgen_campaign_daily)가 국내 원화 전용이라는 것 — 글로벌 모드에서
//   고르면 ₩ 수치가 $ 로 찍히거나(dg) USD 합계에 원화가 그대로 더해졌다(both).
//   → 소스 선택은 국내(kr)에서만 열고, 나머지 모드는 해당 모드 메타 1개로 고정·라벨 교체.
//   ('🟢 구글 디멘드젠' 탭이 m==='kr' 전용으로 숨겨지는 것과 같은 기준)
const _WSRC_LABEL={kr:'국내 메타',gl:'글로벌 메타',vn:'밴스드 메타',cr:'국내소재 메타'};
let _wSrcKr='kr';    // 국내 모드에서 고른 소스를 기억 → 모드 왕복해도 복원
let _wSrcGl='glv';   // 글로벌 모드 선택 기억. 기본=밴스드 포함(매출탭 grevVanced 와 동일 기본값)
let _wSrcHint0=null; // index.html 에 적힌 국내(디멘드젠) 안내문구 원본
const _WSRC_HINT_GL='💡 밴스드=대만 밴스드 계정(vanced_ad_performance_daily) 지출·매출을 일별 USD/KRW 환율로 환산해 합산. 국가필터가 대만·전체가 아니면 밴스드는 자동 제외';
function _syncWeeklySource(m){
  const sel=document.getElementById('wSource'); if(!sel) return;
  const hint=document.getElementById('wSrcHint');
  if(hint&&_wSrcHint0===null)_wSrcHint0=hint.textContent;
  if(m==='kr'){
    sel.innerHTML='<option value="kr">국내 메타</option><option value="dg">디멘드젠(타이트)</option><option value="both">국내 종합(메타+디멘드젠)</option>';
    sel.value=_wSrcKr; sel.disabled=false;
    if(hint){hint.style.display='';hint.textContent=_wSrcHint0;}
  }else if(m==='gl'){
    sel.innerHTML='<option value="glv">글로벌 메타 (밴스드 포함)</option><option value="kr">글로벌 메타 (밴스드 미포함)</option>';
    sel.value=_wSrcGl; sel.disabled=false;
    if(hint){hint.style.display='';hint.textContent=_WSRC_HINT_GL;}
  }else{
    sel.innerHTML='<option value="kr">'+(_WSRC_LABEL[m]||'메타')+'</option>';
    sel.value='kr'; sel.disabled=true;   // 디멘드젠(국내 원화)은 선택지에서 제거
    if(hint)hint.style.display='none';
  }
}

// ===== MODE SWITCH =====
// applyMode = 화면만 바꾸는 순수 적용 함수(히스토리 기록 없음).
// 사용자가 직접 누르는 경로는 아래 switchMode() 래퍼가 담당해 히스토리에 기록한다.
function applyMode(m){
  MODE=m;
  _syncWeeklySource(m);
  document.querySelectorAll('.mode-btn').forEach(b=>b.classList.toggle('active',b.dataset.mode===m));
  const mLabels={kr:'국내 · KRW',gl:'글로벌 · USD',cr:'국내소재 · KRW',vn:'밴스드 · KRW',kpi:'📈 지표 하이아라키 KPI',mkt:'👤 마케터별 소재 성과',exp:'🧪 실험 · 세트 A/B 비교'};
  document.getElementById('modeLabel').textContent=mLabels[m]||m;
  const isKpi=(m==='kpi');
  const isMkt=(m==='mkt');
  const isExp=(m==='exp');
  const isSpecial=isKpi||isMkt||isExp;
  // Tab visibility (unified)
  document.querySelectorAll('.tab').forEach(t=>{
    if(t.dataset.t==='vntwtrend'){t.style.display='none';return} // 레거시 대만 추이차트 — 국가 필터(countrySel)로 대체
    if(t.classList.contains('exp-only')){t.style.display=isExp?'inline-block':'none';return}
    if(t.classList.contains('mkt-only')){t.style.display=isMkt?'inline-block':'none';return}
    if(t.classList.contains('kpi-only')){t.style.display=isKpi?'inline-block':'none';return}
    if(t.classList.contains('global-only')){t.style.display=(!isSpecial&&m==='gl')?'inline-block':'none';return}
    if(t.dataset.t==='ggdgkr'){t.style.display=(!isSpecial&&m==='kr')?'inline-block':'none';return} // [Tight] 디멘드젠은 국내(kr) 전용 (vn 의 ggdgct 와 라벨 중복 방지)
    if(t.dataset.t==='tiktok'){t.style.display=(!isSpecial&&m==='kr')?'inline-block':'none';return} // 틱톡은 국내(kr) 전용 — 밴스드(vn)엔 틱톡 집행이 없다
    if(t.dataset.t==='krank'){t.style.display=(!isSpecial&&(m==='kr'||m==='vn'||m==='gl'))?'inline-block':'none';return} // 세트랭킹: 국내·밴스드·글로벌 공통(kr-only 클래스보다 먼저 처리)
    if(t.dataset.t==='dupvar'){t.style.display=(!isSpecial&&(m==='kr'||m==='vn'||m==='gl'))?'inline-block':'none';return} // 복제·변형 계보: 국내·밴스드·글로벌 공통
    if(t.classList.contains('kr-only')){t.style.display=(!isSpecial&&(m==='kr'||m==='vn'))?'inline-block':'none';return}
    if(t.classList.contains('vn-only')){t.style.display=(!isSpecial&&m==='vn')?'inline-block':'none';return}
    if(t.classList.contains('creative-only')){t.style.display=(!isSpecial&&m==='cr')?'inline-block':'none';return}
    t.style.display=isSpecial?'none':'inline-block';
  });
  if(isMkt){
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
    document.querySelector('.tab[data-t="mktDash"]').classList.add('active');
    document.getElementById('p-mktDash').classList.add('active');
    if(!CR_AD.length)ensureBigTable('cr').then(()=>renderMarketer());else renderMarketer();
    return;
  }
  if(isKpi){
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
    document.querySelector('.tab[data-t="kpiDash"]').classList.add('active');
    document.getElementById('p-kpiDash').classList.add('active');
    ensureKpiData().then(()=>renderTab('kpiDash'));
    return;
  }
  if(isExp){
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
    const _et=document.querySelector('.tab[data-t="exp"]');if(_et)_et.classList.add('active');
    document.getElementById('p-exp').classList.add('active');
    _expEnsureSrc(()=>renderExperiment());  // 소재(ad_id) · 구글 디멘드젠 세트(ad_group_id) 조회용 lazy 로드
    renderExperiment();
    return;
  }
  // returning from KPI mode: if active tab is now hidden, reset to dashboard
  {const _at=document.querySelector('.tab.active');
   if(!_at||_at.classList.contains('kpi-only')||_at.classList.contains('mkt-only')||(_at.classList.contains('vn-only')&&m!=='vn')){
     document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
     document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
     document.querySelector('.tab[data-t="dashboard"]').classList.add('active');
     document.getElementById('p-dashboard').classList.add('active');
   }}
  // Switch data (lazy-load big tables)
  _populateCountrySel(m);
  if(m==='kr'){AD=buildAD('kr');HIGHLIGHTS=KR_HL;HL_MEMO=KR_HM;HL_SRC=KR_SRC}
  else if(m==='gl'){AD=buildAD('gl');HIGHLIGHTS=GL_HL;HL_MEMO=GL_HM;HL_SRC=GL_SRC}
  else if(m==='cr'){AD=buildAD('cr');HIGHLIGHTS=CR_HL;HL_MEMO=CR_HM;HL_SRC=CR_SRC;
    if(!CR_AD.length)ensureBigTable('cr').then(()=>{AD=buildAD('cr');rebuildLookups();const t=document.querySelector('.tab.active');if(t)renderTab(t.dataset.t);});}
  else if(m==='vn'){AD=buildAD('vn');HIGHLIGHTS=VN_HL;HL_MEMO=VN_HM;HL_SRC=VN_SRC}
  rebuildLookups();
  // Reset selects
  dashState.dateA=null;dashState.dateB=null;
  document.getElementById('dtStart').innerHTML='';
  document.getElementById('dtEnd').innerHTML='';
  {const _df=document.getElementById('dtFilter');if(_df)_df.value=''}  // 모드 바뀌면 필터 해제 — 이전 모드 키워드로 표가 빈 채 보이는 사고 방지
  document.getElementById('dpSel').innerHTML='';
  // Render active tab
  const activeTab=document.querySelector('.tab.active');
  if(activeTab)renderTab(activeTab.dataset.t);
}

// ===== 라우팅 · 브라우저 히스토리 (뒤로가기/앞으로가기) =====
// 화면 상태 = MODE(모드 버튼) × 활성 탭 × COUNTRY(국가 필터) 세 가지다.
// 이 셋을 URL 해시(#모드/탭/국가)에 실어 pushState 로 기록하면 뒤로가기가 이전 화면으로 돌아간다.
// 해시를 쓰는 이유: Vercel 제로컨피그 정적 배포라 경로형(/gl/trend)을 쓰면 새로고침 시 404 가 나고
// rewrite 설정이 따로 필요하다. 해시는 서버 설정 없이 새로고침·북마크·공유가 그대로 된다.
let _navApplying=false;   // popstate 복원 중 재기록 방지 (뒤로가기 무한루프 차단)

function navState(){
  const at=document.querySelector('.tab.active');
  return {mode:MODE, tab:at?at.dataset.t:null, country:COUNTRY||'ALL'};
}
function navHash(s){
  const c=(s.country&&s.country!=='ALL')?s.country:'';
  const t=s.tab||'';
  if(!t&&!c)return '#'+s.mode;              // 실험 모드처럼 탭이 없는 화면
  if(!c)return '#'+s.mode+'/'+t;
  return '#'+s.mode+'/'+(t||'-')+'/'+c;
}
function navParse(h){
  const p=String(h||'').replace(/^#/,'').split('/').filter(x=>x!=='');
  if(!p.length)return null;
  if(['kr','gl','cr','vn','exp','kpi','mkt'].indexOf(p[0])<0)return null;   // 모르는 해시는 무시
  return {mode:p[0], tab:(p[1]&&p[1]!=='-')?p[1]:null, country:p[2]||'ALL'};
}
function navPush(replace){
  if(_navApplying)return;                                    // 복원 중에는 기록하지 않는다
  const s=navState(), h=navHash(s);
  if(!replace&&location.hash===h)return;                     // 같은 화면 반복 클릭은 기록하지 않는다
  try{history[replace?'replaceState':'pushState'](s,'',h)}catch(e){}
}
function navApply(s,force){
  _navApplying=true;
  try{
    const modeChanged=(s.mode!==MODE);
    const countryChanged=((s.country||'ALL')!==COUNTRY);
    COUNTRY=s.country||'ALL';    // applyMode 안의 _populateCountrySel 이 이 값을 유지·보정한다
    // 목표 탭을 먼저 활성화해 두면 applyMode 가 그 탭을 바로 렌더한다 (이중 렌더 방지)
    const el=s.tab?document.querySelector('.tab[data-t="'+s.tab+'"]'):null;
    const panel=s.tab?document.getElementById('p-'+s.tab):null;
    if(el&&panel){
      document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
      el.classList.add('active');panel.classList.add('active');
    }
    if(force||modeChanged){
      applyMode(s.mode);                                     // 데이터 재구성 + 활성 탭 렌더까지 담당
    } else if(countryChanged){
      const sel=document.getElementById('countrySel');if(sel)sel.value=COUNTRY;
      AD=buildAD(MODE);rebuildLookups();
      const t=document.querySelector('.tab.active');if(t)renderTab(t.dataset.t);
    } else if(el&&panel){
      renderTab(s.tab);                                      // 같은 모드 안에서 탭만 이동
    }
    // 그 모드에 없는 탭 조합(예: #gl/chrev)으로 들어온 경우 대시보드로 안전 복귀
    const at=document.querySelector('.tab.active');
    if(at&&at.style.display==='none'){
      const d=document.querySelector('.tab[data-t="dashboard"]');
      if(d&&d.style.display!=='none'){
        document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
        document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
        d.classList.add('active');document.getElementById('p-dashboard').classList.add('active');
        renderTab('dashboard');
      }
    }
  }finally{_navApplying=false}
}
// 사용자가 모드 버튼을 누르는 경로 — 화면을 바꾸고 히스토리에 한 칸 쌓는다 (index.html 의 onclick 이 부른다)
function switchMode(m){applyMode(m);navPush()}
// 최초 진입·새로고침: URL 이 가리키는 화면으로 복원한다 (해시가 없으면 국내 대시보드)
function navBoot(){
  const s=navParse(location.hash)||{mode:'kr',tab:'dashboard',country:'ALL'};
  navApply(s,true);
  navPush(true);            // 첫 화면은 새 기록을 쌓지 않고 현재 항목을 덮어쓴다
}
window.addEventListener('popstate',e=>{
  // 시간별 ROAS 화면이 열려 있으면 뒤로가기는 '그 화면 닫기'다 (추이차트를 다시 렌더하지 않는다 —
  // 재렌더는 스크롤 위치를 잃는다). hrOpen 이 해시 그대로 상태 한 칸을 쌓아 두었다.
  if(typeof HR_OPEN!=='undefined'&&HR_OPEN){hrClose();return}
  const s=e.state||navParse(location.hash);
  if(s)navApply(s,false);
});

function rebuildLookups(){
  const bd={};
  AD.forEach(r=>{if(!bd[r.date])bd[r.date]={s:0,r:0,p:0,mp:0,uc:0,imp:0};bd[r.date].s+=r.spend;bd[r.date].r+=r.revenue;bd[r.date].p+=r.profit;bd[r.date].mp+=r.results_mp;bd[r.date].uc+=r.unique_clicks;bd[r.date].imp+=(r.impressions||0)});
  DAILY=bd;DATES=Object.keys(bd).sort().reverse();
  const bp={};AD.forEach(r=>{if(!bp[r.product])bp[r.product]=0;bp[r.product]+=r.spend});
  PRODS=Object.keys(bp).sort((a,b)=>bp[b]-bp[a]);
  _csvDefaultDates();
}

// ===== 기간별 CSV 다운로드 =====
// zoom-bar 의 시작/종료일을 선택하면 현재 모드(AD)의 원본 행을 해당 기간으로 필터링해 CSV 저장.
function _csvEscape(v){
  if(v==null)return'';
  let s=(typeof v==='object')?JSON.stringify(v):String(v);
  if(/[",\n\r]/.test(s))s='"'+s.replace(/"/g,'""')+'"';
  return s;
}
function _csvDefaultDates(){
  const s=document.getElementById('csvStart'),e=document.getElementById('csvEnd');
  if(!s||!e||!DATES.length)return;
  const max=DATES[0],min=DATES[DATES.length-1];        // DATES 는 최신순 정렬
  s.min=e.min=min;s.max=e.max=max;
  if(!e.value)e.value=max;                              // 기본 종료일 = 최신 데이터일
  if(!s.value){                                         // 기본 시작일 = 최신 -29일 (최근 30일)
    const d=new Date(max);d.setDate(d.getDate()-29);
    const lo=d.toISOString().slice(0,10);
    s.value=lo<min?min:lo;
  }
}
function csvDownloadRange(){
  if(!AD||!AD.length){alert('데이터가 아직 로드되지 않았습니다. 잠시 후 다시 시도하세요.');return}
  const sv=document.getElementById('csvStart').value,ev=document.getElementById('csvEnd').value;
  if(!sv||!ev){alert('시작일과 종료일을 선택하세요.');return}
  const lo=sv<=ev?sv:ev, hi=sv<=ev?ev:sv;              // 순서 뒤바뀌어도 보정
  const rows=AD.filter(r=>r.date>=lo&&r.date<=hi).sort((a,b)=>a.date<b.date?-1:a.date>b.date?1:0);
  if(!rows.length){alert('선택한 기간('+lo+' ~ '+hi+')에 데이터가 없습니다.');return}
  // 컬럼 순서: 주요 컬럼 우선, 나머지는 등장 순서대로 (데이터 누락 없이 전체 필드 포함)
  const cols=[],seen=new Set();
  ['date','product','campaign_name','campaign_id','adset_name','adset_id','ad_name','ad_id',
   'spend','revenue','profit','results_mp','unique_clicks','impressions','budget']
    .forEach(k=>{if(rows.some(r=>k in r)){cols.push(k);seen.add(k)}});
  rows.forEach(r=>Object.keys(r).forEach(k=>{if(!seen.has(k)){seen.add(k);cols.push(k)}}));
  const lines=[cols.join(',')];
  rows.forEach(r=>lines.push(cols.map(c=>_csvEscape(r[c])).join(',')));
  const csv='﻿'+lines.join('\r\n');          // BOM: Excel 한글 깨짐 방지
  const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;a.download='tightauto_'+MODE+'_'+lo+'_'+hi+'.csv';
  document.body.appendChild(a);a.click();a.remove();
  setTimeout(()=>URL.revokeObjectURL(url),1000);
}

// ===== 코어 데이터 경고 배너 =====
// fetch 실패나 0건은 화면상 '지출/매출이 진짜 0' 과 구분되지 않는다 → 상단에 명시적으로 띄운다.
//   (2026-08-11: 글로벌 광고 테이블이 비어 매출탭 '글로벌' 행 지출이 0·ROAS 0 으로 그려진 사고)
// ★ 낡음(stale) 감지도 함께 한다 — 캐시(IndexedDB)로 그린 뒤 fresh fetch 가 실패하거나
//   아예 응답 없이 매달리면, 화면은 '옛날 데이터'를 아무 표시 없이 계속 보여준다.
//   실제로 글로벌 탭이 9일간 8/6 에 고착됐는데 배너가 없어 아무도 몰랐다(2026-08-16).
//   각 코어 테이블의 최신 날짜가 '어제'보다 오래되면 며칠 밀렸는지 배너에 띄운다.
//   (파이프라인 cron 이 매일 밤에 어제치를 적재하므로 '어제'가 정상 하한 — 하루 여유가 있어 오탐 없음)
function _lastDate(rows){let m='';(rows||[]).forEach(r=>{if(r&&r.date>m)m=r.date});return m}
function _staleList(){
  const _y=new Date();_y.setDate(_y.getDate()-1);
  const yDay=_y.getFullYear()+'-'+String(_y.getMonth()+1).padStart(2,'0')+'-'+String(_y.getDate()).padStart(2,'0');
  const out=[];
  [['국내 광고',KR_AD],['글로벌 광고',GL_AD],['밴스드 광고',VN_AD]].forEach(([label,rows])=>{
    if(!rows||!rows.length)return;                 // 0건은 empty 쪽에서 이미 경고
    const last=_lastDate(rows);
    if(!last||last>=yDay)return;
    const days=Math.round((new Date(yDay)-new Date(last))/864e5);
    out.push(label+' (최신 '+last+', '+days+'일 밀림)');
  });
  return out;
}
// staleOnly=true: 캐시 렌더 직후 호출용. 이 시점의 '0건'은 아직 fresh fetch 전이라
//   정상인 경우가 많다(캐시에 없던 테이블) → 낡음만 본다. 실패/0건 판정은 fetch 완료 후에.
function renderCoreWarn(staleOnly){
  const fails=staleOnly?[]:Object.keys(CORE_FAIL||{});
  const empty=[];
  if(!staleOnly){
    if(!KR_AD.length)empty.push('국내 광고');
    if(!GL_AD.length)empty.push('글로벌 광고');
    if(!VN_AD.length)empty.push('밴스드 광고');
    if(!STRIPE_DATA.length)empty.push('Stripe 매출');
    if(!TOSS_DAILY.length)empty.push('토스 매출');
  }
  const stale=_staleList();
  let el=document.getElementById('coreWarn');
  if(!fails.length&&!empty.length&&!stale.length){if(el)el.remove();return}
  if(!el){el=document.createElement('div');el.id='coreWarn';document.body.appendChild(el)}
  el.style.cssText='position:fixed;left:0;right:0;top:0;z-index:9999;background:#b91c1c;color:#fff;'
    +'font-size:12px;line-height:1.5;padding:7px 12px;box-shadow:0 2px 6px rgba(0,0,0,.3);font-family:inherit';
  const btn='style="margin-left:8px;padding:2px 8px;border:1px solid #fff;border-radius:3px;background:transparent;color:#fff;font-size:11px;cursor:pointer;font-family:inherit"';
  // 사유(CORE_FAIL 값)까지 띄운다 — 테이블 이름만 보면 '세션 만료' 인지 '네트워크 차단' 인지
  //   구분이 안 돼서, 콘솔을 열지 않으면 원인을 못 찾았다(2026-08-23). 테이블명 접두어는 떼고 중복 제거.
  const esc=s=>String(s).replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]));
  const reasons=staleOnly?[]:[...new Set(Object.values(CORE_FAIL||{}).map(v=>String(v).replace(/^\S+\s+/,'')))].slice(0,3);
  const isAuth=reasons.some(r=>/401|JWT|permission denied/i.test(r));
  const head=(fails.length||empty.length)
    ? '⚠️ <b>데이터 로드 실패</b> — 아래 지표는 0 으로 그려질 수 있습니다(실제 0 이 아님). '
    : '⚠️ <b>옛날 데이터를 보고 있습니다</b> — 최신 적재분이 반영되지 않았습니다. ';
  el.innerHTML=head
    +(fails.length?'<b>실패:</b> '+fails.join(' · ')+'. ':'')
    +(reasons.length?'<b>사유:</b> '+esc(reasons.join(' / '))+'. ':'')
    +(isAuth?'<b>→ 로그인 세션 문제입니다. [🔑 다시 로그인] 을 누르세요.</b> ':'')
    +(empty.length?'<b>0건:</b> '+empty.join(' · ')+'. ':'')
    +(stale.length?'<b>낡음:</b> '+stale.join(' · ')+'. ':'')
    +(isAuth?'<button onclick="authLost(\'다시 로그인해 주세요.\')" '+btn+'>🔑 다시 로그인</button>':'')
    +'<button onclick="location.reload()" '+btn+'>🔄 다시 불러오기</button>'
    +'<button onclick="purgeCacheAndReload()" '+btn+'>🧹 캐시 비우고 다시 불러오기</button>'
    +'<span onclick="document.getElementById(\'coreWarn\').remove()" style="float:right;cursor:pointer;padding:0 4px">✕</span>';
}

// ===== INIT =====
// ★ null = 그 테이블 fetch 실패(_cf 가 표시) → 기존 값을 유지한다.
//   빈 배열로 덮으면 화면이 조용히 0 으로 그려진다(2026-08-11 글로벌 지출 0 사고).
function _applyCore(d){
  const keep=(v,cur)=>(v==null?cur:v);
  KR_AD=keep(d.kr,KR_AD);GL_AD=d.gl==null?GL_AD:canonGLRows(d.gl);VN_AD=keep(d.vn,VN_AD);
  STRIPE_DATA=keep(d.stripe,STRIPE_DATA);TOSS_DATA=keep(d.toss,TOSS_DATA);
  NSA_DAILY=keep(d.nsa,NSA_DAILY);GOOGLE_ADS=keep(d.google,GOOGLE_ADS);GOOGLE_DG=keep(d.googledg,GOOGLE_DG);NAVER_PL=keep(d.naverpl,NAVER_PL);
  NAVER_MP=keep(d.naver_mp,NAVER_MP);TOSS_DAILY=keep(d.toss_kr,TOSS_DAILY);
  if(d.alimtalk!=null)ALIMTALK=d.alimtalk;  // CRM 채널(알림톡) — null(실패)이면 기존 값 유지
  // 하이라이트: in-place merge — HIGHLIGHTS 참조 identity 유지 + 로컬 쓰기(저장 직후) 보존
  // (이전엔 KR_HL={} 으로 재할당해서 HIGHLIGHTS 가 옛 객체에 dangle → 모드 전환/refresh 시 사라짐)
  // ★ 오늘(updated_at ≥ 오늘0시) 마킹만 적용 — 지난날 것은 스킵(자동 삭제). 로컬 저장 보존 위해 delete는 안 함.
  (d.krhl||[]).forEach(x=>{if(_hlIsToday(x.updated_at)){KR_HL[x.adset_id]=x.highlight;KR_HM[x.adset_id]=x.memo||null;KR_SRC[x.adset_id]=x.source||null}});
  (d.glhl||[]).forEach(x=>{if(_hlIsToday(x.updated_at)){GL_HL[x.adset_id]=x.highlight;GL_HM[x.adset_id]=x.memo||null;GL_SRC[x.adset_id]=x.source||null}});
  (d.crhl||[]).forEach(x=>{if(_hlIsToday(x.updated_at)){CR_HL[x.ad_id]=x.highlight;CR_HM[x.ad_id]=x.memo||null;CR_SRC[x.ad_id]=x.source||null}});
  (d.vnhl||[]).forEach(x=>{if(_hlIsToday(x.updated_at)){VN_HL[x.adset_id]=x.highlight;VN_HM[x.adset_id]=x.memo||null;VN_SRC[x.adset_id]=x.source||null}});
  // durable 메모(daily_memos) → DMEMO 맵. dmemo 없으면(캐시경로/미실행) 기존 값 유지.
  if(d.dmemo){DMEMO={};d.dmemo.forEach(x=>{if(x&&x.date&&x.entity_id&&x.region)DMEMO[_dmKey(x.region,x.date,x.entity_id)]=x.memo||null})}
  memoIdxInvalidate();
}
async function initData(){
  // 0시 롤오버 감지 → 추이차트 하이라이트·메모 자동 삭제 (탭 열려있는 상태로 자정 넘길 때)
  HL_DAY=_hlDayStr();
  if(!window._hlMidnightTimer){
    window._hlMidnightTimer=setInterval(()=>{const t=_hlDayStr();if(t!==HL_DAY){HL_DAY=t;autoClearTrendHL()}},30000);
  }
  const D180='&date=gte.'+_dateCutoff(215);
  // 1단계: IndexedDB 캐시에서 모든 테이블 즉시 로드 (병렬)
  const cKeys=['kr','gl','vn','krhl','glhl','crhl','vnhl','stripe','toss','nsa','google','googledg','naverpl','naver_mp','toss_kr','cr','glcr','nsa_kw','naver_kw','ggdg_ct','ggdg_sp','ggdg_tight','gcamp','alimtalk'];
  const cached={};
  await Promise.all(cKeys.map(k=>cacheGet('t_'+k).then(v=>{cached[k]=v?.data;})));
  let renderedFromCache=false;
  if(cached.kr&&cached.kr.length){
    _applyCore(cached);
    CR_AD=cached.cr||[];
    GL_CR=canonGLRows(cached.glcr||[]);   // 마케터탭(글로벌) 즉시 표시용 — fresh 는 renderMarketer 가 백그라운드로 갱신
    NSA_KW=cached.nsa_kw||[];NAVER_KW=cached.naver_kw||[];GGDG_CT=cached.ggdg_ct||[];GGDG_SP=cached.ggdg_sp||[];GGDG_TIGHT=cached.ggdg_tight||[];GCAMP=cached.gcamp||[];
    // ⚠️ _BIG_LOADED.ggdgct 를 캐시에서 true 로 두지 않는다 — 그러면 ensureBigTable 이
    //    영영 fresh fetch 를 안 해서 새 적재(신규 날짜 지출 등)가 안 보임.
    //    캐시는 즉시 표시용, 갱신은 renderGgdgContent 의 stale-while-revalidate 가 담당.
    window._CR_BY_ADSET=null;
    navBoot(); // 캐시로 즉시 렌더 — URL 해시가 가리키는 화면으로 복원(없으면 국내 대시보드)
    renderedFromCache=true;
    // 캐시가 낡았으면 이 시점에 바로 알린다. fresh fetch 가 응답 없이 매달리면
    // applyAndRerender 가 영영 안 불려서 배너도 못 뜬다 — 그 구멍을 막는다.
    // (fresh 가 도착하면 applyAndRerender 의 renderCoreWarn 이 배너를 지운다)
    renderCoreWarn(true);
  }
  // 2단계: 핵심 데이터 fresh fetch (캐시 있으면 백그라운드, 없으면 await)
  //   실패한 테이블은 [] 가 아니라 null 로 넘긴다 → _applyCore 가 기존 값 유지, 캐시도 덮어쓰지 않음.
  //   무엇이 실패했는지는 CORE_FAIL 에 모아 상단 경고 배너로 노출한다(조용한 0 방지).
  CORE_FAIL={};
  const _cf=(label,p)=>p.catch(e=>{CORE_FAIL[label]=String(e&&e.message||e);console.error('[core fetch 실패]',label,e);return null});
  const corePromise=Promise.all([
    _cf('국내 광고(ad_performance_daily)',sbAll('ad_performance_daily','spend',D180)),
    _cf('글로벌 광고(global_ad_performance_daily)',sbAll('global_ad_performance_daily','spend_usd',D180)),
    _cf('밴스드 광고(vanced_ad_performance_daily)',sbAll('vanced_ad_performance_daily','spend',D180)),
    sbQ('adset_highlights','select=*').catch(()=>null),
    sbQ('global_adset_highlights','select=*').catch(()=>null),
    sbQ('ad_creative_highlights','select=*').catch(()=>null),
    sbQ('vanced_adset_highlights','select=*').catch(()=>null),
    _cf('Stripe 매출(global_stripe_daily)',sbQ('global_stripe_daily','select=*&order=date.desc&limit=2000')),
    _cf('토스 매출(toss_daily_revenue)',sbQ('toss_daily_revenue','select=*&order=date.desc&limit=2000')),
    _cf('네이버SA(naver_sa_daily)',sbAll('naver_sa_daily','cost_vat',D180)),
    _cf('구글 광고(google_ads_daily)',sbQ('google_ads_daily','select=*&order=date.desc&limit=2000')),
    sbQ('naver_daily_mp','select=*&order=date.desc&limit=2000').catch(()=>null),
    _cf('토스 매출(toss_daily_revenue)',sbQ('toss_daily_revenue','select=*&order=date.desc&limit=2000')),
    sbQ('google_demandgen_daily','select=*&order=date.desc&limit=2000').catch(()=>null),
    _cf('네이버 파워링크(naver_powerlink_daily)',sbQ('naver_powerlink_daily','select=*&order=date.desc&limit=2000')),
    sbQ('daily_memos','select=*').catch(()=>null),
    _cf('CRM 알림톡(alimtalk_daily_campaign)',sbQ('alimtalk_daily_campaign','select=date,campaign_key,rev,sent,cost&order=date.desc&limit=5000')),
  ]).then(arr=>{
    const [kr,gl,vn,krhl,glhl,crhl,vnhl,stripe,toss,nsa,google,naver_mp,toss_kr,googledg,naverpl,dmemo,alimtalk]=arr;
    return {kr,gl,vn,krhl,glhl,crhl,vnhl,stripe,toss,nsa,google,naver_mp,toss_kr,googledg,naverpl,dmemo,alimtalk};
  });
  const applyAndRerender=async(d)=>{
    _applyCore(d);
    purgeStaleTrendHL();  // fresh 로드 시 DB의 지난날(<오늘0시) 하이라이트 행 정리(1회)
    if(renderedFromCache){
      // AD 는 switchMode 시점의 스냅샷이므로 fresh VN_AD/KR_AD 갱신 후 재할당 필요
      // 안 그러면 DATES 가 stale 캐시 기준이라 today/yesterday 가 dd 윈도우에서 누락됨
      _populateCountrySel(MODE);
      if(MODE==='kr'){AD=buildAD('kr');HIGHLIGHTS=KR_HL;HL_MEMO=KR_HM;HL_SRC=KR_SRC}
      else if(MODE==='gl'){AD=buildAD('gl');HIGHLIGHTS=GL_HL;HL_MEMO=GL_HM;HL_SRC=GL_SRC}
      else if(MODE==='cr'){AD=buildAD('cr');HIGHLIGHTS=CR_HL;HL_MEMO=CR_HM;HL_SRC=CR_SRC}
      else if(MODE==='vn'){AD=buildAD('vn');HIGHLIGHTS=VN_HL;HL_MEMO=VN_HM;HL_SRC=VN_SRC}
      rebuildLookups();
      // 펼침 인덱스도 무효화 (다음 클릭 시 fresh 로 재구축)
      window._CR_BY_ADSET=null;
      const t=document.querySelector('.tab.active');if(t)renderTab(t.dataset.t);
    } else {
      CR_AD=[];NAVER_KW=[];NSA_KW=[];
      navBoot();
    }
    // 캐시 저장 — 실패(null)한 테이블은 건너뛴다. 빈 값으로 덮으면 다음 새로고침도 0 인 채로 고착된다.
    const ts=Date.now();
    Object.keys(d).forEach(k=>{if(d[k]!=null)cacheSet('t_'+k,{ts,data:d[k]})});
    renderCoreWarn();
  };
  if(renderedFromCache){
    corePromise.then(applyAndRerender); // 백그라운드 갱신
  } else {
    await corePromise.then(applyAndRerender); // 첫 방문은 await
  }
  // 큰 보조 테이블: 모드 진입 시 lazy 로드 (ensureBigTable)
  // 단, CR_AD 는 추이차트의 ▶ 펼침에 자주 쓰이므로 브라우저 idle 시 prefetch
  const idle=window.requestIdleCallback||((cb)=>setTimeout(cb,3000));
  //   prefetch 가 끝나면 국내소재 모드를 보고 있는 경우 다시 그린다 —
  //   안 그러면 IndexedDB 의 옛 CR_AD 로 그려진 화면이 탭을 옮길 때까지 그대로 남는다(백필 직후 특히).
  idle(()=>{ensureBigTable('cr').then(()=>{
    if(MODE!=='cr')return;
    AD=buildAD('cr');rebuildLookups();
    const t=document.querySelector('.tab.active');if(t)renderTab(t.dataset.t);
  });},{timeout:30000});
}
// 큰 테이블은 모드 진입 시 lazy 로드
async function ensureBigTable(name){
  if(window._BIG_LOADED&&window._BIG_LOADED[name])return;
  if(!window._BIG_LOADING)window._BIG_LOADING={};
  if(window._BIG_LOADING[name])return window._BIG_LOADING[name];
  const D180='&date=gte.'+_dateCutoff(215);
  const tasks={
    cr:()=>sbAll('ad_creative_daily','spend',D180).then(d=>{CR_AD=d;window._CR_BY_ADSET=null;cacheSet('t_cr',{ts:Date.now(),data:d});}),
    glcr:()=>sbAll('global_ad_creative_daily','spend_usd',D180).then(d=>{GL_CR=canonGLRows(d);cacheSet('t_glcr',{ts:Date.now(),data:GL_CR});}),
    nsakw:()=>sbAll('naver_sa_keyword_daily','cost_vat',D180).then(d=>{NSA_KW=d||[];cacheSet('t_nsa_kw',{ts:Date.now(),data:NSA_KW});}),
    naverkw:()=>sbQ('naver_keyword_mp_daily','select=*&order=date.desc&limit=10000'+D180).then(d=>{NAVER_KW=d||[];cacheSet('t_naver_kw',{ts:Date.now(),data:NAVER_KW});}),
    ggdgct:()=>Promise.all([
        sbQ('google_demandgen_content_mp_daily','select=*&order=date.desc'+D180).catch(()=>[]),
        sbQ('google_demandgen_content_spend_daily','select=*&order=date.desc'+D180).catch(()=>[]),
      ]).then(([rev,sp])=>{GGDG_CT=rev||[];GGDG_SP=sp||[];cacheSet('t_ggdg_ct',{ts:Date.now(),data:GGDG_CT});cacheSet('t_ggdg_sp',{ts:Date.now(),data:GGDG_SP});}),
    ggdgkr:()=>sbQ('google_demandgen_campaign_daily','select=*&order=date.desc'+D180).then(d=>{GGDG_TIGHT=d||[];cacheSet('t_ggdg_tight',{ts:Date.now(),data:GGDG_TIGHT});}),
    // 매출탭 구글 5분할 — 필요한 컬럼만(행 1만 안팎). 실패 시 매출탭은 구 소스(시트 검색광고+[Tight]DG)로 폴백.
    gcamp:()=>sbQ('google_campaign_daily','select=date,channel_type,country,owner,spend,revenue&order=date.desc&limit=30000'+D180).then(d=>{GCAMP=d||[];cacheSet('t_gcamp',{ts:Date.now(),data:GCAMP});}),
  };
  if(!tasks[name])return;
  window._BIG_LOADING[name]=tasks[name]().then(()=>{
    if(!window._BIG_LOADED)window._BIG_LOADED={};
    window._BIG_LOADED[name]=true;
  }).catch(()=>{
    // 실패는 '로드됨'이 아니다 — 진행중 플래그를 지워 다음 진입에서 재시도되게 한다.
    //   (안 지우면 이미 resolve 된 promise 만 계속 돌려줘서 호출부가 영영 빈 데이터를 본다)
    delete window._BIG_LOADING[name];
  });
  return window._BIG_LOADING[name];
}

// ===== 지표 하이아라키 KPI (kpi_metrics) =====
let KPI_DATA=null,_kpiLoading=null,kpiCharts={},kpiDashPeriod='weekly',kpiSel={weekly:null,monthly:null};
async function ensureKpiData(){
  if(KPI_DATA)return KPI_DATA;
  if(_kpiLoading)return _kpiLoading;
  _kpiLoading=sbQ('kpi_metrics','select=*&order=period_start.desc')
    .then(d=>{KPI_DATA=Array.isArray(d)?d:[];return KPI_DATA})
    .catch(()=>{KPI_DATA=[];return KPI_DATA});
  return _kpiLoading;
}
const _wonK=n=>n==null?'-':'₩'+Math.round(n).toLocaleString('ko-KR');
const _pctK=n=>n==null?'-':(n*100).toFixed(1)+'%';
const _numK=n=>n==null||n===0?'-':Math.round(n).toLocaleString('ko-KR');
function _kpiRows(period){return (KPI_DATA||[]).filter(r=>r.period===period)
  .sort((a,b)=>a.period_start<b.period_start?1:-1);} // newest first
function _kpiDestroy(){Object.values(kpiCharts).forEach(c=>{try{c.destroy()}catch(e){}});kpiCharts={}}

// ===== KPI 상품별 Top10 (실매출, kpi_product_metrics) =====
let KPI_PRODUCT_DATA=null,_kpiProdLoading=null;
function ensureKpiProductData(){
  if(KPI_PRODUCT_DATA)return Promise.resolve(KPI_PRODUCT_DATA);
  if(_kpiProdLoading)return _kpiProdLoading;
  _kpiProdLoading=sbQ('kpi_product_metrics','select=*&order=period_start.desc,rank.asc')
    .then(d=>{KPI_PRODUCT_DATA=Array.isArray(d)?d:[];return KPI_PRODUCT_DATA})
    .catch(()=>{KPI_PRODUCT_DATA=[];return KPI_PRODUCT_DATA});
  return _kpiProdLoading;
}
function renderKpiProducts(sel){
  if(!document.getElementById('kpiProductSection'))return;
  ensureKpiProductData().then(all=>{
    const host=document.getElementById('kpiProductSection');if(!host)return;
    const rows=(all||[]).filter(r=>r.period===sel.period&&r.period_start===sel.period_start).sort((a,b)=>a.rank-b.rank);
    if(!rows.length){host.innerHTML='<div class="chart-card"><h3>🏆 Top10 상품 (실매출)</h3><div style="padding:24px;text-align:center;color:#888">이 기간 상품별 데이터가 없습니다. <code>kpi_product_metrics</code> 적재 후 표시됩니다.</div></div>';return}
    let t='<table class="kpi-table" style="width:100%"><thead><tr><th style="text-align:left">#</th><th style="text-align:left">상품</th><th>실매출</th><th>판매수</th><th>객단가</th></tr></thead><tbody>';
    rows.forEach(r=>{t+='<tr><td style="text-align:left">'+r.rank+'</td><td style="text-align:left;font-weight:600">'+(r.product||'')+'</td><td style="color:#2563eb;font-weight:600">'+_wonK(r.revenue)+'</td><td>'+_numK(r.sales)+'</td><td>'+_wonK(r.aov)+'</td></tr>'});
    t+='</tbody></table>';
    host.innerHTML='<div class="chart-card" style="margin-bottom:16px"><h3>🏆 Top10 상품 (실매출 · '+(sel.period_label||'')+')</h3>'
      +'<div class="chart-wrap" style="height:'+Math.max(220,rows.length*34+40)+'px"><canvas id="kpiChProd"></canvas></div></div>'
      +'<div class="chart-card">'+t+'</div>';
    try{if(kpiCharts.cp)kpiCharts.cp.destroy()}catch(e){}
    kpiCharts.cp=new Chart(document.getElementById('kpiChProd'),{type:'bar',
      data:{labels:rows.map(r=>r.product),datasets:[{label:'실매출',data:rows.map(r=>r.revenue),backgroundColor:'#2563eb'}]},
      options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},
        tooltip:{callbacks:{label:c=>'실매출 '+_wonK(c.parsed.x)}}},
        scales:{x:{ticks:{callback:v=>v>=1e8?(v/1e8).toFixed(1)+'억':(v>=1e4?(v/1e4).toFixed(0)+'만':v)}}}}});
  });
}

function renderKpiDashboard(){
  if(!KPI_DATA){ensureKpiData().then(renderKpiDashboard);return}
  _kpiDestroy();
  const el=document.getElementById('kpiDashBody');
  const rows=_kpiRows(kpiDashPeriod).filter(r=>(r.revenue||0)>0); // meaningful, newest first
  if(!rows.length){el.innerHTML='<div style="padding:48px;text-align:center;color:#888">데이터가 아직 없습니다. <code>kpi_supabase.py</code> 적재 후 표시됩니다.</div>';return}
  const sel=rows.find(r=>r.period_start===kpiSel[kpiDashPeriod])||rows[0]; // 선택한 기간(기본 최신)
  const si=rows.indexOf(sel);
  const latest=sel;                       // 카드/증감은 선택한 기간 기준
  const prev=rows[si+1]||null;            // 직전 기간(증감 비교용)
  const N=kpiDashPeriod==='weekly'?12:6;
  const series=rows.slice(si,si+N).slice().reverse(); // 선택 기간으로 끝나는 추이
  const labels=series.map(r=>r.period_label);
  let h='';
  h+='<div class="dash-filter"><span style="font-weight:700">📈 지표 하이아라키 KPI</span>'
    +'<label style="margin-left:14px">기간</label><select id="kpiDashPeriodSel">'
    +'<option value="weekly"'+(kpiDashPeriod==='weekly'?' selected':'')+'>주간</option>'
    +'<option value="monthly"'+(kpiDashPeriod==='monthly'?' selected':'')+'>월간</option></select>'
    +'<label style="margin-left:10px">'+(kpiDashPeriod==='weekly'?'주차':'월')+'</label>'
    +'<select id="kpiPeriodPick">'+rows.map(r=>'<option value="'+r.period_start+'"'+(r.period_start===sel.period_start?' selected':'')+'>'+r.period_label+'</option>').join('')+'</select>'
    +'<span style="color:#888;margin-left:12px;font-size:11px">기준일: '+sel.period_end+(si===0?' · 최신':'')+'</span></div>';
  const _dlt=(cur,prv)=>{if(prv==null||prv===0||cur==null)return '';const d=(cur-prv)/Math.abs(prv)*100;const up=d>=0;
    return '<div style="font-size:10px;margin-top:2px;color:'+(up?'#16a34a':'#dc2626')+'">'+(up?'▲':'▼')+Math.abs(d).toFixed(1)+'% <span style="color:#aaa">vs 직전</span></div>'};
  h+='<div class="kpi-grid">';
  [{l:'💵 매출',v:_wonK(latest.revenue),c:'#2563eb',cur:latest.revenue,prv:prev&&prev.revenue},
   {l:'💸 예산',v:_wonK(latest.budget),c:'#d00',cur:latest.budget,prv:prev&&prev.budget},
   {l:'💰 순이익',v:_wonK(latest.net_profit),c:(latest.net_profit>=0?'#16a34a':'#dc2626'),cur:latest.net_profit,prv:prev&&prev.net_profit},
   {l:'🎯 ROAS',v:_pctK(latest.roas),c:'#7c3aed',cur:latest.roas,prv:prev&&prev.roas},
   {l:'🛒 결제율',v:_pctK(latest.pay_rate),c:'#0891b2',cur:latest.pay_rate,prv:prev&&prev.pay_rate},
   {l:'🧾 객단가',v:_wonK(latest.aov),c:'#b45309',cur:latest.aov,prv:prev&&prev.aov}
  ].forEach(k=>h+='<div class="kpi-card"><div class="k-label">'+k.l+'</div><div class="k-value" style="color:'+k.c+'">'+k.v+'</div>'+_dlt(k.cur,k.prv)+'</div>');
  h+='</div>';
  h+='<div class="chart-grid">'
    +'<div class="chart-card"><h3>💰 매출·예산·순이익</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh1"></canvas></div></div>'
    +'<div class="chart-card"><h3>🎯 ROAS 추이</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh2"></canvas></div></div>'
    +'<div class="chart-card"><h3>🛒 결제율 추이</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh3"></canvas></div></div>'
    +'<div class="chart-card"><h3>🧾 객단가 추이</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh4"></canvas></div></div>'
    +'<div class="chart-card"><h3>🎯 CPP(구매당 비용) 추이</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh6"></canvas></div></div>'
    +'<div class="chart-card"><h3>👤 CAC(고객획득비용) 추이</h3><div class="chart-wrap" style="height:280px"><canvas id="kpiCh7"></canvas></div></div>'
    +'</div>';
  h+='<div class="chart-card" style="margin-bottom:16px"><h3>👥 판매수·PV 추이</h3><div style="position:relative;height:300px"><canvas id="kpiCh5"></canvas></div></div>';
  h+='<div id="kpiProductSection" style="margin-bottom:16px"><div style="padding:24px;text-align:center;color:#888">상품별 실매출 불러오는 중…</div></div>';
  el.innerHTML=h;
  document.getElementById('kpiDashPeriodSel').addEventListener('change',e=>{kpiDashPeriod=e.target.value;kpiSel[kpiDashPeriod]=null;renderKpiDashboard()});
  document.getElementById('kpiPeriodPick').addEventListener('change',e=>{kpiSel[kpiDashPeriod]=e.target.value;renderKpiDashboard()});
  // 축 눈금: 인접 눈금이 같은 숫자로 뭉치지 않게 눈금 간격(step) 기준으로 소수 자리수 자동 결정
  const _tv=t=>(t&&typeof t==='object')?t.value:t;
  const _axStep=ticks=>{if(!ticks||ticks.length<2)return 0;let m=Infinity;for(let i=1;i<ticks.length;i++){const d=Math.abs(_tv(ticks[i])-_tv(ticks[i-1]));if(d>0&&d<m)m=d}return m===Infinity?0:m};
  const _decals=step=>(!step||!isFinite(step)||step<=0)?1:Math.max(0,Math.min(6,Math.ceil(-Math.log10(step))));
  const wonAxis=(v,i,ticks)=>{const st=_axStep(ticks),a=Math.abs(v);
    if(a>=1e8)return (v/1e8).toFixed(Math.max(1,_decals(st/1e8)))+'억';
    if(a>=1e4)return (v/1e4).toFixed(Math.max(1,_decals(st/1e4)))+'만';
    const d=_decals(st);return d>0?v.toFixed(d):''+Math.round(v)};
  const mkLine=(id,label,data,color,pct)=>new Chart(document.getElementById(id),{type:'line',
    data:{labels,datasets:[{label,data,borderColor:color,backgroundColor:color+'22',tension:0.3,fill:true,pointRadius:3}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},
      tooltip:{callbacks:{label:c=>pct?(c.parsed.y*100).toFixed(2)+'%':_wonK(c.parsed.y)}}},
      scales:{y:{ticks:{callback:(v,i,ticks)=>pct?(v*100).toFixed(Math.max(1,_decals(_axStep(ticks)*100)))+'%':wonAxis(v,i,ticks)}}}}});
  kpiCharts.c1=new Chart(document.getElementById('kpiCh1'),{data:{labels,datasets:[
    {type:'bar',label:'매출',data:series.map(r=>r.revenue),backgroundColor:'#2563eb'},
    {type:'bar',label:'예산',data:series.map(r=>r.budget),backgroundColor:'#f59e0b'},
    {type:'line',label:'순이익',data:series.map(r=>r.net_profit),borderColor:'#16a34a',backgroundColor:'#16a34a22',tension:0.3,pointRadius:3}
  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom'},
    tooltip:{callbacks:{label:c=>c.dataset.label+': '+_wonK(c.parsed.y)}}},
    scales:{y:{ticks:{callback:wonAxis}}}}});
  kpiCharts.c2=mkLine('kpiCh2','ROAS',series.map(r=>r.roas),'#7c3aed',true);
  kpiCharts.c3=mkLine('kpiCh3','결제율',series.map(r=>r.pay_rate),'#0891b2',true);
  kpiCharts.c4=mkLine('kpiCh4','객단가',series.map(r=>r.aov),'#b45309',false);
  kpiCharts.c6=mkLine('kpiCh6','CPP',series.map(r=>r.cpp),'#0d9488',false);
  kpiCharts.c7=mkLine('kpiCh7','CAC',series.map(r=>r.cac),'#db2777',false);
  kpiCharts.c5=new Chart(document.getElementById('kpiCh5'),{data:{labels,datasets:[
    {type:'bar',label:'판매수',data:series.map(r=>r.sales),backgroundColor:'#6366f1',yAxisID:'y'},
    {type:'line',label:'PV',data:series.map(r=>r.pv),borderColor:'#ec4899',backgroundColor:'#ec489922',tension:0.3,pointRadius:3,yAxisID:'y1'}
  ]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom'}},
    scales:{y:{position:'left',title:{display:true,text:'판매수'}},
            y1:{position:'right',title:{display:true,text:'PV'},grid:{drawOnChartArea:false}}}}});
  renderKpiProducts(sel);
}

function renderKpiTable(period){
  if(!KPI_DATA){ensureKpiData().then(()=>renderKpiTable(period));return}
  const tbl=document.getElementById(period==='weekly'?'kpiWTbl':'kpiMTbl');
  const rows=_kpiRows(period);
  let h='<thead><tr><th style="text-align:left">기간</th><th>예산</th><th>매출</th><th>PV</th><th>판매수</th>'
    +'<th>순이익</th><th>객단가</th><th>ROAS</th><th>CPP</th><th>결제율</th><th>CAC</th></tr></thead><tbody>';
  rows.forEach(r=>{
    h+='<tr><td style="text-align:left;font-weight:600">'+r.period_label+'</td>'
      +'<td>'+_wonK(r.budget)+'</td><td>'+_wonK(r.revenue)+'</td>'
      +'<td>'+_numK(r.pv)+'</td><td>'+_numK(r.sales)+'</td>'
      +'<td style="color:'+((r.net_profit||0)>=0?'#16a34a':'#dc2626')+'">'+_wonK(r.net_profit)+'</td>'
      +'<td>'+_wonK(r.aov)+'</td><td style="font-weight:600">'+_pctK(r.roas)+'</td>'
      +'<td>'+_wonK(r.cpp)+'</td><td>'+_pctK(r.pay_rate)+'</td><td>'+_wonK(r.cac)+'</td></tr>';
  });
  h+='</tbody>';
  tbl.innerHTML=h;
}

// ===== 마케터 모드 (CR_AD 소재 레벨, 이름 substring 필터) — tightauto-scraper report 구성 복제(라이트 테마) =====
const KR_MARKETERS=['수연','희상','혜린','본걸','지은','휘동','지연','정헌','연희','지영','하루','훤기','베스'];
const GL_MARKETERS=['본걸','지은','훤기','채채','지영','하루'];
const _mkToday=()=>{const d=new Date();return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0')};
const _mkAddDays=n=>{const d=new Date();d.setDate(d.getDate()+n);return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0')};
let mktState={region:'kr',name:'수연',from:_mkAddDays(-13),to:_mkToday(),preset:'14d',sortKey:'roas',sortDir:-1};
let mktCharts={};
const _mkNorm=s=>(s==null?'':String(s)).normalize('NFC').toLowerCase().replace(/\s+/g,' ').trim();
// 귀속 규칙: ①ad_name(제작자 태그) 1순위. ②ad_name 에 '아무 제작자도 없을 때만' adset_name(세트 주인) 2순위 폴백.
//   → 제작자 A 소재가 B 세트에 있어도 B로 안 넘어가고, 무명 소재(랜딩 등)만 세트 주인에게 귀속.
//   (캠페인명·메모는 제작자 출처가 아니라 매칭 제외)
const _MK_ALL=[...new Set([...KR_MARKETERS,...GL_MARKETERS])].map(_mkNorm);
function _adHasAnyMarketer(an){for(const n of _MK_ALL){if(an.includes(n))return true}return false}
function _mkMatch(r,needle){
  const an=_mkNorm(r.ad_name);
  if(an&&an.includes(needle))return true;            // 1순위: 소재 제작자
  if(!_adHasAnyMarketer(an)){                          // ad_name 에 제작자 태그가 전혀 없을 때만
    const sn=_mkNorm(r.adset_name);
    if(sn&&sn.includes(needle))return true;           // 2순위: 세트 주인
  }
  return false;
}
function _mkCreated(adName){if(!adName)return null;const m=String(adName).match(/_(\d{6})(?:_|$|x|\b)/);if(!m)return null;const s=m[1];const mm=+s.slice(2,4),dd=+s.slice(4,6);if(mm<1||mm>12||dd<1||dd>31)return null;return '20'+s.slice(0,2)+'-'+s.slice(2,4)+'-'+s.slice(4,6)}
function _mkAggregate(rows){const byId={};
  rows.forEach(r=>{const key=r.ad_id||(r.ad_name+'__'+r.adset_id);
    if(!byId[key])byId[key]={ad_id:r.ad_id,ad_name:r.ad_name||'(이름없음)',adset_name:r.adset_name,campaign_name:r.campaign_name,product:r.product,created:_mkCreated(r.ad_name),imp:0,clk:0,spend:0,rev:0,profit:0,res:0,days:new Set()};
    const t=byId[key];const sp=+(r.spend!=null?r.spend:r.spend_usd)||0,rv=+(r.revenue!=null?r.revenue:r.revenue_usd)||0,pf=(r.profit!=null?+r.profit:(r.profit_usd!=null?+r.profit_usd:(rv-sp)));t.imp+=(+r.impressions||0);t.clk+=(+r.unique_clicks||0);t.spend+=sp;t.rev+=rv;t.profit+=pf;t.res+=(+r.results_mp||0);t.days.add(r.date)});
  return Object.values(byId).map(t=>({...t,ndays:t.days.size,ctr:t.imp>0?t.clk/t.imp*100:0,cpm:t.imp>0?t.spend/t.imp*1000:0,roas:t.spend>0?t.rev/t.spend*100:0,cvr:t.clk>0?t.res/t.clk*100:0}));}
function _mkTotals(list){const t={spend:0,rev:0,profit:0,imp:0,clk:0,res:0};list.forEach(c=>{t.spend+=c.spend;t.rev+=c.rev;t.profit+=c.profit;t.imp+=c.imp;t.clk+=c.clk;t.res+=c.res});t.ctr=t.imp>0?t.clk/t.imp*100:0;t.cpm=t.imp>0?t.spend/t.imp*1000:0;t.roas=t.spend>0?t.rev/t.spend*100:0;t.cvr=t.clk>0?t.res/t.clk*100:0;return t}
function _mkDestroy(){Object.values(mktCharts).forEach(c=>{try{c.destroy()}catch(e){}});mktCharts={}}
function renderMarketer(){
  const body=document.getElementById('mktBody');if(!body)return;
  const region=mktState.region;
  const names=region==='gl'?GL_MARKETERS:KR_MARKETERS;
  if(!names.includes(mktState.name))mktState.name=names[0];
  const src=region==='gl'?GL_CR:CR_AD;
  if(!src.length){
    // 재시도 가드: 로드가 실패/빈결과로 끝나면 renderMarketer→ensureBigTable→renderMarketer 가
    //   무한 재귀(마이크로태스크 루프)로 돌아 탭이 멈춘다 → 2회까지만 시도하고 안내를 띄운다.
    const _k='_mkRetry_'+region;window[_k]=(window[_k]||0)+1;
    if(window[_k]>2){body.innerHTML='<div style="padding:48px;text-align:center;color:#c00">소재 데이터를 불러오지 못했습니다. 새로고침(F5) 후 다시 시도해 주세요.</div>';return}
    body.innerHTML='<div style="padding:48px;text-align:center;color:#888">소재 데이터 로딩 중…</div>';
    ensureBigTable(region==='gl'?'glcr':'cr').then(renderMarketer);return}
  window['_mkRetry_'+region]=0;
  // 캐시로 즉시 그린 뒤(위 src.length>0), 이번 세션에서 아직 fresh 를 안 받았으면 백그라운드 갱신 1회.
  if(region==='gl'&&!(window._BIG_LOADED&&window._BIG_LOADED.glcr)&&!window._glcrRefreshing){
    window._glcrRefreshing=1;ensureBigTable('glcr').then(()=>{window._glcrRefreshing=0;if(mktState.region==='gl')renderMarketer()});
  }
  // 통화: 국내=₩, 글로벌=$ (money/axMoney 지역화 — 함수 내 money 섀도잉)
  const money=v=>region==='gl'?('$'+Math.round(v||0).toLocaleString('en-US')):('₩'+Math.round(v||0).toLocaleString('ko-KR'));
  const axMoney=v=>region==='gl'?('$'+(v/1000).toFixed(1)+'k'):('₩'+(v/10000).toFixed(0)+'만');
  const needle=_mkNorm(mktState.name);
  const mineAll=src.filter(r=>_mkMatch(r,needle));
  const rows=mineAll.filter(r=>r.date>=mktState.from&&r.date<=mktState.to);
  let list=_mkAggregate(rows);const T=_mkTotals(list);
  const ALL=_mkTotals(_mkAggregate(mineAll));
  const sk=mktState.sortKey,sd=mktState.sortDir;
  const sv=c=>sk==='created'?(c.created||''):sk==='ndays'?c.ndays:sk==='ad_name'?(c.ad_name||''):(c[sk]||0);
  list.sort((a,b)=>{const x=sv(a),y=sv(b);return x<y?sd:x>y?-sd:0});
  // ★ 글로벌(GL_CR=global_ad_creative_daily)은 컬럼이 spend_usd/revenue_usd/profit_usd 다.
  //   여기서 r.spend/r.revenue 만 읽던 탓에 '📈 일별 추이' 3개 선이 전부 0 으로 그려졌다(KPI 카드는 _mkAggregate 라 정상).
  const _sp=r=>+(r.spend!=null?r.spend:r.spend_usd)||0;
  const _rv=r=>+(r.revenue!=null?r.revenue:r.revenue_usd)||0;
  const _pf=(r,sp,rv)=>r.profit!=null?+r.profit:(r.profit_usd!=null?+r.profit_usd:(rv-sp));
  const byDate={};rows.forEach(r=>{if(!byDate[r.date])byDate[r.date]={s:0,r:0,p:0};const sp=_sp(r),rv=_rv(r);byDate[r.date].s+=sp;byDate[r.date].r+=rv;byDate[r.date].p+=_pf(r,sp,rv)});
  const dts=Object.keys(byDate).sort();
  const pct=(v,d)=>(v||0).toFixed(d==null?2:d)+'%';
  let h='';
  // 컨트롤 (마케터 + 프리셋 + 날짜범위)
  h+='<div class="dash-filter" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px">';
  h+='<span style="font-weight:700;font-size:14px">👤 마케터</span>';
  h+='<span style="display:inline-flex;gap:4px;margin-left:4px">'+[['kr','🇰🇷 국내'],['gl','🌏 글로벌']].map(rg=>'<button class="mkt-region" data-r="'+rg[0]+'" style="padding:4px 9px;border:1px solid '+(region===rg[0]?'#1a73e8':'#ccc')+';border-radius:5px;background:'+(region===rg[0]?'#1a73e8':'#fff')+';color:'+(region===rg[0]?'#fff':'#333')+';font-size:11px;cursor:pointer;font-family:inherit">'+rg[1]+'</button>').join('')+'</span>';
  h+='<select id="mktName">'+names.map(n=>'<option value="'+n+'"'+(n===mktState.name?' selected':'')+'>'+n+'</option>').join('')+'</select>';
  h+='<span style="margin-left:6px;display:inline-flex;gap:4px">'+[['all','전체'],['today','오늘'],['3d','3일'],['7d','7일'],['14d','2주'],['30d','30일']].map(p=>'<button class="mkt-preset" data-p="'+p[0]+'" style="padding:4px 9px;border:1px solid '+(mktState.preset===p[0]?'#1a73e8':'#ccc')+';border-radius:5px;background:'+(mktState.preset===p[0]?'#1a73e8':'#fff')+';color:'+(mktState.preset===p[0]?'#fff':'#333')+';font-size:11px;cursor:pointer;font-family:inherit">'+p[1]+'</button>').join('')+'</span>';
  h+='<input type="date" id="mktFrom" value="'+mktState.from+'" style="font-size:11px"><span style="color:#888">~</span><input type="date" id="mktTo" value="'+mktState.to+'" style="font-size:11px"><button id="mktGo" style="padding:4px 12px;border:1px solid #1a73e8;border-radius:5px;background:#1a73e8;color:#fff;font-size:11px;cursor:pointer;font-family:inherit">조회</button>';
  h+='<span style="color:#888;font-size:11px;margin-left:auto">소재 '+list.length+'개 · '+mktState.from+'~'+mktState.to+(region==='gl'?' · USD':' · KRW')+'</span>';
  h+='</div>';
  // KPI 카드 (원본 8개)
  const card=(l,v,c)=>'<div class="kpi-card"><div class="k-label">'+l+'</div><div class="k-value"'+(c?' style="color:'+c+'"':'')+'>'+v+'</div></div>';
  h+='<div class="kpi-grid">';
  h+=card('총 지출',money(T.spend),'#d00')+card('총 매출',money(T.rev),'#2563eb')+card('순이익',money(T.profit),T.profit>=0?'#16a34a':'#dc2626')+card('평균 ROAS',pct(T.roas,1),T.roas>=100?'#16a34a':'#dc2626')+card('평균 CTR',pct(T.ctr),'#7c3aed')+card('평균 CPM',money(T.cpm),'#b45309')+card('평균 CVR',pct(T.cvr),'#0891b2')+card('총 노출',F(T.imp),'#555');
  h+='</div>';
  // 비교 섹션 (선택 기간 vs 전체 누적)
  const ratio=(a,b)=>b>0?a/b*100:0;
  const cmp=[['지출',money(T.spend),money(ALL.spend),pct(ratio(T.spend,ALL.spend),1)],['매출',money(T.rev),money(ALL.rev),pct(ratio(T.rev,ALL.rev),1)],['순이익',money(T.profit),money(ALL.profit),ALL.profit!==0?pct(ratio(T.profit,ALL.profit),1):'–'],['ROAS',pct(T.roas,1),pct(ALL.roas,1),ALL.roas>0?pct(T.roas/ALL.roas*100,1):'–'],['CTR',pct(T.ctr),pct(ALL.ctr),ALL.ctr>0?pct(T.ctr/ALL.ctr*100,1):'–'],['CPM',money(T.cpm),money(ALL.cpm),ALL.cpm>0?pct(T.cpm/ALL.cpm*100,1):'–'],['CVR',pct(T.cvr),pct(ALL.cvr),ALL.cvr>0?pct(T.cvr/ALL.cvr*100,1):'–'],['노출',F(T.imp),F(ALL.imp),pct(ratio(T.imp,ALL.imp),1)],['클릭',F(T.clk),F(ALL.clk),pct(ratio(T.clk,ALL.clk),1)]];
  h+='<div class="chart-card" style="margin-bottom:12px"><h3>📊 선택 기간 vs 전체 누적 <span style="font-weight:400;color:#888;font-size:10px">(전체=로드된 최근 ~7개월)</span></h3>';
  h+='<table style="width:100%;border-collapse:collapse;font-size:12px"><thead><tr style="color:#888"><th style="text-align:left;padding:5px 8px">지표</th><th style="text-align:right;padding:5px 8px">선택 기간</th><th style="text-align:right;padding:5px 8px">전체 누적</th><th style="text-align:right;padding:5px 8px">선택÷전체</th></tr></thead><tbody>';
  h+=cmp.map(r=>'<tr style="border-top:1px solid #eee"><td style="text-align:left;color:#888;padding:4px 8px">'+r[0]+'</td><td style="text-align:right;padding:4px 8px;font-weight:600">'+r[1]+'</td><td style="text-align:right;padding:4px 8px;color:#888">'+r[2]+'</td><td style="text-align:right;padding:4px 8px;color:#1a73e8">'+r[3]+'</td></tr>').join('');
  h+='</tbody></table></div>';
  // 차트 4종
  h+='<div class="chart-card" style="margin-bottom:12px"><h3>📈 일별 추이 (지출 / 매출 / 순이익)</h3><div class="chart-wrap" style="height:260px"><canvas id="mkTrend"></canvas></div></div>';
  h+='<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">';
  h+='<div class="chart-card"><h3>🎯 가성비 산점도 (지출 × ROAS, 원크기=노출)</h3><div class="chart-wrap" style="height:260px"><canvas id="mkScatter"></canvas></div></div>';
  h+='<div class="chart-card"><h3>🥧 상품별 매출 비중</h3><div class="chart-wrap" style="height:260px"><canvas id="mkProduct"></canvas></div></div>';
  h+='</div>';
  h+='<div class="chart-card" style="margin-bottom:14px"><h3>🏆 Top 10 ROAS</h3><div class="chart-wrap" style="height:300px"><canvas id="mkTop"></canvas></div></div>';
  // 표 (클릭 정렬)
  const cols=[['ad_name','소재','left'],['created','제작일','center'],['ctr','CTR','right'],['roas','ROAS','right'],['cpm','CPM','right'],['cvr','CVR','right'],['imp','노출','right'],['spend','지출','right'],['rev','매출','right'],['profit','순이익','right'],['ndays','기간','center']];
  h+='<div class="sheet-wrap"><table id="mktTbl"><thead><tr><th style="width:28px">#</th>'+cols.map(c=>'<th data-key="'+c[0]+'" style="cursor:pointer;text-align:'+c[2]+(mktState.sortKey===c[0]?';color:#1a73e8':'')+'">'+c[1]+(mktState.sortKey===c[0]?(mktState.sortDir<0?' ▼':' ▲'):'')+'</th>').join('')+'</tr></thead><tbody>';
  if(!list.length){h+='<tr><td colspan="12" style="text-align:center;color:#888;padding:30px">"'+mktState.name+'" 이름이 들어간 소재가 이 기간에 없습니다.</td></tr>'}
  list.forEach((c,i)=>{const meta=[c.product,c.adset_name].filter(Boolean).join(' · ');
    h+='<tr><td style="color:#aaa;text-align:center">'+(i+1)+'</td>'
      +'<td style="text-align:left"><div style="font-weight:600">'+(c.ad_name||'')+'</div><div style="color:#999;font-size:10px">'+meta+'</div></td>'
      +'<td style="text-align:center;color:#888;font-size:11px">'+(c.created||'–')+'</td>'
      +'<td style="text-align:right">'+c.ctr.toFixed(2)+'%</td>'
      +'<td class="'+RC(c.roas)+'" style="text-align:right;font-weight:600">'+c.roas.toFixed(0)+'%</td>'
      +'<td style="text-align:right">'+money(c.cpm)+'</td>'
      +'<td style="text-align:right">'+c.cvr.toFixed(2)+'%</td>'
      +'<td style="text-align:right">'+F(c.imp)+'</td>'
      +'<td style="text-align:right;color:#d00">'+money(c.spend)+'</td>'
      +'<td style="text-align:right;color:#2563eb">'+money(c.rev)+'</td>'
      +'<td style="text-align:right;color:'+(c.profit>=0?'green':'red')+'">'+money(c.profit)+'</td>'
      +'<td style="text-align:center">'+c.ndays+'</td></tr>';});
  h+='</tbody></table></div>';
  body.innerHTML=h;
  // 리스너
  document.getElementById('mktName').addEventListener('change',e=>{mktState.name=e.target.value;renderMarketer()});
  document.querySelectorAll('.mkt-region').forEach(b=>b.addEventListener('click',()=>{mktState.region=b.dataset.r;const nl=mktState.region==='gl'?GL_MARKETERS:KR_MARKETERS;if(!nl.includes(mktState.name))mktState.name=nl[0];renderMarketer()}));
  document.querySelectorAll('.mkt-preset').forEach(b=>b.addEventListener('click',()=>{const p=b.dataset.p;mktState.preset=p;const td=_mkToday();
    if(p==='all'){mktState.from='2024-01-01';mktState.to=td}else if(p==='today'){mktState.from=td;mktState.to=td}else{const n={'3d':3,'7d':7,'14d':14,'30d':30}[p];mktState.from=_mkAddDays(-(n-1));mktState.to=td}renderMarketer()}));
  document.getElementById('mktGo').addEventListener('click',()=>{mktState.from=document.getElementById('mktFrom').value;mktState.to=document.getElementById('mktTo').value;mktState.preset='';renderMarketer()});
  document.querySelectorAll('#mktTbl th[data-key]').forEach(th=>th.addEventListener('click',()=>{const k=th.dataset.key;if(mktState.sortKey===k)mktState.sortDir*=-1;else{mktState.sortKey=k;mktState.sortDir=-1}renderMarketer()}));
  // 차트 렌더
  _mkDestroy();if(typeof Chart==='undefined')return;
  if(dts.length){mktCharts.trend=new Chart(document.getElementById('mkTrend'),{type:'line',data:{labels:dts.map(d=>DK(d).slice(3)),datasets:[
    {label:'지출',data:dts.map(d=>Math.round(byDate[d].s)),borderColor:'#999',backgroundColor:'transparent',tension:0.3,borderWidth:1.5,pointRadius:2},
    {label:'매출',data:dts.map(d=>Math.round(byDate[d].r)),borderColor:'#2563eb',backgroundColor:'transparent',tension:0.3,borderWidth:1.5,pointRadius:2},
    {label:'순이익',data:dts.map(d=>Math.round(byDate[d].p)),borderColor:'#16a34a',backgroundColor:'#16a34a22',tension:0.2,borderWidth:2.5,pointRadius:3,fill:{target:{value:0},above:'#16a34a22',below:'#dc262633'}}
  ]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{font:{size:11}}},tooltip:{callbacks:{label:c=>c.dataset.label+': '+money(c.parsed.y||0)}}},scales:{x:{ticks:{font:{size:9}}},y:{ticks:{font:{size:9},callback:v=>axMoney(v)}}}}});}
  const pts=list.filter(c=>c.spend>0).map(c=>({x:Math.round(c.spend),y:Math.round(c.roas),r:Math.max(3,Math.min(20,Math.sqrt(c.imp/100))),label:c.ad_name,profit:c.profit}));
  if(pts.length){mktCharts.scatter=new Chart(document.getElementById('mkScatter'),{type:'bubble',data:{datasets:[{label:'소재',data:pts,backgroundColor:pts.map(p=>p.profit>=0?'#16a34a88':'#dc262688'),borderColor:pts.map(p=>p.profit>=0?'#16a34a':'#dc2626'),borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>[String(c.raw.label||'').slice(0,40),'지출 '+money(c.raw.x),'ROAS '+c.raw.y+'%']}}},scales:{x:{title:{display:true,text:'지출',font:{size:10}},ticks:{font:{size:9},callback:v=>axMoney(v)}},y:{title:{display:true,text:'ROAS(%)',font:{size:10}},ticks:{font:{size:9}},beginAtZero:true}}}});}
  const byProd={};list.forEach(c=>{const p=c.product||'미분류';byProd[p]=(byProd[p]||0)+c.rev});
  const ps=Object.entries(byProd).sort((a,b)=>b[1]-a[1]);
  if(ps.length){mktCharts.product=new Chart(document.getElementById('mkProduct'),{type:'doughnut',data:{labels:ps.map(p=>p[0]),datasets:[{data:ps.map(p=>Math.round(p[1])),backgroundColor:BRAND_COLORS,borderColor:'#fff',borderWidth:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{font:{size:10},boxWidth:12}},tooltip:{callbacks:{label:c=>c.label+': '+money(c.parsed)}}}}});}
  const top=[...list].sort((a,b)=>(b.roas||0)-(a.roas||0)).slice(0,10);
  if(top.length){mktCharts.top=new Chart(document.getElementById('mkTop'),{type:'bar',data:{labels:top.map(c=>(c.ad_name||'').slice(0,28)),datasets:[{label:'ROAS(%)',data:top.map(c=>Math.round(c.roas)),backgroundColor:top.map(c=>c.roas>=100?'#16a34aaa':'#dc2626aa'),borderColor:top.map(c=>c.roas>=100?'#16a34a':'#dc2626'),borderWidth:1}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>'ROAS '+c.parsed.x+'%'}}},scales:{x:{ticks:{font:{size:9},callback:v=>v+'%'},beginAtZero:true},y:{ticks:{font:{size:9}}}}}});}
}

// ===== TAB ROUTING =====
function renderTab(id){
  if(id==='dashboard')renderDashboard();
  if(id==='trend')renderTrendMain();
  if(id==='auxmetric')renderAux();
  if(id==='vntwtrend')renderVnTwTrend();
  if(id==='trendp')renderTrendProduct();
  if(id==='change')renderChange();
  if(id==='weekly')renderWeekly();
  if(id==='budget')renderBudget();
  if(id==='datetab')renderDateTab();
  if(id==='dateproduct')renderDateProduct();
  if(id==='stripe')renderStripe();
  if(id==='grev')renderGlobalRevenue();
  if(id==='gweek')renderGlobalWeekly();
  if(id==='crank')renderCreativeRanking();
  if(id==='krank')renderAdsetRanking();
  if(id==='dupvar')renderDupVar();
  if(id==='chrev'){renderChannelRevenue();renderChannelDonut();renderChannelBars();}
  if(id==='nsadaily')renderNsaDaily();
  if(id==='nsaweekly')renderNsaWeekly();
  if(id==='ggdgct')renderGgdgContent();
  if(id==='ggdgkr')renderGgdgTight();
  if(id==='tiktok'){renderTiktok();loadTiktok().then(renderTiktok)}  // 스냅샷 먼저 그리고 시트 읽으면 갱신
  if(id==='kpiDash')renderKpiDashboard();
  if(id==='kpiWeekly')renderKpiTable('weekly');
  if(id==='kpiMonthly')renderKpiTable('monthly');
  if(id==='mktDash')renderMarketer();
  if(id==='exp')renderExperiment();
  if(id==='expstat'){_expEnsureSrc(()=>renderExpStatus());renderExpStatus()}
  if(id==='allmedia')renderAllMedia();
  if(id==='compet'){renderCompet();loadCompet().then(renderCompet)}  // 시트 도착하면 다시 그림
}
function renderAllMedia(){
  const f=document.getElementById('allmediaFrame');
  if(f&&!f.src&&f.dataset.src)f.src=f.dataset.src; // 첫 클릭 시에만 로드
}

document.querySelectorAll('.tab').forEach(t=>{t.addEventListener('click',()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(x=>x.classList.remove('active'));
  t.classList.add('active');document.getElementById('p-'+t.dataset.t).classList.add('active');
  renderTab(t.dataset.t);
  navPush();   // 탭 이동을 히스토리에 기록 — 뒤로가기로 이전 탭 복귀
})});
document.getElementById('tGran').addEventListener('change',renderTrendMain);
document.getElementById('tDays').addEventListener('change',renderTrendMain);
document.getElementById('tFilter').addEventListener('input',renderTrendMain);
document.getElementById('trHideDead').addEventListener('change',renderTrendMain);
document.getElementById('axDays').addEventListener('change',renderAux);
document.getElementById('axFilter').addEventListener('input',renderAux);
document.getElementById('tDaysTw').addEventListener('change',renderVnTwTrend);
document.getElementById('tFilterTw').addEventListener('input',renderVnTwTrend);
document.getElementById('tpDays').addEventListener('change',renderTrendProduct);
document.getElementById('tpUnit').addEventListener('change',renderTrendProduct);
document.getElementById('tpFilter').addEventListener('input',renderTrendProduct);
document.getElementById('cFilter').addEventListener('input',renderChange);
document.getElementById('wMode').addEventListener('change',renderWeekly);
document.getElementById('wSource').addEventListener('change',function(){if(MODE==='kr')_wSrcKr=this.value;else if(MODE==='gl')_wSrcGl=this.value;renderWeekly()});
document.getElementById('dtStart').addEventListener('change',renderDateTab);
document.getElementById('dtEnd').addEventListener('change',renderDateTab);
document.getElementById('dtFilter').addEventListener('input',renderDateTab);
document.getElementById('dpSel').addEventListener('change',renderDateProduct);
document.getElementById('dpPeriod').addEventListener('change',renderDateProduct);
document.getElementById('crDays').addEventListener('change',renderCreativeRanking);
document.getElementById('crSort').addEventListener('change',renderCreativeRanking);
document.getElementById('krDays').addEventListener('change',renderAdsetRanking);
document.getElementById('krSort').addEventListener('change',renderAdsetRanking);
document.getElementById('chrDays').addEventListener('change',renderChannelRevenue);
document.getElementById('chrScope').addEventListener('change',renderChannelRevenue);
document.getElementById('chrVanced').addEventListener('change',renderChannelRevenue);
document.getElementById('chrVanced').addEventListener('change',renderChannelDonut);
document.getElementById('chrChartMode').addEventListener('change',renderChannelRevenue);
document.getElementById('chrChartStyle').addEventListener('change',renderChannelRevenue);
document.getElementById('chrDonutDays').addEventListener('change',renderChannelDonut);
document.getElementById('chrScope').addEventListener('change',renderChannelDonut);
document.getElementById('chrBarDays').addEventListener('change',function(){
  // 드롭다운으로 '최근 N일'을 고르면 캘린더 선택은 해제(둘이 동시에 살아있으면 어느 쪽인지 헷갈림).
  // '📅 직접 선택'을 고르면 캘린더 입력을 그대로 두고(비어 있으면 최근 30일 기준으로 미리 채운다) 그 구간을 쓴다.
  const f=document.getElementById('chrBarFrom'),t=document.getElementById('chrBarTo');
  if(this.value==='custom'){
    if(f&&t&&!(f.value&&t.value)){const e=new Date(),s=new Date();s.setDate(e.getDate()-29);f.value=_chrDstr(s);t.value=_chrDstr(e)}
    if(f)f.focus();
  }else if(f&&t){f.value='';t.value=''}
  renderChannelBars();
});
document.getElementById('chrBarFrom').addEventListener('change',renderChannelBars);
document.getElementById('chrBarTo').addEventListener('change',renderChannelBars);
document.getElementById('chrScope').addEventListener('change',renderChannelBars);
document.getElementById('chrVanced').addEventListener('change',renderChannelBars);
document.getElementById('chrView').addEventListener('change',function(){
  const view=this.value;
  const sel=document.getElementById('chrDays');
  let opts,def,unit;
  if(view==='daily'){opts=[14,30,60,90,180,210];def=30;unit='일'}
  else if(view==='weekly'){opts=[4,8,12,24];def=12;unit='주'}
  else if(view==='hourly'){opts=[2,3,5,7];def=3;unit='일'}   // 4시간 버킷: 일수 × 6 = 열
  else{opts=[3,6,12,24];def=12;unit='개월'}
  sel.innerHTML=opts.map(n=>'<option value="'+n+'"'+(n===def?' selected':'')+'>'+n+unit+'</option>').join('');
  renderChannelRevenue();
});
document.getElementById('nsaDays').addEventListener('change',renderNsaDaily);
document.getElementById('nsaSort').addEventListener('change',renderNsaDaily);
document.getElementById('nsaMinSpend').addEventListener('change',renderNsaDaily);
document.getElementById('nsaKwTop').addEventListener('change',renderNsaDaily);
document.getElementById('nsaWeeks').addEventListener('change',renderNsaWeekly);
document.getElementById('nsawMinSpend').addEventListener('change',renderNsaWeekly);
document.getElementById('ggdgDays').addEventListener('change',renderGgdgContent);
document.getElementById('ggdgkrDays').addEventListener('change',renderGgdgTight);
document.getElementById('ttDays').addEventListener('change',renderTiktok);
document.getElementById('ttFilter').addEventListener('input',renderTiktok);
document.getElementById('ggdgTop').addEventListener('change',renderGgdgContent);
document.getElementById('ggdgMinRev').addEventListener('change',renderGgdgContent);

// ===== HIGHLIGHT =====
// HIGHLIGHTS: 추이차트용 (adset_highlights 테이블, 전체삭제 가능)
// r.highlight: 날짜탭용 (ad_performance_daily.highlight 컬럼, 영구저장)
let currentHlId=null;
let currentHlGgdg=false;   // 색상 피커가 구글 디멘드젠 탭에서 열렸는지(저장 경로 분기)
const hlTbl=()=>{const m={kr:'adset_highlights',gl:'global_adset_highlights',cr:'ad_creative_highlights',vn:'vanced_adset_highlights'};return m[MODE]||'adset_highlights'};
const hlIdCol=()=>MODE==='cr'?'ad_id':'adset_id';
const perfTbl=()=>{const m={kr:'ad_performance_daily',gl:'global_ad_performance_daily',cr:'ad_creative_daily',vn:'vanced_ad_performance_daily'};return m[MODE]||'ad_performance_daily'};
// 하이라이트 배경색 + AI 출처 점선 테두리(.hl-ai). source='ai'(오늘의퍼포먼스봇 자동 마킹)면 점선 테두리로 사람 마킹과 구분.
//   0시 지나면 하이라이트·테두리 함께 삭제(HL_SRC도 clear/오늘필터 대상), 다음날 봇 새 조언이 재생성.
//   (Meta 실제 예산변경 테두리 budBc는 box-shadow로 별개 표시 — outline과 공존)
function hlClass(id){const ai=HL_SRC[id]==='ai'?'hl-ai':'';const h=HIGHLIGHTS[id];if(!h||!HL_CONFIG[h])return ai;/*하이라이트 취소돼도 AI면 테두리만 유지*/return HL_CONFIG[h].cls+(ai?' '+ai:'')}
// 현재 MODE의 원본 배열 반환 (norm된 AD 가 아닌 raw)
function _srcAD(){
  return MODE==='kr'?KR_AD:(MODE==='gl'?GL_AD:(MODE==='cr'?CR_AD:VN_AD));
}
// AD + 원본배열 동시 갱신 (GL은 norm이 spread 복사해서 분리되므로 둘 다 업데이트 필요)
function _syncRowField(id, date, field, value){
  AD.forEach(r=>{if(rowId(r)===id&&r.date===date)r[field]=value});
  _srcAD().forEach(r=>{if(rowId(r)===id&&r.date===date)r[field]=value});
}

async function saveHL(id,c){
  // 1) 추이차트용 저장 (adset_highlights)
  // source는 payload에 넣지 않는다 → AI가 찍은 source='ai'는 upsert에서 보존됨(omit 컬럼은 미변경).
  //   ⇒ 사람이 하이라이트를 바꾸거나 취소(✕)해도 그날 테두리는 그대로 유지. 신규 사람마킹은 source=null→테두리 없음.
  const body={[hlIdCol()]:id,highlight:c||null,updated_at:new Date().toISOString()};
  await fetch(SB_URL+'/rest/v1/'+hlTbl(),{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify(body)});
  HIGHLIGHTS[id]=c||null; // HL_SRC[id]는 건드리지 않음 → AI면 'ai' 유지(테두리 유지), 신규는 undefined(테두리 없음)
  // 2) ★ 날짜탭용 영구저장 — 증감액을 마킹한 '오늘' 날짜 셀에 저장 (어제 아님)
  const col=hlIdCol();
  const _t=new Date();
  const tDate=_t.getFullYear()+'-'+String(_t.getMonth()+1).padStart(2,'0')+'-'+String(_t.getDate()).padStart(2,'0');
  await fetch(SB_URL+'/rest/v1/'+perfTbl()+'?date=eq.'+tDate+'&'+col+'=eq.'+id,{method:'PATCH',headers:{...SBH,'Content-Type':'application/json','Prefer':'return=minimal'},body:JSON.stringify({highlight:c||null})});
  // 3) ★ 사람 선택 durable 기록 (daily 행 유무와 무관 → 글로벌 유실 방지, 조언 학습용). 국내·글로벌만.
  //    perfTbl PATCH는 '오늘 행'이 없으면 0행 갱신돼 유실되지만(특히 글로벌 늦은 적재), 여기는 항상 남는다.
  if(MODE==='kr'||MODE==='gl'){
    fetch(SB_URL+'/rest/v1/human_advice_marks',{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify({date:tDate,adset_id:id,region:MODE,tag:c||null,updated_at:new Date().toISOString()})}).catch(()=>{});
  }
  // 메모리 동기화 — AD + 원본배열(GL_AD/VN_AD 등) 모두 갱신 → 탭 전환/모드 전환 후에도 유지
  _syncRowField(id, tDate, 'highlight', c||null);
  rerenderTrendView();
  // 날짜탭이 열려 있으면 같이 그린다 — 날짜탭 하이라이트는 HIGHLIGHTS 를 그대로 읽으므로
  // 저장 한 번으로 추이차트·날짜탭·'메타에 예산 적용' 대상이 같은 값을 보게 된다.
  const _at=document.querySelector('.tab.active');
  if(_at&&_at.dataset.t==='datetab')renderDateTab();
  abSyncBtn();  // 날짜탭 '메타에 예산 적용' 버튼의 대상 개수 갱신
}
// 구글 디멘드젠(국내탭) 세트 하이라이트 — 추이차트와 같은 adset_highlights 테이블·같은 색·0시 자동삭제·전체삭제 공유.
//   키는 구글 ad_group_id(11자리 내외)라 메타 adset_id(17자리)와 겹치지 않는다.
//   단 saveHL 의 ② ad_performance_daily PATCH(메타 전용)·③ human_advice_marks(봇 조언 학습)는 하지 않는다
//   — 메타 세트가 아니어서 매칭될 행이 없고, 조언 학습 데이터를 오염시키지 않기 위해.
async function saveHLGgdg(id,c){
  const body={adset_id:id,highlight:c||null,updated_at:new Date().toISOString()};
  await fetch(SB_URL+'/rest/v1/adset_highlights',{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify(body)});
  KR_HL[id]=c||null;HIGHLIGHTS[id]=c||null;
  renderGgdgTight();   // 안에서 GGDG_ROWS·abSyncBtnG 까지 갱신된다
}
function showCPGgdg(id,el){showCP(id,el);currentHlGgdg=true}
function showCP(id,el){currentHlId=id;currentHlGgdg=false;const cp=document.getElementById('colorPicker');const r=el.getBoundingClientRect();cp.style.left=(r.left+window.scrollX)+'px';cp.style.top=(r.bottom+window.scrollY+4)+'px';cp.classList.add('show')}
async function clearAllHighlights(){
  if(!confirm('추이차트 하이라이트가 삭제됩니다.\n메모는 날짜별로 남습니다(추이차트 메모칸·날짜탭 모두 유지).\n\n계속할까요?'))return;
  const tbl=hlTbl();const col=hlIdCol();
  // 하이라이트 또는 추이차트 메모가 있는 모든 id (메모만 있는 행도 삭제 대상)
  const ids=[...new Set([...Object.keys(HIGHLIGHTS).filter(k=>HIGHLIGHTS[k]),...Object.keys(HL_MEMO).filter(k=>HL_MEMO[k])])];
  if(!ids.length){alert('삭제할 하이라이트·메모가 없습니다.');return}
  for(const id of ids){
    // 하이라이트 테이블 행 삭제 → highlight + memo 동시 제거 (추이차트에서만 사라짐)
    await fetch(SB_URL+'/rest/v1/'+tbl+'?'+col+'=eq.'+id,{method:'DELETE',headers:{...SBH,'Prefer':'return=minimal'}});
    delete HIGHLIGHTS[id];
    delete HL_MEMO[id];
    delete HL_SRC[id];
    // ★ perfTbl의 highlight·memo 컬럼은 건드리지 않음 → 날짜탭 영구 유지
  }
  rerenderTrendView();
}
// ===== 0시 자동 삭제 =====
// 0시가 지나면 추이차트 하이라이트·메모를 자동 삭제(모든 모드 테이블 + 메모리).
// perfTbl(날짜탭) 기록은 건드리지 않아 영구 보존 — clearAllHighlights와 동일 원칙.
const _HL_TBLS=['adset_highlights','global_adset_highlights','ad_creative_highlights','vanced_adset_highlights'];
async function purgeStaleTrendHL(){
  // updated_at < 오늘0시 인 행만 삭제 → 방금 저장한(오늘) 마킹은 안전
  const cut=encodeURIComponent(_hlMidnightISO());
  await Promise.all(_HL_TBLS.map(t=>fetch(SB_URL+'/rest/v1/'+t+'?updated_at=lt.'+cut,{method:'DELETE',headers:{...SBH,'Prefer':'return=minimal'}}).catch(()=>{})));
}
function _clearAllTrendHLMem(){
  [KR_HL,GL_HL,CR_HL,VN_HL,KR_HM,GL_HM,CR_HM,VN_HM,KR_SRC,GL_SRC,CR_SRC,VN_SRC].forEach(m=>{Object.keys(m).forEach(k=>delete m[k])});
}
async function autoClearTrendHL(){
  _clearAllTrendHLMem();      // 롤오버 시점의 메모리는 전부 전날 마킹 → 전체 클리어
  await purgeStaleTrendHL();  // DB도 전날(<오늘0시) 행 삭제
  rerenderTrendView();
}
document.getElementById('colorPicker').querySelectorAll('.cp-btn').forEach(b=>{b.addEventListener('click',e=>{e.stopPropagation();if(currentHlId)(currentHlGgdg?saveHLGgdg:saveHL)(currentHlId,b.dataset.c);document.getElementById('colorPicker').classList.remove('show')})});
// .clickable = 하이라이트 지정 셀 전용 클래스(추이차트·디멘드젠). fx 유무와 무관하게 피커가 닫히지 않도록.
document.addEventListener('click',e=>{if(!e.target.closest('.color-picker')&&!e.target.closest('.clickable'))document.getElementById('colorPicker').classList.remove('show')});

// ===== 추이차트 메모 이력 =====
// 메모는 '쓴 날짜'로 durable 저장소(daily_memos)에 남는다 → 0시 자동삭제·하이라이트 전체삭제와 무관하게 보존.
//   · 오늘 칸(textarea) = 오늘 쓴 메모(수정 가능)
//   · 그 아래 = 지난 메모를 '날짜 + 내용' 으로 최신순 나열 → 하루가 지나면 자동으로 날짜가 붙어 내려간다.
let _MEMO_IDX=null;
function memoIdxInvalidate(){_MEMO_IDX=null}
function _memoIndex(){
  if(_MEMO_IDX)return _MEMO_IDX;
  const idx={};
  Object.keys(DMEMO).forEach(k=>{
    const v=DMEMO[k];if(!v)return;
    const p=k.split('|');if(p.length<3)return;
    const key=p[0]+'|'+p.slice(2).join('|');      // region|entity_id (id 에 | 가 있어도 안전)
    (idx[key]||(idx[key]=[])).push({date:p[1],memo:v});
  });
  Object.keys(idx).forEach(k=>idx[k].sort((a,b)=>a.date<b.date?1:(a.date>b.date?-1:0)));
  _MEMO_IDX=idx;
  return idx;
}
function _mEsc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;')}
// 지난 메모(오늘 제외) 목록 HTML. 기본 4개까지 보여주고 나머지는 마우스오버로.
function _memoHistHtml(id,today){
  const arr=(_memoIndex()[MODE+'|'+id]||[]).filter(x=>x.date!==today);
  if(!arr.length)return '';
  const N=4;
  let h='<div class="memo-hist">';
  arr.slice(0,N).forEach(x=>{
    h+='<div class="mh" title="'+_mEsc(DK(x.date)+' · '+x.memo)+'"><b>'+DK(x.date).slice(3)+'</b> '+_mEsc(x.memo)+'</div>';
  });
  if(arr.length>N){
    const rest=arr.slice(N).map(x=>DK(x.date).slice(3)+' '+x.memo).join(' / ');
    h+='<div class="mh mh-more" title="'+_mEsc(rest)+'">+'+(arr.length-N)+'개 더</div>';
  }
  return h+'</div>';
}
// ===== MEMO =====
// 날짜탭 메모: perfTbl.memo (해당 날짜·영구저장) — 하이라이트 전체삭제와 무관하게 유지
async function saveMemo(date,id,memo,el){
  const m=(memo&&memo.trim())?memo:null;
  const tbl=perfTbl();
  const idCol=hlIdCol();
  // 1) 날짜탭 원본(perfTbl.memo) — 행 있으면 갱신
  await fetch(SB_URL+'/rest/v1/'+tbl+'?date=eq.'+date+'&'+idCol+'=eq.'+id,{method:'PATCH',headers:{...SBH,'Content-Type':'application/json','Prefer':'return=minimal'},body:JSON.stringify({memo:m})});
  // 2) durable 저장소(daily_memos) — 행 존재 무관(B)
  fetch(SB_URL+'/rest/v1/daily_memos',{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify({date,entity_id:id,region:MODE,memo:m,updated_at:new Date().toISOString()})}).catch(()=>{});
  DMEMO[_dmKey(MODE,date,id)]=m;memoIdxInvalidate();
  // 3) 추이차트에도 반영(C) — 하이라이트 테이블 memo + HL_MEMO. 다음 렌더에 추이차트 메모칸에 표시.
  fetch(SB_URL+'/rest/v1/'+hlTbl(),{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify({[hlIdCol()]:id,memo:m,updated_at:new Date().toISOString()})}).catch(()=>{});
  HL_MEMO[id]=m;
  // 메모리 동기화 — 탭/모드 전환 후에도 유지
  _syncRowField(id, date, 'memo', m);
  const ind=el.parentNode.querySelector('.memo-saved');if(ind){ind.classList.add('show');setTimeout(()=>ind.classList.remove('show'),1500)}
}
// 추이차트 메모: 하이라이트 테이블 memo 컬럼에 저장(전체삭제 시 함께 제거) +
//   날짜탭(perfTbl.memo)에도 해당 날짜로 영구저장 → 추이차트에서만 하이라이트와 함께 사라짐.
async function saveTrendMemo(date,id,memo,el){
  const m=(memo&&memo.trim())?memo:null;
  // 1) 추이차트용 — 하이라이트 테이블 upsert (highlight 컬럼은 미포함 → 기존 하이라이트 보존)
  const body={[hlIdCol()]:id,memo:m,updated_at:new Date().toISOString()};
  await fetch(SB_URL+'/rest/v1/'+hlTbl(),{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify(body)});
  HL_MEMO[id]=m;
  // 2) 날짜탭 영구저장. durable(daily_memos) upsert = 행 존재와 무관(글로벌 지연적재 유실 방지·B).
  //    perfTbl.memo PATCH도 병행(행 있으면 갱신 — 봇/CSV 등 기존 리더 호환).
  if(date){
    fetch(SB_URL+'/rest/v1/daily_memos',{method:'POST',headers:{...SBH,'Content-Type':'application/json','Prefer':'resolution=merge-duplicates'},body:JSON.stringify({date,entity_id:id,region:MODE,memo:m,updated_at:new Date().toISOString()})}).catch(()=>{});
    DMEMO[_dmKey(MODE,date,id)]=m;memoIdxInvalidate();
    fetch(SB_URL+'/rest/v1/'+perfTbl()+'?date=eq.'+date+'&'+hlIdCol()+'=eq.'+id,{method:'PATCH',headers:{...SBH,'Content-Type':'application/json','Prefer':'return=minimal'},body:JSON.stringify({memo:m})}).catch(()=>{});
    _syncRowField(id,date,'memo',m);
  }
  const ind=el.parentNode.querySelector('.memo-saved');if(ind){ind.classList.add('show');setTimeout(()=>ind.classList.remove('show'),1500)}
}

// ===== DASHBOARD =====
let dashCharts={};
let dashState={dateA:null,dateB:null,metric:'profit',goalMonth:null};

// 단일 기간 집계 — 두 날짜 드롭다운을 [lo,hi] 구간으로 해석 (순서 무관)
function aggregatePeriod(start,end){
  const lo=start<=end?start:end, hi=start<=end?end:start;
  const rows=AD.filter(r=>r.date>=lo&&r.date<=hi);
  const s=rows.reduce((a,b)=>a+b.spend,0),r=rows.reduce((a,b)=>a+b.revenue,0),p=r-s;
  const mp=rows.reduce((a,b)=>a+b.results_mp,0),uc=rows.reduce((a,b)=>a+b.unique_clicks,0);
  const imp=rows.reduce((a,b)=>a+(b.impressions||0),0),cpm=imp>0?s/imp*1000:0;
  return{s,r,p,mp,uc,imp,cpm,roas:s>0?r/s*100:0,cvr:uc>0&&mp>0?mp/uc*100:0,rows,lo,hi};
}

function renderDashboard(){
  const el=document.getElementById('dashContent');
  if(!dashState.dateA&&DATES.length){dashState.dateA=DATES[0];dashState.dateB=DATES[Math.min(6,DATES.length-1)]||DATES[0]}
  const dateOpts=DATES.map(d=>'<option value="'+d+'">'+DK(d)+' ('+WD(d)+')</option>').join('');
  const modeLabel={kr:'🇰🇷 국내',gl:'🌏 글로벌',cr:'🎨 국내소재',vn:'🎯 밴스드'}[MODE]||MODE;

  let h='';
  // 매출 목표 배너 (국내 / 글로벌 모드) — 항상 표시, 날짜 필터 종료일(가장 늦은 선택일)의 월 기준
  if(MODE==='kr'||MODE==='gl'){
    const pad=n=>String(n).padStart(2,'0');
    const now=new Date();
    const yest=new Date();yest.setDate(yest.getDate()-1);
    const yYmd=yest.getFullYear()+'-'+pad(yest.getMonth()+1)+'-'+pad(yest.getDate());
    const curMonth=now.getFullYear()+'-'+pad(now.getMonth()+1);
    // 대상 월: 월 드롭다운(goalMonth) 선택값 우선, 없으면 현재월. 목표설정월 ∪ 현재월을 최신순 리스트로.
    const allGoalMonths=new Set();['kr','gl'].forEach(m=>Object.keys(MONTHLY_REVENUE_GOAL[m]||{}).forEach(k=>allGoalMonths.add(k)));
    allGoalMonths.add(curMonth);
    let minM=curMonth;allGoalMonths.forEach(m=>{if(m<minM)minM=m});
    const monthList=[];{let yy=+minM.slice(0,4),mm=+minM.slice(5,7);const cy=+curMonth.slice(0,4),cm=+curMonth.slice(5,7);while(yy<cy||(yy===cy&&mm<=cm)){monthList.push(yy+'-'+pad(mm));mm++;if(mm>12){mm=1;yy++}}}
    monthList.reverse();
    const yMonth=(dashState.goalMonth&&monthList.includes(dashState.goalMonth))?dashState.goalMonth:curMonth;
    const yr=+yMonth.slice(0,4),mo=+yMonth.slice(5,7);
    const totalDaysInMonth=new Date(yr,mo,0).getDate();
    const monthStart=yMonth+'-01';
    const monthEnd=yMonth+'-'+pad(totalDaysInMonth);
    const isCurMonth=(yMonth===curMonth);
    // 누적 종료일: 당월이면 어제까지, 과거 월이면 월말까지 (min(월말, 어제))
    const endYmd=(monthEnd<yYmd?monthEnd:yYmd);
    const goal=(MONTHLY_REVENUE_GOAL[MODE]||{})[yMonth];
    let mtd=0,extra='';
    if(MODE==='kr'){
      const tossMap={};TOSS_DAILY.forEach(r=>{tossMap[r.date]=(r.net_amount||r.total_amount||0)});
      const d0=new Date(monthStart),d1=new Date(endYmd);
      for(let d=new Date(d0);d<=d1;d.setDate(d.getDate()+1)){
        const k=d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());
        mtd+=(HIST_REVENUE[k]!=null?HIST_REVENUE[k]:(tossMap[k]||0));
      }
    }else{
      // 글로벌: STRIPE_DATA(revenue_usd) 를 USD 그대로 누적한다(환율 곱하지 않음).
      //   글로벌 탭 지표가 전부 달러라 목표·실적도 달러로 맞춘다. 원화 환산액은 참고로만 괄호에.
      let fbRate=0;
      for(const r of STRIPE_DATA){if(r.usd_krw_rate>0){fbRate=r.usd_krw_rate;break}}
      STRIPE_DATA.forEach(r=>{
        if(r.date>=monthStart&&r.date<=endYmd)mtd+=(r.revenue_usd||0);
      });
      // 환율은 밴스드 대만 기여(KRW 원본)를 USD 로 환산할 때 쓰는 값이라 근거로만 남긴다.
      extra=' <span style="color:#888;font-size:11px">(환율 '+(fbRate?fbRate.toFixed(0):'-')+')</span>';
    }
    // 경과일/남은일: 당월이면 어제까지 경과, 과거 월이면 완료(남은 0)
    const hasData=(endYmd>=monthStart);
    const elapsed=isCurMonth?((yest.getFullYear()===now.getFullYear()&&yest.getMonth()===now.getMonth())?yest.getDate():0):totalDaysInMonth;
    const daysLeft=isCurMonth?Math.max(1,totalDaysInMonth-elapsed):0;
    // 국내=원, 글로벌=달러 (글로벌 탭에서 원화 표기를 남기지 않는다)
    const fmt=n=>MODE==='gl'?('$'+Math.round(n).toLocaleString('en-US')):('₩'+Math.round(n).toLocaleString('ko-KR'));
    // 글로벌: 실적(Stripe KRW)을 '순수 글로벌'과 '밴스드 대만 기여'로 분리.
    // 밴스드 대만 = vanced_ad_performance_daily(VN_TW_ACC, KRW) 대상 월 누적. '글로벌 채널=Stripe−대만밴스드귀속' 정의와 동일.
    let vncTw=0,pureRev=mtd,vncShare=0,pureShare=0;const vncColor='#9333ea';
    if(MODE==='gl'){
      // VN_AD.revenue 는 KRW → 일별 환율로 USD 환산(mtd 가 USD 라 단위를 맞춘다)
      (VN_AD||[]).forEach(r=>{if(String(r.ad_account_id||'')===VN_TW_ACC&&r.date>=monthStart&&r.date<=endYmd){
        const _rt=usdKrwRateAt(r.date)||1450; vncTw+=(+r.revenue||0)/_rt;
      }});
      vncTw=Math.max(0,Math.min(vncTw,mtd));
      pureRev=Math.max(0,mtd-vncTw);
      if(mtd>0){vncShare=vncTw/mtd*100;pureShare=pureRev/mtd*100;}
    }
    const labelFlag=MODE==='kr'?'🇰🇷 국내':'🌏 글로벌';
    const bg=MODE==='kr'?'linear-gradient(135deg,#f0f7ff,#e6f0fb)':'linear-gradient(135deg,#fef6f0,#fbe9d8)';
    const bd=MODE==='kr'?'#b8d4e8':'#e8c8a8';
    const dateLabel=!hasData?'해당 월 데이터 없음'
      :(isCurMonth?(elapsed>0?'기준일: '+DK(endYmd)+' (어제까지 누적)':'이번 달 집계 시작 전 (어제까지 데이터 없음)')
        :'기준일: '+DK(monthEnd)+' (월 마감)');
    const monthOpts=monthList.map(m=>'<option value="'+m+'"'+(m===yMonth?' selected':'')+'>'+m.slice(0,4)+'년 '+parseInt(m.slice(5))+'월</option>').join('');
    const titleHtml='<span style="font-size:14px;font-weight:700">🎯 '+labelFlag+' 매출 목표 달성률</span>'
      +'<select id="goalMonthSel" style="font-size:12px;margin-left:8px;padding:2px 6px;border:1px solid '+bd+';border-radius:4px;font-family:inherit;cursor:pointer">'+monthOpts+'</select>';
    if(goal){
      const pct=goal>0?(mtd/goal*100):0;
      const remaining=goal-mtd;
      const needPerDay=Math.max(0,remaining)/Math.max(1,daysLeft);
      const barW=Math.min(100,Math.max(0,pct)).toFixed(1);
      const barColor=pct>=100?'#16a34a':(pct>=70?'#2563eb':(pct>=40?'#d97706':'#dc2626'));
      const pureW=(barW*(mtd>0?pureRev/mtd:0)).toFixed(2);
      const vncW=(barW*(mtd>0?vncTw/mtd:0)).toFixed(2);
      h+='<div style="background:'+bg+';border:1px solid '+bd+';border-radius:8px;padding:14px 18px;margin-bottom:14px">'
        +'<div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:8px">'
        +titleHtml
        +'<span style="font-size:11px;color:#666">'+dateLabel+'</span>'
        +'</div>'
        +'<div style="display:flex;gap:24px;margin-top:8px;font-size:13px;flex-wrap:wrap;align-items:center">'
        +'<span><b style="color:#555">목표</b> '+fmt(goal)+'</span>'
        +'<span><b style="color:#555">실적</b> <span style="color:#2563eb;font-weight:700">'+fmt(mtd)+'</span>'+extra+'</span>'
        +'<span><b style="color:#555">'+(remaining>0?'남은':'초과')+'</b> '+(remaining>0?fmt(remaining):'<span style="color:#16a34a;font-weight:700">+'+fmt(-remaining)+'</span>')+'</span>'
        +(daysLeft>0&&remaining>0?'<span style="color:#888;font-size:11px">남은 '+daysLeft+'일 · 필요 일평균 '+fmt(needPerDay)+'</span>':'')
        +'<span style="margin-left:auto;color:'+barColor+';font-size:22px;font-weight:700">'+pct.toFixed(1)+'%</span>'
        +'</div>'
        +'<div style="display:flex;background:#e0e6ee;border-radius:4px;height:10px;margin-top:10px;overflow:hidden">'
        +'<div style="background:'+barColor+';height:100%;width:'+(MODE==='gl'?pureW:barW)+'%;transition:width 0.5s" title="순수 글로벌"></div>'
        +(MODE==='gl'?'<div style="background:'+vncColor+';height:100%;width:'+vncW+'%;transition:width 0.5s" title="밴스드 대만"></div>':'')
        +'</div>'
        +(MODE==='gl'&&vncTw>0?'<div style="display:flex;gap:18px;margin-top:9px;font-size:11.5px;flex-wrap:wrap;align-items:center">'
          +'<span style="display:flex;align-items:center;gap:5px"><span style="width:11px;height:11px;border-radius:2px;background:'+barColor+';display:inline-block"></span><b style="color:#555">순수 글로벌</b> '+fmt(pureRev)+' <span style="color:#888">('+pureShare.toFixed(1)+'%)</span></span>'
          +'<span style="display:flex;align-items:center;gap:5px"><span style="width:11px;height:11px;border-radius:2px;background:'+vncColor+';display:inline-block"></span><b style="color:#555">🎯 밴스드 대만 기여</b> '+fmt(vncTw)+' <span style="color:#888">('+vncShare.toFixed(1)+'%)</span></span>'
          +'</div>':'')
        +'</div>';
    }else{
      // 목표 미설정: 실적만 표시 (배너는 항상 노출)
      h+='<div style="background:'+bg+';border:1px solid '+bd+';border-radius:8px;padding:14px 18px;margin-bottom:14px">'
        +'<div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:8px">'
        +titleHtml
        +'<span style="font-size:11px;color:#666">'+dateLabel+'</span>'
        +'</div>'
        +'<div style="display:flex;gap:24px;margin-top:8px;font-size:13px;flex-wrap:wrap;align-items:center">'
        +'<span><b style="color:#555">실적</b> <span style="color:#2563eb;font-weight:700">'+fmt(mtd)+'</span>'+extra+'</span>'
        +'<span style="margin-left:auto;color:#999;font-size:14px;font-weight:700">목표 미설정</span>'
        +'</div>'
        +'</div>';
    }
  }
  h+='<div class="dash-filter"><span style="font-weight:700">'+modeLabel+'</span><label style="margin-left:14px">시작</label><select id="dA">'+dateOpts+'</select><label style="margin-left:10px">종료</label><select id="dB">'+dateOpts+'</select><span id="dPeriodInfo" style="color:#888;margin-left:12px;font-size:11px"></span></div>';

  const A=aggregatePeriod(dashState.dateA,dashState.dateB);

  h+='<div class="kpi-grid">';
  const kpiData=[
    {label:'💰 순이익',v:A.p,isMoney:true,colorByValue:true},
    {label:'🎯 ROAS',v:A.roas,isPct:true},
    {label:'💸 지출',v:A.s,isMoney:true,color:'#d00'},
    {label:'💵 매출',v:A.r,isMoney:true,color:'#2563eb'},
    {label:'🛒 CVR',v:A.cvr,isPct:true},
    {label:'📢 CPM',v:A.cpm,isMoney:true,color:'#795548'},
  ];
  kpiData.forEach(k=>{
    let valCls='';if(k.colorByValue)valCls=k.v>=0?'color:#16a34a':'color:#dc2626';
    if(k.color)valCls='color:'+k.color;
    const val=k.isMoney?money(k.v):(k.v||0).toFixed(2)+'%';
    h+='<div class="kpi-card"><div class="k-label">'+k.label+'</div><div class="k-value" style="'+valCls+'">'+val+'</div></div>';
  });
  h+='</div>';

  // Charts
  h+='<div class="chart-grid">';
  h+='<div class="chart-card"><h3>📊 상품별 합계 <select id="dBarMetric"><option value="profit">이익</option><option value="roas">ROAS</option><option value="spend">지출</option><option value="cvr">CVR</option><option value="revenue">매출</option><option value="purchases">구매수(PG)</option></select></h3><div class="chart-wrap" style="height:280px"><canvas id="chBar"></canvas></div></div>';
  h+='<div class="chart-card"><h3>🥧 지출 vs 매출 비중 — 기간</h3><div class="donut-pair"><div class="donut-cell"><div class="donut-cap">지출</div><div class="chart-wrap"><canvas id="chPieA"></canvas></div></div><div class="donut-cell"><div class="donut-cap">매출</div><div class="chart-wrap"><canvas id="chPieRevA"></canvas></div></div></div></div>';
  h+='<div class="chart-card"><h3>🏆 Top 5 세트 (ROAS)</h3><ul class="top-list" id="topListA"></ul></div>';
  h+='<div class="chart-card"><h3>⚠️ 주의 세트 (지출↑ ROAS↓)</h3><ul class="top-list" id="warnListA"></ul></div>';
  h+='</div>';

  // 메타 매출 비중 차트 (일별)
  h+='<div class="chart-card" style="margin-bottom:16px"><h3>📈 메타 매출 비중 <select id="dRevUnit"><option value="day" selected>일별</option><option value="week">주별</option><option value="month">월별</option></select><select id="dRevDays"><option value="14">14일</option><option value="30" selected>30일</option><option value="60">60일</option><option value="90">90일</option><option value="180">180일</option><option value="210">210일</option></select></h3><div style="position:relative;height:300px"><canvas id="chRevDaily"></canvas></div></div>';

  // 메타 일별 성과 테이블 (위 차트와 동일 기간 · 매출=메타 귀속 매출, 토스 전체 아님)
  h+='<div class="chart-card" style="margin-bottom:16px"><h3>📋 메타 일별 성과 <span style="font-weight:400;color:#888;font-size:10px">매출=메타 귀속 매출 (토스 전체 아님) · 위 차트와 동일 기간</span></h3><div class="mdt-wrap" id="metaDailyTableWrap"></div></div>';

  // Stripe summary for global
  if(MODE==='gl'&&STRIPE_DATA.length){
    const sDates=[...new Set(STRIPE_DATA.map(r=>r.date))].sort().reverse().slice(0,7);
    h+='<div style="margin-top:8px"><div style="font-size:12px;font-weight:700;margin-bottom:8px">💳 Stripe 매출 (USD) — 최근 7일</div><table style="width:100%;max-width:800px"><thead><tr><th>날짜</th><th>대만</th><th>홍콩</th><th>일본</th><th>태국</th><th>싱가포르</th><th>합계</th></tr></thead><tbody>';
    sDates.forEach(d=>{const tw=STRIPE_DATA.find(r=>r.date===d&&r.country==='대만')?.revenue_usd||0;const hk=STRIPE_DATA.find(r=>r.date===d&&r.country==='홍콩')?.revenue_usd||0;const jp=STRIPE_DATA.find(r=>r.date===d&&r.country==='일본')?.revenue_usd||0;const th=STRIPE_DATA.find(r=>r.date===d&&r.country==='태국')?.revenue_usd||0;const sg=STRIPE_DATA.find(r=>r.date===d&&r.country==='싱가포르')?.revenue_usd||0;
    h+='<tr><td>'+DK(d)+'('+WD(d)+')</td><td style="text-align:right">$'+F(tw)+'</td><td style="text-align:right">$'+F(hk)+'</td><td style="text-align:right">$'+F(jp)+'</td><td style="text-align:right">$'+F(th)+'</td><td style="text-align:right">$'+F(sg)+'</td><td style="text-align:right;font-weight:700">$'+F(tw+hk+jp+th+sg)+'</td></tr>'});
    h+='</tbody></table></div>';
  }

  // 🔍 GL_AD(광고귀속 매출) vs Stripe(실제결제 매출) 비교 — 글로벌 모드 전용
  if(MODE==='gl'&&STRIPE_DATA.length&&GL_AD.length){
    const days=14;
    const allDates=[...new Set([...GL_AD.map(r=>r.date),...STRIPE_DATA.map(r=>r.date)])].sort().reverse().slice(0,days);
    const adByDate={},stripeByDate={};
    GL_AD.forEach(r=>{if(!adByDate[r.date])adByDate[r.date]=0;adByDate[r.date]+=(r.revenue_usd||0)});
    STRIPE_DATA.forEach(r=>{if(!stripeByDate[r.date])stripeByDate[r.date]=0;stripeByDate[r.date]+=(r.revenue_usd||0)});
    let totA=0,totS=0;
    allDates.forEach(d=>{totA+=adByDate[d]||0;totS+=stripeByDate[d]||0});
    const totRatio=totS>0?(totA/totS*100):0;
    const ratioColor=r=>r<50||r>200?'#dc2626':(r<80||r>150?'#d97706':'#16a34a');
    h+='<div style="margin-top:16px"><div style="font-size:12px;font-weight:700;margin-bottom:8px">🔍 광고귀속(GL_AD) vs 실제결제(Stripe) — 최근 '+days+'일 <span style="font-weight:400;color:#888;font-size:10px">attribution 정확도 · 합계 비율 <span style="color:'+ratioColor(totRatio)+';font-weight:700">'+totRatio.toFixed(0)+'%</span></span></div>';
    h+='<table style="width:100%;max-width:900px"><thead><tr><th>날짜</th><th>광고귀속(GL_AD)</th><th>실제결제(Stripe)</th><th>차액</th><th>비율</th></tr></thead><tbody>';
    allDates.forEach(d=>{
      const a=adByDate[d]||0,s=stripeByDate[d]||0;
      const diff=a-s,ratio=s>0?(a/s*100):0,c=ratioColor(ratio);
      h+='<tr><td>'+DK(d)+'('+WD(d)+')</td><td style="text-align:right">$'+F(a)+'</td><td style="text-align:right">$'+F(s)+'</td><td style="text-align:right;color:'+(diff>=0?'#16a34a':'#dc2626')+'">'+(diff>=0?'+':'-')+'$'+F(Math.abs(diff))+'</td><td style="text-align:right;color:'+c+';font-weight:600">'+(s>0?ratio.toFixed(0)+'%':'-')+'</td></tr>';
    });
    h+='<tr style="background:#f5f5f5;font-weight:700"><td>합계</td><td style="text-align:right">$'+F(totA)+'</td><td style="text-align:right">$'+F(totS)+'</td><td style="text-align:right;color:'+((totA-totS)>=0?'#16a34a':'#dc2626')+'">'+((totA-totS)>=0?'+':'-')+'$'+F(Math.abs(totA-totS))+'</td><td style="text-align:right;color:'+ratioColor(totRatio)+'">'+totRatio.toFixed(0)+'%</td></tr>';
    h+='</tbody></table><div style="font-size:9px;color:#666;margin-top:4px">※ 비율 80~150% 정상, 50~200% 주의(노란색), 그 외 이상(빨간색). Stripe는 일부 통화 결제만 포함될 수 있음.</div></div>';
  }

  el.innerHTML=h;

  // Wire dropdowns
  const _gms=document.getElementById('goalMonthSel');
  if(_gms)_gms.addEventListener('change',e=>{dashState.goalMonth=e.target.value;renderDashboard()});
  document.getElementById('dA').value=dashState.dateA;
  document.getElementById('dB').value=dashState.dateB;
  document.getElementById('dBarMetric').value=dashState.metric;
  const dLo=new Date(A.lo),dHi=new Date(A.hi);
  const days=Math.round((dHi-dLo)/864e5)+1;
  document.getElementById('dPeriodInfo').textContent='기간: '+DK(A.lo)+' ~ '+DK(A.hi)+' ('+days+'일)';
  document.getElementById('dA').addEventListener('change',e=>{dashState.dateA=e.target.value;renderDashboard()});
  document.getElementById('dB').addEventListener('change',e=>{dashState.dateB=e.target.value;renderDashboard()});
  document.getElementById('dBarMetric').addEventListener('change',e=>{dashState.metric=e.target.value;drawDashBar(A)});
  document.getElementById('dRevDays').addEventListener('change',()=>{drawRevDaily();drawMetaDailyTable()});
  document.getElementById('dRevUnit').addEventListener('change',drawRevDaily);

  setTimeout(()=>{drawDashBar(A);drawDashPie(A);drawDashPieRev(A);drawDashTopLists(A);drawRevDaily();drawMetaDailyTable()},50);
}

// Chart.js plugin: outside leader-line labels with arrow tip for doughnut slices.
// 슬라이스 중간각 → 외부 leader → 화살표 → 다중행 라벨. minPct 미만은 생략.
// 호버 없이도 상품 · 금액 · 비중% 노출. 옵션: opts.format(label,value,pct), opts.minPct.
const outsideLabelsPlugin={
  id:'outsideLabels',
  afterDraw(chart,_a,opts){
    if(!opts||!opts.enabled)return;
    const meta=chart.getDatasetMeta(0);if(!meta||!meta.data||!meta.data.length)return;
    const ds=chart.data.datasets[0];const labels=chart.data.labels||[];
    const total=ds.data.reduce((a,b)=>a+(+b||0),0);if(total<=0)return;
    const fmt=opts.format||((l,v,p)=>l+' '+p.toFixed(0)+'%');
    const minPct=opts.minPct!=null?opts.minPct:1.5;
    const lineColor=opts.lineColor||'#666';
    const textColor=opts.textColor||'#222';
    const font=opts.font||'10px sans-serif';
    const horiz=opts.horiz||16;   // 수평 leader 길이
    const radial=opts.radial||10; // 방사 leader 길이
    const ah=opts.arrowSize||4;
    const lineH=opts.lineHeight||11;
    const ctx=chart.ctx;ctx.save();ctx.font=font;ctx.fillStyle=textColor;ctx.strokeStyle=lineColor;ctx.lineWidth=1;
    meta.data.forEach((arc,i)=>{
      const v=+ds.data[i]||0;if(v<=0)return;
      const pct=v/total*100;if(pct<minPct)return;
      const p=arc.getProps(['x','y','startAngle','endAngle','outerRadius'],true);
      const mid=(p.startAngle+p.endAngle)/2;
      const cx=Math.cos(mid),sy=Math.sin(mid);
      const x0=p.x+cx*p.outerRadius,y0=p.y+sy*p.outerRadius;
      const x1=p.x+cx*(p.outerRadius+radial),y1=p.y+sy*(p.outerRadius+radial);
      const right=cx>=0;
      const x2=x1+(right?horiz:-horiz),y2=y1;
      // leader
      ctx.beginPath();ctx.moveTo(x0,y0);ctx.lineTo(x1,y1);ctx.lineTo(x2,y2);ctx.stroke();
      // arrow tip at x2 (outward)
      ctx.beginPath();
      if(right){ctx.moveTo(x2,y2);ctx.lineTo(x2-ah,y2-ah);ctx.moveTo(x2,y2);ctx.lineTo(x2-ah,y2+ah);}
      else{ctx.moveTo(x2,y2);ctx.lineTo(x2+ah,y2-ah);ctx.moveTo(x2,y2);ctx.lineTo(x2+ah,y2+ah);}
      ctx.stroke();
      // label
      const text=String(fmt(labels[i]||'',v,pct));const lines=text.split('\n');
      ctx.textBaseline='middle';ctx.textAlign=right?'left':'right';
      const startY=y2-(lines.length-1)*lineH/2;const tx=x2+(right?3:-3);
      lines.forEach((ln,li)=>ctx.fillText(ln,tx,startY+li*lineH));
    });
    ctx.restore();
  }
};
Chart.register(outsideLabelsPlugin);

const BRAND_COLORS=['#4285f4','#ea4335','#fbbc04','#34a853','#9c27b0','#ff6d01','#00acc1','#e91e63','#795548','#607d8b','#f59e0b','#10b981'];

function aggByProduct(rows){
  const by={};rows.forEach(r=>{if(!by[r.product])by[r.product]={product:r.product,spend:0,revenue:0,profit:0,mp:0,uc:0,meta:0};by[r.product].spend+=r.spend;by[r.product].revenue+=r.revenue;by[r.product].profit+=r.profit;by[r.product].mp+=r.results_mp;by[r.product].uc+=r.unique_clicks;by[r.product].meta+=(r.results_meta||0)});
  return Object.values(by).filter(x=>x.spend>0);
}

function drawDashBar(A){
  const metric=dashState.metric;
  const agg=aggByProduct(A.rows);
  const getVal=m=>{
    if(metric==='profit')return m.profit;
    if(metric==='spend')return m.spend;
    if(metric==='revenue')return m.revenue;
    if(metric==='roas')return m.spend>0?m.revenue/m.spend*100:0;
    if(metric==='cvr')return m.uc>0&&m.mp>0?m.mp/m.uc*100:0;
    if(metric==='purchases')return m.meta||0;
    return 0;
  };
  agg.sort((a,b)=>getVal(b)-getVal(a));
  const top=agg.slice(0,12);
  if(dashCharts.bar)dashCharts.bar.destroy();
  const ctx=document.getElementById('chBar');if(!ctx)return;
  const labelMap={profit:'이익',roas:'ROAS(%)',spend:'지출',cvr:'CVR(%)',revenue:'매출',purchases:'구매수(PG)'};
  dashCharts.bar=new Chart(ctx,{type:'bar',data:{
    labels:top.map(x=>x.product),
    datasets:[{label:labelMap[metric]||metric,data:top.map(getVal),backgroundColor:'#4285f4'}]
  },options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'top',labels:{font:{size:11}}}},scales:{x:{ticks:{font:{size:10}}},y:{ticks:{font:{size:10}}}}}});
}

// 도넛 차트 공통 옵션 — outsideLabels(외부 leader + 화살표 + 다중행 라벨) 활성화
function _donutOptions(total){
  return {
    responsive:true, maintainAspectRatio:false,
    layout:{padding:{left:64,right:64,top:14,bottom:14}},
    plugins:{
      legend:{display:false},
      tooltip:{callbacks:{label:c=>{const v=c.parsed||0;const pct=total>0?(v/total*100):0;return c.label+': '+money(v)+' ('+pct.toFixed(1)+'%)';}}},
      outsideLabels:{enabled:true,minPct:2,format:(l,v,p)=>l+'\n'+money(v)+' · '+p.toFixed(0)+'%'}
    }
  };
}

function drawDashPie(A){
  const agg=aggByProduct(A.rows).filter(x=>x.spend>0).sort((a,b)=>b.spend-a.spend).slice(0,10);
  if(dashCharts.pie)dashCharts.pie.destroy();
  const ctx=document.getElementById('chPieA');if(!ctx)return;
  const total=agg.reduce((s,x)=>s+x.spend,0);
  dashCharts.pie=new Chart(ctx,{type:'doughnut',data:{labels:agg.map(x=>x.product),datasets:[{data:agg.map(x=>x.spend),backgroundColor:BRAND_COLORS}]},options:_donutOptions(total)});
}

function drawDashPieRev(A){
  const agg=aggByProduct(A.rows).filter(x=>x.revenue>0).sort((a,b)=>b.revenue-a.revenue).slice(0,10);
  if(dashCharts.pieRev)dashCharts.pieRev.destroy();
  const ctx=document.getElementById('chPieRevA');if(!ctx)return;
  const total=agg.reduce((s,x)=>s+x.revenue,0);
  dashCharts.pieRev=new Chart(ctx,{type:'doughnut',data:{labels:agg.map(x=>x.product),datasets:[{data:agg.map(x=>x.revenue),backgroundColor:BRAND_COLORS}]},options:_donutOptions(total)});
}

function drawDashTopLists(A){
  const byA={};A.rows.forEach(r=>{const rid=rowId(r);if(!byA[rid])byA[rid]={name:MODE==='cr'?(r.ad_name||''):(r.adset_name||''),product:r.product,spend:0,revenue:0};byA[rid].spend+=r.spend;byA[rid].revenue+=r.revenue});
  const list=Object.values(byA).filter(x=>x.spend>=100).map(x=>({...x,roas:x.spend>0?x.revenue/x.spend*100:0}));
  const top=list.sort((a,b)=>b.roas-a.roas).slice(0,5);
  const topEl=document.getElementById('topListA');
  if(topEl)topEl.innerHTML=top.map((x,i)=>'<li><span><span class="rank">#'+(i+1)+'</span>'+(x.name||'').slice(0,22)+' <span style="color:#888;font-size:9px">('+x.product+')</span></span><span class="val" style="color:#16a34a">'+x.roas.toFixed(0)+'%</span></li>').join('')||'<li style="color:#888">데이터 없음</li>';
  const warn=list.filter(x=>x.spend>=500&&x.roas<80).sort((a,b)=>b.spend-a.spend).slice(0,5);
  const wEl=document.getElementById('warnListA');
  if(wEl)wEl.innerHTML=warn.map((x,i)=>'<li><span><span class="rank" style="color:#dc2626">!</span>'+(x.name||'').slice(0,22)+'</span><span class="val" style="color:#dc2626">'+x.roas.toFixed(0)+'% / '+money(x.spend)+'</span></li>').join('')||'<li style="color:#888">양호</li>';
}

// ===== META REVENUE DAILY CHART =====
function drawRevDaily(){
  const daysN=parseInt(document.getElementById('dRevDays')?.value||'30');
  const unit=document.getElementById('dRevUnit')?.value||'day';
  const dd=DATES.slice(0,daysN).reverse(); // oldest→newest (선택한 일수 창)
  // Daily aggregation (Meta)
  const daily={};
  AD.forEach(r=>{
    if(!daily[r.date])daily[r.date]={spend:0,revenue:0,mp:0,uc:0,imp:0};
    daily[r.date].spend+=r.spend;
    daily[r.date].revenue+=r.revenue;
    daily[r.date].mp+=r.results_mp;
    daily[r.date].uc+=r.unique_clicks;
    daily[r.date].imp+=(r.impressions||0);
  });
  // Toss 전체 매출 lookup
  const tossMap={};
  TOSS_DATA.forEach(r=>{tossMap[r.date]=r.net_amount||0});

  // 일/주/월 버킷팅 — 선택한 일수 창(dd) 안에서 합산 (주=월요일 시작 WM, 월=YYYY-MM)
  const bk={};const order=[];
  dd.forEach(d=>{
    const key=unit==='week'?WM(d):unit==='month'?d.slice(0,7):d;
    if(!bk[key]){bk[key]={key,spend:0,metaRev:0,tossRev:0};order.push(key)}
    const x=daily[d];if(x){bk[key].spend+=x.spend;bk[key].metaRev+=x.revenue}
    bk[key].tossRev+=tossMap[d]||0;
  });
  const buckets=order.map(k=>bk[k]);
  const _blab=k=>{if(unit==='week'){const m=new Date(k);return(m.getMonth()+1)+'/'+m.getDate()}if(unit==='month')return k.slice(2);return DK(k).slice(3)};
  const labels=buckets.map(b=>_blab(b.key));
  const profitData=buckets.map(b=>b.metaRev-b.spend);
  const metaRevData=buckets.map(b=>b.metaRev);
  const tossRevData=buckets.map(b=>b.tossRev);
  const roasData=buckets.map(b=>b.spend>0?b.metaRev/b.spend*100:0);
  // ★ 메타 비중 = Meta 매출(MP) / 토스 전체 매출 × 100
  const ratioData=buckets.map(b=>b.tossRev>0?b.metaRev/b.tossRev*100:0);

  if(dashCharts.revDaily)dashCharts.revDaily.destroy();
  const ctx=document.getElementById('chRevDaily');if(!ctx)return;
  dashCharts.revDaily=new Chart(ctx,{type:'bar',data:{
    labels,
    datasets:[
      {type:'bar',label:'순이익',data:profitData,backgroundColor:profitData.map(v=>v>=0?'rgba(22,163,74,0.35)':'rgba(220,38,38,0.35)'),borderColor:profitData.map(v=>v>=0?'#16a34a':'#dc2626'),borderWidth:1,yAxisID:'y',order:4},
      {type:'bar',label:'메타매출',data:metaRevData,backgroundColor:'rgba(37,99,235,0.25)',borderColor:'#2563eb',borderWidth:1,yAxisID:'y',order:3},
      {type:'bar',label:'토스전체',data:tossRevData,backgroundColor:'rgba(139,92,246,0.2)',borderColor:'#8b5cf6',borderWidth:1,yAxisID:'y',order:2},
      {type:'line',label:'ROAS%',data:roasData,borderColor:'#16a34a',backgroundColor:'transparent',borderWidth:2,pointRadius:2,yAxisID:'y1',order:1},
      {type:'line',label:'메타비중%',data:ratioData,borderColor:'#f59e0b',backgroundColor:'transparent',borderWidth:2,pointRadius:2,borderDash:[4,4],yAxisID:'y1',order:0},
    ]
  },options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
    plugins:{legend:{position:'top',labels:{font:{size:10}}},
      tooltip:{callbacks:{label:function(ctx){
        const v=ctx.raw;if(ctx.dataset.yAxisID==='y1')return ctx.dataset.label+': '+v.toFixed(1)+'%';
        return ctx.dataset.label+': '+money(v);
      }}}
    },
    scales:{
      y:{position:'left',ticks:{font:{size:9},callback:v=>{if(MODE==='gl')return'$'+(v/1000).toFixed(0)+'k';return'₩'+(v/10000).toFixed(0)+'만'}},grid:{color:'#f0f0f0'}},
      y1:{position:'right',min:0,max:Math.max(300,...roasData,...ratioData)+20,ticks:{font:{size:9},callback:v=>v+'%'},grid:{display:false}},
      x:{ticks:{font:{size:9},maxRotation:45}}
    }
  }});
}

// 메타 일별 성과 테이블 — 위 chRevDaily 차트와 동일 기간/데이터.
// 컬럼: 날짜 · 노출 · 클릭 · CTR · CPC · 광고비 · CPA · 직접 전환 · 직접 매출 · 직접ROAS · 총전환
// 매출은 메타 귀속 매출(r.revenue), 토스 전체 매출 아님. MODE(gl)는 money()가 $ 처리.
function drawMetaDailyTable(){
  const wrap=document.getElementById('metaDailyTableWrap');if(!wrap)return;
  const daysN=parseInt(document.getElementById('dRevDays')?.value||'30');
  const dd=DATES.slice(0,daysN); // newest first
  const daily={};
  AD.forEach(r=>{
    if(!daily[r.date])daily[r.date]={imp:0,clicks:0,spend:0,revenue:0,meta:0,mp:0};
    const x=daily[r.date];
    x.imp+=(r.impressions||0);
    x.clicks+=(r.clicks||r.unique_clicks||0);
    x.spend+=(r.spend||0);
    x.revenue+=(r.revenue||0);
    x.meta+=(r.results_meta||0);
    x.mp+=(r.results_mp||0);
  });
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  const T={imp:0,clicks:0,spend:0,revenue:0,meta:0,mp:0};
  dd.forEach(d=>{const x=daily[d];if(x){T.imp+=x.imp;T.clicks+=x.clicks;T.spend+=x.spend;T.revenue+=x.revenue;T.meta+=x.meta;T.mp+=x.mp;}});

  const roasCls=v=>v>=200?'roas-hi':(v>0&&v<100?'roas-lo':'');
  const rowCells=x=>{
    const ctr=x.imp>0?x.clicks/x.imp*100:0;
    const cpc=x.clicks>0?x.spend/x.clicks:0;
    const cpa=x.meta>0?x.spend/x.meta:0;
    const roas=x.spend>0?x.revenue/x.spend*100:0;
    return '<td>'+F(x.imp)+'</td>'
      +'<td>'+F(x.clicks)+'</td>'
      +'<td>'+ctr.toFixed(2)+'%</td>'
      +'<td>'+money(cpc)+'</td>'
      +'<td>'+money(x.spend)+'</td>'
      +'<td>'+money(cpa)+'</td>'
      +'<td>'+F(x.meta)+'</td>'
      +'<td style="font-weight:600">'+money(x.revenue)+'</td>'
      +'<td class="'+roasCls(roas)+'">'+(x.spend>0?roas.toFixed(0)+'%':'-')+'</td>'
      +'<td>'+F(x.mp)+'</td>';
  };

  let h='<table class="mdt"><thead><tr>'
    +'<th class="dt">날짜</th><th>노출</th><th>클릭</th><th>CTR</th><th>CPC</th>'
    +'<th>광고비</th><th>CPA</th><th>직접 전환</th><th>직접 매출</th><th>직접ROAS</th><th>총전환</th>'
    +'</tr></thead><tbody>';
  h+='<tr class="mdt-total"><td class="dt">합계 '+dd.length+'일</td>'+rowCells(T)+'</tr>';
  dd.forEach(d=>{
    const x=daily[d]||{imp:0,clicks:0,spend:0,revenue:0,meta:0,mp:0};
    const cls=d===yDay?' class="mdt-yday"':'';
    h+='<tr'+cls+'><td class="dt">'+DK(d)+'('+WD(d)+')</td>'+rowCells(x)+'</tr>';
  });
  h+='</tbody></table>';
  wrap.innerHTML=h;
}

// 추이차트 정렬 기준 = '현재 일예산' — 세트별로 budget 이 잡힌 가장 최근 날짜의 값.
//   메타 예산은 수동 설정값이라 최신일 값이 곧 지금 Ads Manager 의 일예산이다.
//   표시 기간(dd) 밖 날짜도 훑어야 오래 쉰 세트도 마지막 예산으로 줄을 세운다.
//   budget 이 없는 소스(국내소재 cr·구글·네이버)는 0 이 되어 기존 기준(지출)으로 자연히 떨어진다.
//   ※ 2026-08-20: 기본 정렬을 '전날 지출순' → '예산순'으로 바꾸고, 변형(복제·tROAS 등) 세트를
//     원본 밑에 계보로 묶던 규칙(dvTreeOrder)은 기본에서 뺐다. 예전처럼 보고 싶으면 상단
//     '정렬' 드롭다운에서 카테고리를 고르면 된다(TSORT).
function curBudMap(rows,accFilter){
  const last={};
  rows.forEach(r=>{
    if(accFilter&&!accFilter(r))return;
    const b=+(r.budget)||0;if(!(b>0))return;
    const rid=rowId(r);if(!rid)return;
    const p=last[rid];if(!p||r.date>p.d)last[rid]={d:r.date,b:b};
  });
  const out={};Object.keys(last).forEach(k=>{out[k]=last[k].b});return out;
}
// 세트 정렬 비교자 — 예산↓ → (동률·예산없음) 전날 지출↓ → 7일 지출↓
function budCmp(a,b){return (( b._bud||0)-(a._bud||0))||((b._yS||0)-(a._yS||0))||((b._s||0)-(a._s||0))}

// ===== 추이차트 정렬 모드 =====
//   모든 추이차트(일별·주월·보조지표·대만·상품별·틱톡)가 이 값 하나를 공유한다.
//   budget   = 💸 예산순(기본) — 현재 일예산 큰 순
//   spend    = 지출순 — 전날(주월은 최근 기간) 지출 큰 순 (예전 기본값)
//   category = 카테고리 — 원본 밑에 변형(복제·tROAS 등)을 계보로 묶어 붙임(dvTreeOrder)
let TSORT=(function(){try{return localStorage.getItem('tsort')||'budget'}catch(e){return 'budget'}})();
function tSortMode(){return TSORT}
function setTSort(v){
  TSORT=v||'budget';
  try{localStorage.setItem('tsort',TSORT)}catch(e){}
  _syncTSortSel();                                  // 탭마다 있는 드롭다운 동기화
  const t=document.querySelector('.tab.active');if(t)renderTab(t.dataset.t);
}
// 시작 시 각 탭 드롭다운을 저장값으로 맞춘다(스크립트가 body 끝에서 실행 → 즉시 + DOMContentLoaded 양쪽).
function _syncTSortSel(){document.querySelectorAll('.tsort-sel').forEach(el=>{el.value=TSORT})}
_syncTSortSel();document.addEventListener('DOMContentLoaded',_syncTSortSel);
// 상품(📦) 그룹 정렬 — 예산순일 때만 예산 합 기준, 나머지는 기존대로 최근 지출 기준.
//   byProd[k] = {yS:최근 지출 합, bud:예산 합}
function orderProdKeys(byProd){
  const m=tSortMode();
  return Object.keys(byProd).sort((a,b)=>m==='budget'
    ?((byProd[b].bud-byProd[a].bud)||(byProd[b].yS-byProd[a].yS))
    :(byProd[b].yS-byProd[a].yS));
}
// 상품 안의 세트 정렬 — 카테고리 모드에서만 계보(원본+변형) 배치를 쓴다.
function orderSets(arr){
  const m=tSortMode();
  if(m==='category')return dvTreeOrder(arr,MODE==='kr'||MODE==='gl');
  dvTreeOrder(arr,false);          // 계보 플래그 초기화(└ 들여쓰기·🧬 토글 제거)
  return m==='spend'?arr.sort((a,b)=>((b._yS||0)-(a._yS||0))||((b._s||0)-(a._s||0))):arr.sort(budCmp);
}

// 추이차트 세트 계보 정렬 — 오리지널 세트를 맨 위, 그 아래에 troas·구매당비용·복제 등
//   실험(파생) 세트를 들여쓰기로 붙여 '하위 세트'처럼 보이게 한다(실제 트리는 아니고 배치만).
//   토글 2단: 오리지널 행의 🧬N = 실험세트 접기/펼치기(기본 펼침), ▶ = 소재 펼치기(기존 그대로).
//   가족 판정은 🧬복제·변형 탭과 같은 dvClassify(세트명 마커·날짜토큰 제거 후 계보키).
//   정렬: ① 가족(원본+파생) 매출 합 큰 순 ② 가족 안에서는 원본 먼저, 나머지는 매출 큰 순.
//   원본이 기간 안에 없으면(중단·이름변경) 매출 1위 파생이 가족 머리가 되고 들여쓰기 없음.
function dvTreeOrder(adsets,enabled){
  if(!enabled){adsets.forEach(a=>{a._dvChild=false;a._dvHead=null;a._dvKids=0});return adsets}
  const fam={};
  adsets.forEach(a=>{
    const c=dvClassify(a.an||'');a._dv=c;
    const k=c.key||('#'+a.id);
    if(!fam[k])fam[k]={mem:[],r:0,s:0};
    fam[k].mem.push(a);fam[k].r+=(a._r||0);fam[k].s+=(a._s||0);
  });
  const fams=Object.keys(fam).map(k=>fam[k]);
  fams.forEach(f=>{
    f.mem.sort((x,y)=>{
      const ox=(x._dv&&x._dv.kind==='orig')?0:1,oy=(y._dv&&y._dv.kind==='orig')?0:1;
      if(ox!==oy)return ox-oy;
      return ((y._r||0)-(x._r||0))||((y._s||0)-(x._s||0));
    });
    //   들여쓰기는 '원본이 아닌' 파생만 — 동명 원본이 둘이면 둘 다 맨 위에 나란히 두고,
    //   원본이 아예 없는 가족은 머리(매출 1위 파생)를 들여쓰지 않는다(위가 비어 보이지 않게).
    f.mem.forEach((m,i)=>{m._dvChild=(i>0&&m._dv.kind!=='orig');m._dvHead=null;m._dvKids=0});
    //   가족 머리(맨 윗행) ↔ 실험세트 연결 — 머리의 🧬 토글이 data-dvfam 으로 자식 행을 찾는다.
    const _hd=f.mem[0],_kid=f.mem.filter(m=>m._dvChild);
    _kid.forEach(m=>{m._dvHead=_hd.id});_hd._dvKids=_kid.length;
  });
  fams.sort((a,b)=>(b.r-a.r)||(b.s-a.s));
  const out=[];fams.forEach(f=>f.mem.forEach(m=>out.push(m)));
  return out;
}
// 오리지널 행에 붙는 실험세트 토글 버튼(자식 없으면 빈 문자열).
function famBtn(a){
  if(!a._dvKids)return'';
  return `<span class="fam-caret" data-open="1" title="실험(복제·변형) 세트 ${a._dvKids}개 접기" onclick="event.stopPropagation();toggleFamily('${a.id}',this)">▼${a._dvKids}</span>`;
}
// 실험세트 토글 — 오리지널 행의 🧬N 클릭 시 그 가족의 실험(복제·변형) 행을 접기/펼치기.
//   기본은 펼침(렌더 시 그대로 노출), 클릭하면 접힌다. 접을 때는 그 실험세트들이
//   ▶ 로 펼쳐둔 소재 행(tr.creative-expanded)도 함께 숨긴다.
function toggleFamily(headId,btn){
  const tbody=btn.closest('tbody');if(!tbody)return;
  const rows=tbody.querySelectorAll('tr[data-dvfam="'+CSS.escape(headId)+'"]');
  const open=btn.dataset.open!=='0';
  const show=!open;
  rows.forEach(tr=>{
    tr.style.display=show?'':'none';
    const cid=tr.dataset.adsetRow;
    if(cid)tbody.querySelectorAll('tr.creative-expanded[data-parent="'+CSS.escape(cid)+'"]').forEach(r=>{r.style.display=show?'':'none'});
  });
  btn.dataset.open=show?'1':'0';
  const _t=btn.closest('table');if(_t){_fitNameCols(_t);_fixSticky(_t)}
  btn.textContent=(show?'▼':'▶')+rows.length;
  btn.title='실험(복제·변형) 세트 '+rows.length+'개 '+(show?'접기':'펼치기');
}
// ===== TREND (상품별 그룹) =====
function renderTrend(opts){
  // opts.tableId/daysElId/filterElId/accFilter — 대만 추이차트 등 분리 뷰용. (이벤트핸들러가 Event를 넘겨도 안전)
  opts=(opts&&opts.tableId)?opts:{};
  const TBL=opts.tableId||'tTbl';
  const daysElId=opts.daysElId||'tDays';
  const filterElId=opts.filterElId||'tFilter';
  // accFilter: 국가 구분은 상단 countrySel(국가 필터)로 처리 → 구 ad_account(대만계정) 분리 제거.
  const accFilter=opts.accFilter||null;
  // opts.rows: 명시 시 전역 AD 대신 이 데이터셋으로 렌더(글로벌 추이차트 타이트/밴스드/모두 등)
  //   글로벌은 GL_META_DAYS 날짜만 Meta 보고값으로 치환해서 그린다(다른 탭은 영향 없음).
  const _ROWS0=opts.rows||AD;
  const ROWS=(MODE==='gl')?_ROWS0.map(glMetaRow):_ROWS0;
  console.log('[DBG] renderTrend called, MODE=',MODE,'TBL=',TBL);
  const days=parseInt(document.getElementById(daysElId).value);
  const dd=DATES.slice(0,days);const d7=dd.slice(0,7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // 오늘 날짜 — 증감액 테두리는 '오늘' 셀 기준(어제 아님). 오늘 컬럼은 현재 마킹(HIGHLIGHTS)으로 그려 per-date row 누락에도 항상 표시.
  const _td=new Date();const today=_td.getFullYear()+'-'+String(_td.getMonth()+1).padStart(2,'0')+'-'+String(_td.getDate()).padStart(2,'0');
  // 이틀 전(D-2): 어제 데이터의 하루 전날에 증감액(증액/감액)을 했는지 표시 (KR/GL 모드)
  const _d2=new Date();_d2.setDate(_d2.getDate()-2);const d2Day=_d2.getFullYear()+'-'+String(_d2.getMonth()+1).padStart(2,'0')+'-'+String(_d2.getDate()).padStart(2,'0');
  const CHG_HL=new Set(['up20','up10','down10','down20','down50','watch']);
  const chgD2={};ROWS.forEach(r=>{if(r.date===d2Day&&CHG_HL.has(r.highlight))chgD2[rowId(r)]=r.highlight});
  const showChg=MODE==='kr'||MODE==='gl';
  // 광고 계정 컬럼: 국내·글로벌 추이차트에서만 캠페인 왼쪽에 계정 '이름' 표시 (ID는 미표시)
  const showAcc=MODE==='kr'||MODE==='gl';
  // 증감액(증액/감액) 테두리: 실제 메타 예산(budget)의 전일 대비 변화로 자동 판정.
  //   예산은 수동 설정값이라 값이 바뀌면 = 사람이 증액/감액한 것. 과거 보유한 모든 날짜 셀에 퍼센트별 색 링 표시.
  //   색: +20%↑=up20, +변화=up10, -변화=down10, -20%↓=down20, -50%↓=down50(연한 회색).
  //   |변화율|<BUD_MIN_PCT% 는 노이즈로 무시.
  //   BUD_MIN_PCT: KR·GL전체는 예산이 정확값(정수 KRW / country분할 합=원예산)이라 소액 변화도 실제 → 0.5%.
  //     GL 특정국가 선택 시엔 예산이 '지출비중 분할'이라 지출흔들림 노이즈가 크므로 3% 유지.
  //   off(세트 OFF)는 예산 값이 그대로 남아 예산비교로 감지 불가 → 기존 수동 마킹(highlight==='off')으로 검은 링 유지.
  const budExact=(MODE==='kr')||(MODE==='gl'&&COUNTRY==='ALL');
  const BUD_MIN_PCT=budExact?0.5:3;
  const budBorderKey=(pct)=>pct>=20?'up20':pct>=BUD_MIN_PCT?'up10':pct<=-50?'down50':pct<=-20?'down20':pct<=-BUD_MIN_PCT?'down10':null;
  const AUX=opts.metric==='aux';  // 보조지표 모드: 셀에 CTR/CVR/CPM/CPP/구매당단가 표시
  // 데이터 셀 클릭 → 시간별 ROAS 화면(hrOpen). 행이 '세트'인 모드에서만 — 소재별(cr)은 광고 단위라 제외.
  //   HR_MODES 를 직접 안 보는 이유: 그 const 는 파일 맨 아래라 여기서 참조하면 TDZ 위험이 있다.
  const hrOK=(MODE==='kr'||MODE==='gl'||MODE==='vn');
  const byA={};
  ROWS.forEach(r=>{if(!dd.includes(r.date))return;if(accFilter&&!accFilter(r))return;const rid=rowId(r);if(!byA[rid])byA[rid]={cn:r.campaign_name,an:MODE==='cr'?(r.ad_name||''):(r.adset_name||''),id:rid,product:r.product,acc:r.ad_account_id||'',d:{}};byA[rid].d[r.date]=r});
  // 예산 변화 판정용 '전체 히스토리' 맵 (표시기간 dd 밖의 직전일도 포함해야 가장 오래된 열의 증감도 판정 가능).
  //   dd 로만 비교하면 표시구간 첫 열은 비교 대상(직전일)이 없어 항상 테두리가 안 떴음.
  const budHist={};
  ROWS.forEach(r=>{if(accFilter&&!accFilter(r))return;const b=+(r.budget)||0;if(b>0){const rid=rowId(r);(budHist[rid]||(budHist[rid]={}))[r.date]=b}});
  // 정렬 기준용 '현재 일예산' 맵 (표시기간 밖 날짜까지 포함한 최신 예산)
  const BUD=curBudMap(ROWS,accFilter);
  let list=Object.values(byA).map(a=>{let s=0,rv=0,p=0,uc=0,mp=0,imp=0;d7.forEach(d=>{if(a.d[d]){s+=a.d[d].spend;rv+=a.d[d].revenue;p+=a.d[d].profit;uc+=a.d[d].unique_clicks;mp+=a.d[d].results_mp;imp+=(a.d[d].impressions||0)}});a._s=s;a._r=rv;a._p=p;a._roas=s>0?rv/s*100:0;a._cvr=uc>0&&mp>0?mp/uc*100:0;a._ctr=imp>0?uc/imp*100:0;a._cpm=imp>0?s/imp*1000:0;a._uc=uc;a._mp=mp;a._imp=imp;a._yS=a.d[yDay]?a.d[yDay].spend:0;a._bud=BUD[a.id]||0;return a});
  // 세트필터: 키워드 입력 시 캠페인/세트명/ID에 키워드가 포함된 세트만 표시 (종합·소계도 필터 결과 기준)
  const tKw=(document.getElementById(filterElId).value||'').trim().toLowerCase();
  if(tKw)list=list.filter(a=>((a.cn||'')+' '+(a.an||'')+' '+(a.id||'')).toLowerCase().includes(tKw));
  // ★ perf: 7일 + 어제 모두 지출 0 인 비활성 세트는 "개별 행"만 숨김 (DOM 부담 감소).
  //   단, 종합/소계/일별 합계는 전체 세트 기준으로 계산해야 추이차트(주간) 합과 일치한다.
  //   (예전엔 list 자체를 필터해 합계가 과거 구간에서 누락됐음)
  // 하이라이트가 걸려 있는 세트는 색 유지 위해 항상 표시.
  //   국내소재(cr)는 세트가 아니라 '소재' 단위라 30일 창에도 800~3000행이 잡혀 렌더가 수 초씩 걸렸다.
  //   → cr 에도 같은 숨김 규칙을 적용하되, 죽은 소재를 다시 보고 싶을 때가 있어 체크박스(#trHideDead)로 끌 수 있게 한다.
  {const _w=document.getElementById('trHideWrap');if(_w)_w.style.display=(MODE==='cr')?'':'none';}
  const _hideDead=(MODE==='kr'||MODE==='gl')||(MODE==='cr'&&(document.getElementById('trHideDead')||{}).checked!==false);
  const isHidden=a=>!tKw&&_hideDead&&!(a._s>0||a._yS>0||HIGHLIGHTS[a.id]);
  const ths=dd.map(d=>{const w=WD(d);const yd=d===yDay?' col-yday':'';
    // 메타 기준으로 그린 날짜는 헤더에 표시 — 날짜탭(Mixpanel)과 숫자가 다른 이유를 바로 알 수 있게
    const mm=(MODE==='gl'&&isGlMetaDay(d))?'<span title="이 날짜는 Mixpanel 대신 Meta 보고값(매출=지출×메타ROAS, 구매수=results_meta) 기준입니다" style="color:#1a73e8;font-size:9px">ᴹ</span>':'';
    return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')'+mm+'</th>'}).join('');
  const colSpan=dd.length+4+(showChg?2:0)+(showAcc?1:0);  // 4=캠페인/이름/ID/7일, showChg(kr·gl)시 예산+메모 2칸(증감 컬럼 제거), showAcc시 광고계정 1칸
  // Summary
  const totD={};dd.forEach(d=>{let s=0,r=0,p=0,mp=0,uc=0,imp=0;list.forEach(a=>{if(a.d[d]){s+=a.d[d].spend;r+=a.d[d].revenue;p+=a.d[d].profit;mp+=a.d[d].results_mp;uc+=a.d[d].unique_clicks;imp+=(a.d[d].impressions||0)}});totD[d]={s,r,p,mp,uc,imp}});
  const ts=d7.reduce((a,d)=>a+(totD[d]?.s||0),0),tr=d7.reduce((a,d)=>a+(totD[d]?.r||0),0),tp=tr-ts,troas=ts>0?tr/ts*100:0;
  const tmp=d7.reduce((a,d)=>a+(totD[d]?.mp||0),0),tuc=d7.reduce((a,d)=>a+(totD[d]?.uc||0),0),tcvr=tuc>0&&tmp>0?tmp/tuc*100:0;
  const timp=d7.reduce((a,d)=>a+(totD[d]?.imp||0),0),tcpm=timp>0?ts/timp*1000:0,tctr=timp>0?tuc/timp*100:0;
  const chgTh='';  // 증감 컬럼 제거(국내·글로벌 추이차트)
  const memoTh=showChg?'<th class="h-memo hmemo">메모</th>':'';
  // 예산 컬럼(세트ID ↔ 메모 사이) — 값은 정렬(💸 예산순)·증감 테두리가 쓰는 것과 같은
  //   '현재 예산' 스냅샷(curBudMap = 세트별 최신 보유일의 budget). 날짜별 값이 아니다.
  //   종합·소계 칸은 비워둔다: CBO 캠페인은 같은 예산이 소속 세트마다 반복돼 세로합이 뻥튀기된다.
  const budTh=showChg?'<th class="hbud" title="현재 일예산(각 세트 최신일 스냅샷) — 표시 기간과 무관하게 지금 값. CBO 캠페인은 세트마다 같은 값이 반복되므로 세로로 더하지 말 것">예산</th>':'';
  const accTh=showAcc?'<th class="hacc" style="text-align:left;white-space:nowrap">광고 계정</th>':'';
  const accTdSr=showAcc?'<td class="fx fxa" style="background:#e8e8e8"></td>':'';  // 종합·소계 행의 빈 계정칸
  let h='<thead><tr>'+accTh+'<th class="hcn" style="text-align:left;white-space:nowrap">캠페인</th><th class="han" style="text-align:left;white-space:nowrap">'+rowNameLabel()+'</th><th class="hid">'+rowIdLabel()+'</th>'+budTh+chgTh+memoTh+'<th>7일</th>'+ths+'</tr></thead><tbody>';
  const legend=AUX?'<div class="r">CTR</div><div class="cv">CVR</div><div class="cm">CPM</div><div class="s">구매당비용</div>':'<div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출금액</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div><div class="cm">CPM</div><div class="cpa">구매당비용</div>';
  h+='<tr class="sr">'+accTdSr+'<td class="fx fx0" style="background:#e8e8e8">종합</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8">'+legend+'</td>'+(showChg?'<td style="background:#e8e8e8"></td><td style="background:#e8e8e8"></td>':'')+'<td class="mc '+RC(troas)+'">'+(AUX?MCAUX(ts,tr,tuc,tmp,timp):MC(troas,tp,ts,tr,tcvr,tcpm,tctr,tmp>0?ts/tmp:0))+'</td>';
  dd.forEach(d=>{const x=totD[d];const yd=d===yDay?' col-yday':'';const roas=x.s>0?x.r/x.s*100:0;const cvr=x.uc>0&&x.mp>0?x.mp/x.uc*100:0;const cpm=x.imp>0?x.s/x.imp*1000:0;const ctr=x.imp>0?x.uc/x.imp*100:0;h+='<td class="mc '+RC(roas)+yd+'">'+(AUX?MCAUX(x.s,x.r,x.uc,x.mp,x.imp):MC(roas,x.p,x.s,x.r,cvr,cpm,ctr,x.mp>0?x.s/x.mp:0))+'</td>'});
  h+='</tr>';
  // Group by product
  const byProd={};list.forEach(a=>{const p=a.product||'기타';if(!byProd[p])byProd[p]={adsets:[],yS:0,bud:0};byProd[p].adsets.push(a);byProd[p].yS+=a._yS;byProd[p].bud+=(a._bud||0)});
  // 정렬은 상단 드롭다운(💸예산순 / 지출순 / 카테고리)이 결정 — orderProdKeys·orderSets 참고.
  orderProdKeys(byProd).forEach(prod=>{
    const g=byProd[prod];
    const pS=g.adsets.reduce((a,x)=>a+x._s,0),pR=g.adsets.reduce((a,x)=>a+x._r,0),pRoas=pS>0?pR/pS*100:0;
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.adsets.length+'개) 전날 '+money(g.yS)+' · 7일 ROAS '+pRoas.toFixed(0)+'%</td></tr>';
    // Product subtotal row
    const pDaily={};dd.forEach(d=>{let s=0,r=0,p=0,mp=0,uc=0,imp=0;g.adsets.forEach(a=>{if(a.d[d]){s+=a.d[d].spend;r+=a.d[d].revenue;p+=a.d[d].profit;mp+=a.d[d].results_mp;uc+=a.d[d].unique_clicks;imp+=(a.d[d].impressions||0)}});pDaily[d]={s,r,p,mp,uc,imp,roas:s>0?r/s*100:0,cvr:uc>0&&mp>0?mp/uc*100:0,ctr:imp>0?uc/imp*100:0,cpm:imp>0?s/imp*1000:0}});
    const pImp=dd.reduce((a,d)=>a+(pDaily[d]?.imp||0),0);
    const pUc=g.adsets.reduce((a,x)=>a+(x._uc||0),0),pMp=g.adsets.reduce((a,x)=>a+(x._mp||0),0);
    const pCpm=pImp>0?pS/pImp*1000:0;
    const pCells=dd.map(d=>{const t=pDaily[d];const yd=d===yDay?' col-yday':'';return t&&t.s?'<td class="mc '+RC(t.roas)+yd+'">'+(AUX?MCAUX(t.s,t.r,t.uc,t.mp,t.imp):MC(t.roas,t.p,t.s,t.r,t.cvr,t.cpm,t.ctr,t.mp>0?t.s/t.mp:0))+'</td>':'<td class="'+yd+'"></td>'}).join('');
    h+='<tr class="sr">'+accTdSr+'<td class="fx fx0" style="background:#e8e8e8">'+prod+' 소계</td><td class="fx fx1" style="background:#e8e8e8"></td><td></td>'+(showChg?'<td></td><td></td>':'')+'<td class="mc '+RC(pRoas)+'">'+(AUX?MCAUX(pS,pR,pUc,pMp,pImp):MC(pRoas,pR-pS,pS,pR,0,pCpm,null,pMp>0?pS/pMp:0))+'</td>'+pCells+'</tr>';
    // Individual adsets (비활성 세트는 행만 생략 — 위 소계/종합엔 이미 포함됨)
    //   '카테고리' 모드에서만 원본 밑에 변형을 계보로 붙인다(그 외엔 예산·지출 순 한 줄 나열).
    orderSets(g.adsets.filter(a=>!isHidden(a))).forEach(a=>{
      // 세트별 예산 변화 테두리 색 맵: 전체 히스토리(budHist)를 오름차순으로 전일(직전 보유일) 대비 비교.
      //   테두리는 표시기간(dd) 내 날짜에만 그리되, 비교 기준일은 dd 밖(직전일)도 허용 → 첫 열 증감도 표시.
      const budBc={};{const bh=budHist[a.id]||{};let pv=null;Object.keys(bh).sort().forEach(d=>{const b=bh[d];if(pv>0&&b!==pv){const k=budBorderKey((b-pv)/pv*100);if(k&&dd.includes(d))budBc[d]=HL_CONFIG[k].bg}pv=b})}
      const cells=dd.map(d=>{const r=a.d[d];const yd=d===yDay?' col-yday':'';const mk=d===today?(HIGHLIGHTS[a.id]||(r&&r.highlight)):(r&&r.highlight);let bcol=budBc[d];if(!bcol&&mk==='off')bcol=HL_CONFIG.off.bg;const cb=(showChg&&bcol)?' style="box-shadow:inset 0 0 0 3px '+bcol+'"':'';if(!r||!r.spend)return'<td class="'+yd+'"'+cb+'></td>';const cpm=r.impressions>0?r.spend/r.impressions*1000:0;const ctr=r.impressions>0?(r.unique_clicks||0)/r.impressions*100:0;return'<td class="mc '+RC(r.roas)+yd+(hrOK?' hr-cell':'')+'"'+(hrOK?' data-hd="'+d+'"':'')+cb+'>'+(AUX?MCAUX(r.spend,r.revenue,(r.unique_clicks||0),(r.results_mp||0),(r.impressions||0)):MC(r.roas,r.profit,r.spend,r.revenue,r.cvr,cpm,ctr,r.results_mp>0?r.spend/r.results_mp:0))+'</td>'}).join('');
      const hl=hlClass(a.id);const ck=' clickable" data-id="'+a.id+'" onclick="showCP(\''+a.id+'\',this)"';
      const chgTd='';  // 증감 컬럼 제거(국내·글로벌 추이차트)
      const budTd=showChg?'<td class="budc">'+(a._bud>0?money(a._bud):'')+'</td>':'';
      // 메모(추이차트): '오늘' 칸에 쓰면 오늘 날짜로 durable(daily_memos) 저장 → 0시가 지나도 남는다.
      //  하루가 지나면 그 메모는 아래 이력으로 내려가고 앞에 날짜(MM/DD)가 붙는다.
      //  날짜탭 perfTbl.memo·하이라이트 테이블에도 함께 써서 기존 리더(봇 등)와 호환.
      let memoTd='';
      if(showChg){
        const mmv=_mEsc(DMEMO[_dmKey(MODE,today,a.id)]||'');
        memoTd='<td class="memo-cell"><textarea class="memo-input" rows="3" placeholder="오늘 메모" data-date="'+today+'" data-id="'+a.id+'" onkeydown="if(event.key===\'Enter\'&&(event.ctrlKey||event.metaKey)){event.preventDefault();this.blur()}" onblur="saveTrendMemo(this.dataset.date,this.dataset.id,this.value,this)">'+mmv+'</textarea><span class="memo-saved">✓</span>'+_memoHistHtml(a.id,today)+'</td>';
      }
      // KR/GL/VN: caret 클릭 → 인라인 소재 목록 펼침
      let caretBtn='';
      if(MODE==='kr'||MODE==='gl'||MODE==='vn'){
        caretBtn='<span class="ex-caret" onclick="event.stopPropagation();toggleAdsetCreatives(\''+a.id+'\',this.closest(\'tr\'))" title="소재 목록 펼치기/접기">▶</span>';
      }
      const anm=accName(a.acc).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
      // 컬럼을 좁게 줄여 이름이 …로 잘려도 마우스를 올리면 전체 이름이 보이도록
      const cnT=abEsc(a.cn||'').replace(/"/g,'&quot;'),anT=abEsc(a.an||'').replace(/"/g,'&quot;');
      const accTd=showAcc?'<td class="fx fxa '+hl+ck+' title="'+anm+'">'+anm+'</td>':'';
      h+='<tr data-adset-row="'+a.id+'" data-acc="'+(a.acc||'')+'"'+(a._dvHead?' data-dvfam="'+a._dvHead+'"':'')+'>'+accTd+'<td class="fx fx0 '+hl+ck+' title="'+cnT+'">'+(a.cn||'')+'</td><td class="fx fx1 '+hl+ck+' title="'+anT+'">'+(a._dvChild?'<span class="dv-sub">└</span>':'')+famBtn(a)+caretBtn+(a.an||'')+'</td><td class="idc '+hl+ck+' style="font-size:9px">'+a.id+'</td>'+chgTd+budTd+memoTd+'<td class="mc '+RC(a._roas)+'">'+(AUX?MCAUX(a._s,a._r,a._uc,a._mp,a._imp):MC(a._roas,a._p,a._s,a._r,a._cvr,a._cpm,a._ctr,a._mp>0?a._s/a._mp:0))+'</td>'+cells+'</tr>';
    });
  });
  const tblEl=document.getElementById(TBL);
  h+='</tbody>';tblEl.innerHTML=h;
  tblEl.dataset.daysEl=daysElId; // 소재펼침(caret)이 올바른 기간 컨트롤을 읽도록
  requestAnimationFrame(()=>{_fitNameCols(tblEl);_initColResize(tblEl);_fixSticky(tblEl)});
}
// 캠페인·세트 컬럼 폭을 '세트 행의 가장 긴 이름'에 딱 맞춘다(--fit-cn/--fit-an CSS 변수).
//   소재를 펼치면 소재명이 세트 컬럼(fx1)에 들어가는데, 소재명은 세트명보다 훨씬 길어
//   컬럼이 그만큼 늘어난 채 남았다 → 소재 행은 이 폭에서 …로 잘리고(전체 이름은 title 툴팁)
//   컬럼은 세트 이름 기준으로만 넓어진다. 접힌 행(display:none)은 폭 계산에서 빠진다.
function _fitNameCols(tblEl){
  if(!tblEl||!tblEl.offsetParent)return;   // 탭이 안 열려 있으면 측정 불가 → 건너뜀
  const rows=tblEl.querySelectorAll('tbody tr:not(.creative-expanded)');
  if(!rows.length)return;
  const rng=document.createRange();
  //   box-sizing:border-box 라서 max-width 는 패딩·테두리까지 포함한 값이어야 글자가 안 잘린다.
  const widest=cls=>{
    let m=0;
    rows.forEach(tr=>{
      const td=tr.querySelector('td.'+cls);
      if(!td)return;
      rng.selectNodeContents(td);
      const txt=rng.getBoundingClientRect().width;
      if(!txt)return;
      const cs=getComputedStyle(td);
      const pad=parseFloat(cs.paddingLeft)+parseFloat(cs.paddingRight)+parseFloat(cs.borderLeftWidth)+parseFloat(cs.borderRightWidth);
      const w=txt+(pad||11);
      if(w>m)m=w;
    });
    return m;
  };
  //   사용자가 헤더를 드래그해 정한 폭(COLW)이 있으면 그 값이 자동 맞춤보다 우선한다.
  const use=(vr,c,auto)=>{const u=COLW[_colwKey(tblEl,c)];const px=u>0?u:auto;if(px>0)tblEl.style.setProperty(vr,px+'px')};
  use('--fit-acc','acc',0);                                  // 계정 컬럼은 지정 없으면 CSS 기본(120px)
  use('--fit-cn','cn',Math.ceil(widest('fx0'))+1);           // +1 = 소수점 반올림 여유(말줄임 방지)
  use('--fit-an','an',Math.ceil(widest('fx1'))+1);
  use('--fit-id','id',0);                                    // ID·메모는 자동맞춤 없이 기본폭(CSS 130px), 드래그로만 바뀜
  use('--fit-memo','memo',0);
}

// ===== 컬럼 폭 드래그 조절 (스프레드시트처럼) =====
//   헤더 오른쪽 끝 얇은 손잡이를 끌면 광고계정·캠페인·세트 컬럼 폭이 바뀐다.
//   폭은 모드·테이블별로 localStorage 에 남고(새로고침해도 유지), 손잡이를 더블클릭하면
//   저장값을 지우고 다시 '가장 긴 이름 자동 맞춤'으로 돌아간다.
const COLW_KEY='ntc_colw_v1';
let COLW={};try{COLW=JSON.parse(localStorage.getItem(COLW_KEY)||'{}')||{}}catch(e){COLW={}}
function _colwSave(){try{localStorage.setItem(COLW_KEY,JSON.stringify(COLW))}catch(e){}}
function _colwKey(tblEl,c){return MODE+'|'+tblEl.id+'|'+c}
const RZ_COLS=[['hacc','--fit-acc','acc'],['hcn','--fit-cn','cn'],['han','--fit-an','an'],['hid','--fit-id','id'],['hmemo','--fit-memo','memo']];
function _initColResize(tblEl){
  if(!tblEl)return;
  RZ_COLS.forEach(cfg=>{
    const cls=cfg[0],vr=cfg[1],c=cfg[2];
    const th=tblEl.querySelector('thead th.'+cls);
    if(!th||th.querySelector('.col-rz'))return;
    const h=document.createElement('span');
    h.className='col-rz';
    h.title='드래그: 컬럼 폭 조절 · 더블클릭: 자동 맞춤으로 되돌리기';
    h.addEventListener('mousedown',e=>_colRzStart(e,tblEl,vr,_colwKey(tblEl,c)));
    h.addEventListener('dblclick',e=>{
      e.preventDefault();e.stopPropagation();
      delete COLW[_colwKey(tblEl,c)];_colwSave();
      tblEl.style.removeProperty(vr);_fitNameCols(tblEl);_fixSticky(tblEl);
    });
    th.appendChild(h);
  });
}
function _colRzStart(e,tblEl,vr,key){
  e.preventDefault();e.stopPropagation();
  const th=e.target.parentNode;
  const x0=e.clientX,w0=th.getBoundingClientRect().width;
  let raf=0;
  const move=ev=>{
    const w=Math.max(40,Math.round(w0+ev.clientX-x0));   // 40px 미만으로는 못 줄임
    tblEl.style.setProperty(vr,w+'px');
    COLW[key]=w;
    if(!raf)raf=requestAnimationFrame(()=>{raf=0;_fixSticky(tblEl)});   // 고정컬럼 오프셋 즉시 추종
  };
  const up=()=>{
    document.removeEventListener('mousemove',move);
    document.removeEventListener('mouseup',up);
    document.body.style.cursor='';document.body.style.userSelect='';
    _colwSave();_fixSticky(tblEl);
  };
  document.addEventListener('mousemove',move);
  document.addEventListener('mouseup',up);
  document.body.style.cursor='col-resize';document.body.style.userSelect='none';
}
// sticky 좌측 컬럼 left 오프셋 재계산.
//   컬럼 순서: [광고 계정(fxa)] · 캠페인(fx0) · 세트(fx1) — 계정 컬럼 유무에 따라 fx0/fx1 시작점이 달라진다.
//   scope: 특정 <tr> 들만 보정하고 싶을 때(소재 펼침 행) 전달, 없으면 테이블 전체.
function _fixSticky(tblEl,scope){
  if(!tblEl)return;
  const fx0=tblEl.querySelector('tbody td.fx0');
  if(!fx0)return;
  const fxa=tblEl.querySelector('tbody td.fxa');
  const aw=fxa?fxa.offsetWidth:0;
  const w0=fx0.offsetWidth;
  const targets=scope&&scope.length?scope:[tblEl];
  targets.forEach(el=>{
    el.querySelectorAll('td.fxa').forEach(td=>td.style.left='0px');
    el.querySelectorAll('td.fx0').forEach(td=>td.style.left=aw+'px');
    el.querySelectorAll('td.fx1').forEach(td=>td.style.left=(aw+w0)+'px');
  });
}

// 🇹🇼 대만 추이차트 — 밴스드 대만 계정(VN_TW_ACC)만, 별도 테이블(vntwTbl)에 동일 포맷으로 렌더
function renderVnTwTrend(){
  renderTrend({tableId:'vntwTbl',daysElId:'tDaysTw',filterElId:'tFilterTw',accFilter:r=>String(r.ad_account_id||'')===VN_TW_ACC});
}
// USD/KRW 일별 환율맵 (Stripe global_stripe_daily 의 usd_krw_rate). 누락일은 직전 영업일로 폴백.
function usdKrwRateAt(date){
  const m={};(STRIPE_DATA||[]).forEach(r=>{if(r.usd_krw_rate&&!m[r.date])m[r.date]=+r.usd_krw_rate});
  if(m[date])return m[date];
  const ds=Object.keys(m).sort();
  if(!ds.length)return 1380; // 안전 폴백
  let best=null;for(const d of ds){if(d<=date)best=d}
  return m[best||ds[ds.length-1]]||1380;
}
// 밴스드 대만(VN_TW_ACC) 행을 KRW→USD 로 환산한 사본 반환 (글로벌 추이/매출에 합치기용).
// spend/revenue/profit/budget 만 통화 환산, roas/cvr/노출/클릭 등 비율·카운트는 그대로.
function vnTwUsdRows(){
  return (VN_AD||[]).filter(r=>String(r.ad_account_id||'')===VN_TW_ACC).map(r=>{
    const rate=usdKrwRateAt(r.date)||1380;
    const spend=(+r.spend||0)/rate, revenue=(+r.revenue||0)/rate;
    return {...r, spend, revenue, profit:revenue-spend, budget:(+r.budget||0)/rate};
  });
}
// 추이차트(메인) 라우팅. 글로벌 모드도 타이트 글로벌(AD)만 표시 —
// 대만 구글(밴스드 운영) 일자별 지출·귀속매출을 KRW→USD 로 환산해 돌려준다. {date:{s,r}}
//   소스=google_campaign_daily(GCAMP, KRW). 대만(country='TW') 행은 검색광고·디멘드젠·기타 전부
//   밴스드 운영이라(매출탭 채널별 G_TW_ALL 정의와 동일) country 필터만으로 충분하다.
//   GCAMP 는 지연로드라 아직 없으면 빈 객체 — 호출부에서 ensureBigTable('gcamp') 후 재렌더한다.
function gTwUsdByDate(){
  const out={};
  (GCAMP||[]).forEach(r=>{
    if(String(r.country||'')!=='TW')return;
    const rate=usdKrwRateAt(r.date)||1380;
    const o=out[r.date]||(out[r.date]={s:0,r:0});
    o.s+=(+r.spend||0)/rate;o.r+=(+r.revenue||0)/rate;
  });
  return out;
}
// 밴스드 대만(VN_TW_ACC)은 여기서 제외하고 별도 탭(🇹🇼 대만 추이차트)에서만 본다.
function renderTrendMain(){
  const gran=(document.getElementById('tGran')||{}).value||'day';
  if(gran==='week'||gran==='month'){renderTrendAgg(gran);return}
  renderTrend();
}
// 추이차트 주별/월별 집계 뷰 — 일별(renderTrend)의 하이라이트·메모·예산테두리·소재펼침은
// 날짜 전용 기능이라 제외하고, 컬럼을 주(WM 월요일)/월(YYYY-MM)로 합산해 동일한 MC 셀로 렌더.
// 데이터원=AD(현재 모드), 기간=#tDays(일수)를 주/월 버킷으로 묶음, 세트필터=#tFilter 공용.
function renderTrendAgg(gran){
  const TBL='tTbl';
  const isWeek=gran==='week';
  const days=parseInt(document.getElementById('tDays').value);
  const dd=DATES.slice(0,days);
  const colKey=d=>isWeek?WM(d):d.slice(0,7);
  const cols=[...new Set(dd.map(colKey))];            // 최신순(좌측이 최근)
  const recentCol=cols[0];
  const wkLabel=wk=>{const m=new Date(wk);const s=new Date(m.getTime()+6*864e5);return(m.getMonth()+1)+'/'+m.getDate()+'~'+(s.getMonth()+1)+'/'+s.getDate()};
  const colLabel=ck=>isWeek?wkLabel(ck):(ck.slice(0,4)+'.'+ck.slice(5,7));
  // rowId 별 · 컬럼별 합산
  const byA={};
  AD.forEach(r=>{if(!dd.includes(r.date))return;const ck=colKey(r.date);const rid=rowId(r);
    if(!byA[rid])byA[rid]={cn:r.campaign_name,an:MODE==='cr'?(r.ad_name||''):(r.adset_name||''),id:rid,product:r.product,acc:r.ad_account_id||'',b:{}};
    const b=byA[rid].b;if(!b[ck])b[ck]={s:0,r:0,p:0,mp:0,uc:0,imp:0};
    b[ck].s+=r.spend;b[ck].r+=r.revenue;b[ck].p+=r.profit;b[ck].mp+=r.results_mp;b[ck].uc+=r.unique_clicks;b[ck].imp+=(r.impressions||0)});
  const BUD=curBudMap(AD);   // 정렬 기준용 현재 일예산
  let list=Object.values(byA).map(a=>{let s=0,r=0,p=0,uc=0,mp=0,imp=0;cols.forEach(ck=>{const b=a.b[ck];if(b){s+=b.s;r+=b.r;p+=b.p;uc+=b.uc;mp+=b.mp;imp+=b.imp}});
    a._s=s;a._r=r;a._p=p;a._roas=s>0?r/s*100:0;a._cvr=uc>0&&mp>0?mp/uc*100:0;a._ctr=imp>0?uc/imp*100:0;a._cpm=imp>0?s/imp*1000:0;a._uc=uc;a._mp=mp;a._imp=imp;
    a._recentS=a.b[recentCol]?a.b[recentCol].s:0;a._bud=BUD[a.id]||0;a._yS=a._recentS;return a});
  // 세트필터 (공용 #tFilter)
  const tKw=(document.getElementById('tFilter').value||'').trim().toLowerCase();
  if(tKw)list=list.filter(a=>((a.cn||'')+' '+(a.an||'')+' '+(a.id||'')).toLowerCase().includes(tKw));
  const ths=cols.map(ck=>'<th style="min-width:var(--cw)">'+colLabel(ck)+'</th>').join('');
  // 광고 계정 컬럼(캠페인 왼쪽) — 일별 뷰(renderTrend)와 동일하게 국내·글로벌만
  const showAcc=MODE==='kr'||MODE==='gl';
  const accTh=showAcc?'<th class="hacc" style="text-align:left;white-space:nowrap">광고 계정</th>':'';
  const accTdSr=showAcc?'<td class="fx fxa" style="background:#e8e8e8"></td>':'';
  // 예산 컬럼 — 일별 뷰(renderTrend)와 같은 자리(세트ID 오른쪽). 주/월엔 메모가 없어 그 다음이 '전체'.
  const showBud=MODE==='kr'||MODE==='gl';
  const budTh=showBud?'<th class="hbud" title="현재 일예산(각 세트 최신일 스냅샷) — 표시 기간과 무관하게 지금 값. CBO 캠페인은 세트마다 같은 값이 반복되므로 세로로 더하지 말 것">예산</th>':'';
  const budTdSr=showBud?'<td style="background:#e8e8e8"></td>':'';   // 종합·소계는 합산 금지라 빈칸
  const colSpan=cols.length+4+(showAcc?1:0)+(showBud?1:0);
  const cell=t=>{if(!t||!t.s)return'<td></td>';const roas=t.s>0?t.r/t.s*100:0;const cvr=t.uc>0&&t.mp>0?t.mp/t.uc*100:0;const cpm=t.imp>0?t.s/t.imp*1000:0;const ctr=t.imp>0?t.uc/t.imp*100:0;return'<td class="mc '+RC(roas)+'">'+MC(roas,t.p,t.s,t.r,cvr,cpm,ctr,t.mp>0?t.s/t.mp:0)+'</td>'};
  // 컬럼별 종합
  const totC={};cols.forEach(ck=>{let s=0,r=0,p=0,mp=0,uc=0,imp=0;list.forEach(a=>{const b=a.b[ck];if(b){s+=b.s;r+=b.r;p+=b.p;mp+=b.mp;uc+=b.uc;imp+=b.imp}});totC[ck]={s,r,p,mp,uc,imp}});
  const ts=cols.reduce((a,ck)=>a+totC[ck].s,0),tr=cols.reduce((a,ck)=>a+totC[ck].r,0),tp=tr-ts,troas=ts>0?tr/ts*100:0;
  const timp=cols.reduce((a,ck)=>a+totC[ck].imp,0),tcpm=timp>0?ts/timp*1000:0;
  const tmp=cols.reduce((a,ck)=>a+totC[ck].mp,0),tuc=cols.reduce((a,ck)=>a+totC[ck].uc,0),tcvr=tuc>0&&tmp>0?tmp/tuc*100:0,tctr=timp>0?tuc/timp*100:0;
  const legend='<div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출금액</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div><div class="cm">CPM</div><div class="cpa">구매당비용</div>';
  let h='<thead><tr>'+accTh+'<th class="hcn" style="text-align:left;white-space:nowrap">캠페인</th><th class="han" style="text-align:left;white-space:nowrap">'+rowNameLabel()+'</th><th class="hid">'+rowIdLabel()+'</th>'+budTh+'<th>전체</th>'+ths+'</tr></thead><tbody>';
  h+='<tr class="sr">'+accTdSr+'<td class="fx fx0" style="background:#e8e8e8">종합</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8">'+legend+'</td>'+budTdSr+'<td class="mc '+RC(troas)+'">'+MC(troas,tp,ts,tr,tcvr,tcpm,tctr,tmp>0?ts/tmp:0)+'</td>';
  cols.forEach(ck=>{h+=cell(totC[ck])});
  h+='</tr>';
  // 상품별 그룹
  const byProd={};list.forEach(a=>{const p=a.product||'기타';if(!byProd[p])byProd[p]={adsets:[],yS:0,bud:0};byProd[p].adsets.push(a);byProd[p].yS+=a._recentS;byProd[p].bud+=(a._bud||0)});
  // 정렬은 일별 뷰와 같은 드롭다운(💸예산순 / 지출순 / 카테고리)을 따른다. 여기선 '지출'=최근 기간 지출.
  orderProdKeys(byProd).forEach(prod=>{
    const g=byProd[prod];
    const pS=g.adsets.reduce((a,x)=>a+x._s,0),pR=g.adsets.reduce((a,x)=>a+x._r,0),pRoas=pS>0?pR/pS*100:0;
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.adsets.length+'개) · '+(isWeek?'전체주':'전체월')+' ROAS '+pRoas.toFixed(0)+'%</td></tr>';
    const pByCol={};cols.forEach(ck=>{let s=0,r=0,p=0,mp=0,uc=0,imp=0;g.adsets.forEach(a=>{const b=a.b[ck];if(b){s+=b.s;r+=b.r;p+=b.p;mp+=b.mp;uc+=b.uc;imp+=b.imp}});pByCol[ck]={s,r,p,mp,uc,imp}});
    const pImp=cols.reduce((a,ck)=>a+pByCol[ck].imp,0),pCpm=pImp>0?pS/pImp*1000:0;
    const pMp=g.adsets.reduce((a,x)=>a+(x._mp||0),0),pUc=g.adsets.reduce((a,x)=>a+(x._uc||0),0),pCvr=pUc>0&&pMp>0?pMp/pUc*100:0,pCtr=pImp>0?pUc/pImp*100:0;
    const pCells=cols.map(ck=>cell(pByCol[ck])).join('');
    h+='<tr class="sr">'+accTdSr+'<td class="fx fx0" style="background:#e8e8e8">'+prod+' 소계</td><td class="fx fx1" style="background:#e8e8e8"></td><td></td>'+budTdSr+'<td class="mc '+RC(pRoas)+'">'+MC(pRoas,pR-pS,pS,pR,pCvr,pCpm,pCtr,pMp>0?pS/pMp:0)+'</td>'+pCells+'</tr>';
    orderSets(g.adsets.slice()).forEach(a=>{
      const cells=cols.map(ck=>cell(a.b[ck])).join('');
      const hl=hlClass(a.id);const ck=' clickable" data-id="'+a.id+'" onclick="showCP(\''+a.id+'\',this)"';
      const anm=accName(a.acc).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
      // 컬럼을 좁게 줄여 이름이 …로 잘려도 마우스를 올리면 전체 이름이 보이도록
      const cnT=abEsc(a.cn||'').replace(/"/g,'&quot;'),anT=abEsc(a.an||'').replace(/"/g,'&quot;');
      const accTd=showAcc?'<td class="fx fxa '+hl+ck+' title="'+anm+'">'+anm+'</td>':'';
      h+='<tr'+(a._dvHead?' data-dvfam="'+a._dvHead+'"':'')+'>'+accTd+'<td class="fx fx0 '+hl+ck+' title="'+cnT+'">'+(a.cn||'')+'</td><td class="fx fx1 '+hl+ck+' title="'+anT+'">'+(a._dvChild?'<span class="dv-sub">└</span>':'')+famBtn(a)+(a.an||'')+'</td><td class="idc '+hl+ck+' style="font-size:9px">'+a.id+'</td>'+(showBud?'<td class="budc">'+(a._bud>0?money(a._bud):'')+'</td>':'')+'<td class="mc '+RC(a._roas)+'">'+MC(a._roas,a._p,a._s,a._r,a._cvr,a._cpm,a._ctr,a._mp>0?a._s/a._mp:0)+'</td>'+cells+'</tr>';
    });
  });
  const tblEl=document.getElementById(TBL);
  h+='</tbody>';tblEl.innerHTML=h;
  requestAnimationFrame(()=>{_fitNameCols(tblEl);_initColResize(tblEl);_fixSticky(tblEl)});
}
// 보조지표 — 추이차트와 동일 구조(국내탭), 셀만 CTR/CVR/CPM/CPP/구매당단가로 렌더
function renderAux(){
  renderTrend({tableId:'axTbl',daysElId:'axDays',filterElId:'axFilter',metric:'aux'});
}
// 하이라이트/메모 변경 후 재렌더 — 대만 추이차트 탭이 떠 있으면 그 테이블을, 아니면 기본 추이차트를 갱신
function rerenderTrendView(){
  // 구글 디멘드젠 탭이 열려 있으면 그쪽을 갱신 — 하이라이트 전체삭제·0시 자동삭제가 즉시 반영되도록.
  const at=document.querySelector('.tab.active');
  if(at&&at.dataset.t==='ggdgkr'){renderGgdgTight();return}
  const tw=document.getElementById('p-vntwtrend');
  if(tw&&tw.classList.contains('active'))renderVnTwTrend();else renderTrendMain();
}

// ===== TREND: Toggle 소재 (creatives) under a 세트 row (KR/GL/VN) =====
// 클릭 시 해당 세트의 소재만 Supabase에서 직접 fetch (CR_AD 미사용)
// → 클릭 즉시 lag 없음. 대기시간은 fetch 완료까지(~200ms)
async function toggleAdsetCreatives(adsetId, anchorRow){
  if(MODE!=='kr'&&MODE!=='gl'&&MODE!=='vn')return;
  const tbody=anchorRow.parentNode;
  const sel='tr.creative-expanded[data-parent="'+CSS.escape(adsetId)+'"]';
  const existing=tbody.querySelectorAll(sel);
  const caret=anchorRow.querySelector('.ex-caret');
  if(existing.length){
    existing.forEach(r=>r.remove());
    if(caret)caret.textContent='▶';
    delete anchorRow.dataset.crAll;
    return;
  }
  // 클릭 즉시 caret 변경 → 브라우저에 양보 → 사용자에 응답성 보장
  if(caret)caret.textContent='⌛';
  await new Promise(r=>requestAnimationFrame(r));
  let records;
  // KR: ad_creative_daily, GL: global_ad_creative_daily (USD 필드 정규화), VN: vanced_ad_creative_daily
  const tbl=MODE==='gl'?'global_ad_creative_daily':(MODE==='vn'?'vanced_ad_creative_daily':'ad_creative_daily');
  const isGL=MODE==='gl';
  // 1) KR 만 prefetch 인덱스 활용. VN/GL 은 항상 per-adset 직접 fetch (DB ground truth).
  const bigKey=MODE==='kr'?'cr':null;
  const freshLoaded=bigKey && window._BIG_LOADED && window._BIG_LOADED[bigKey];
  const prefetchSrc=freshLoaded?CR_AD:null;
  const prefetchKey=MODE==='kr'?'_CR_BY_ADSET':null;
  if(prefetchSrc&&prefetchSrc.length){
    if(!window[prefetchKey]){
      const idx={};
      for(let i=0;i<prefetchSrc.length;i++){
        const r=prefetchSrc[i];const k=r.adset_id;if(!k)continue;
        (idx[k]||(idx[k]=[])).push(r);
      }
      window[prefetchKey]=idx;
    }
    records=window[prefetchKey][adsetId]||[];
  } else {
    // 2) per-adset fetch (in-flight 중복 방지) — KR prefetch 미완료 또는 GL 모드
    const cacheKey=tbl+'::'+adsetId;
    if(!window._CR_PER_ADSET)window._CR_PER_ADSET={};
    let entry=window._CR_PER_ADSET[cacheKey];
    if(entry===undefined){
      const D90=_dateCutoff(215);
      const url=SB_URL+'/rest/v1/'+tbl+'?select=*&order=date.desc&adset_id=eq.'+encodeURIComponent(adsetId)+'&date=gte.'+D90+'&limit=100000';
      const p=fetch(url,{headers:SBH}).then(r=>r.json()).catch(()=>[]);
      window._CR_PER_ADSET[cacheKey]=p;
      records=await p;
      window._CR_PER_ADSET[cacheKey]=records;
    } else if(entry&&typeof entry.then==='function'){
      records=await entry;
    } else {
      records=entry;
    }
  }
  // GL 모드: spend_usd/revenue_usd/profit_usd → spend/revenue/profit 으로 정규화
  if(isGL&&records&&records.length){
    records=records.map(r=>({...r,spend:r.spend_usd,revenue:r.revenue_usd,profit:r.profit_usd,product:GL_PRODUCT_CANON[r.product]||r.product}));
  }
  // 현재 추이차트 날짜 범위 (세트 row 와 동일하게) — 앵커 테이블의 기간 컨트롤 사용(대만 탭 등 분리 뷰 대응)
  const _daysElId=(anchorRow.closest('table')&&anchorRow.closest('table').dataset.daysEl)||'tDays';
  const days=parseInt(document.getElementById(_daysElId).value);
  const dd=DATES.slice(0,days);const d7=dd.slice(0,7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  const showChg=(MODE==='kr'||MODE==='gl');
  const showAcc=(MODE==='kr'||MODE==='gl');   // 세트 행과 컬럼 수를 맞추기 위한 빈 '광고 계정' 칸
  const ddSet=new Set(dd);
  const byC={};
  for(let i=0;i<records.length;i++){
    const r=records[i];
    if(!ddSet.has(r.date))continue;
    const cid=r.ad_id;
    if(!byC[cid])byC[cid]={an:r.ad_name||'',id:cid,acc:r.ad_account_id||'',d:{}};
    byC[cid].d[r.date]=r;
  }
  const list=Object.values(byC);
  list.forEach(c=>{
    let s=0,rv=0,p=0,uc=0,mp=0,imp=0;
    d7.forEach(d=>{const r=c.d[d];if(r){s+=r.spend||0;rv+=r.revenue||0;p+=r.profit||0;uc+=r.unique_clicks||0;mp+=r.results_mp||0;imp+=(r.impressions||0);}});
    c._s=s;c._r=rv;c._p=p;c._mp=mp;
    c._roas=s>0?rv/s*100:0;
    c._cvr=(uc>0&&mp>0)?mp/uc*100:0;
    c._ctr=imp>0?uc/imp*100:0;
  });
  list.sort((a,b)=>(b._s||0)-(a._s||0));
  const fullCount=list.length;
  const displayList=list;
  const hidden=0;
  const totalCols=anchorRow.cells.length;
  if(!list.length){
    const tr=document.createElement('tr');
    tr.className='creative-expanded';
    tr.setAttribute('data-parent',adsetId);
    tr.innerHTML='<td colspan="'+totalCols+'" style="padding:8px 24px;background:#fff7e6;color:#999;font-size:11px;font-style:italic">↳ 소재 데이터 없음 ('+tbl+' 미수집 또는 해당 기간 미운영)</td>';
    anchorRow.after(tr);
    if(caret)caret.textContent='▼';
    return;
  }
  // 소재 소계: 일자별 합산
  const subDaily={};
  dd.forEach(d=>{let s=0,r=0,p=0,uc=0,mp=0,imp=0;
    list.forEach(c=>{const x=c.d[d];if(x){s+=x.spend||0;r+=x.revenue||0;p+=x.profit||0;uc+=x.unique_clicks||0;mp+=x.results_mp||0;imp+=(x.impressions||0);}});
    subDaily[d]={s,r,p,mp,roas:s>0?r/s*100:0,cvr:uc>0&&mp>0?mp/uc*100:0,ctr:imp>0?uc/imp*100:0}});
  let tS=0,tR=0,tP=0,tUc=0,tMp=0,tImp=0;
  list.forEach(c=>{tS+=c._s;tR+=c._r;tP+=c._p;
    d7.forEach(d=>{const r=c.d[d];if(r){tUc+=r.unique_clicks||0;tMp+=r.results_mp||0;tImp+=(r.impressions||0);}});
  });
  const tRoas=tS>0?tR/tS*100:0,tCvr=(tUc>0&&tMp>0)?tMp/tUc*100:0,tCtr=tImp>0?tUc/tImp*100:0;
  const trs=[];
  // 1) 소재 소계 row — 세트 row 와 동일한 컬럼 구조
  {
    const tr=document.createElement('tr');
    tr.className='creative-expanded cr-subtotal sr';
    tr.setAttribute('data-parent',adsetId);
    const subLabel='↳ 소재 소계 ('+fullCount+'개)';
    const subCells=dd.map(d=>{const t=subDaily[d];const yd=d===yDay?' col-yday':'';return t&&t.s?'<td class="mc '+RC(t.roas)+yd+'" style="background:#fff3cd">'+MC(t.roas,t.p,t.s,t.r,t.cvr,null,t.ctr,t.mp>0?t.s/t.mp:0)+'</td>':'<td class="'+yd+'" style="background:#fff3cd"></td>'}).join('');
    tr.innerHTML=(showAcc?'<td class="fx fxa" style="background:#fff3cd"></td>':'')
      +'<td class="fx fx0" style="background:#fff3cd;font-size:10px"><b>'+subLabel+'</b></td>'
      +'<td class="fx fx1" style="background:#fff3cd"></td>'
      +'<td style="background:#fff3cd"></td>'
      +(showChg?'<td style="background:#fff3cd"></td><td style="background:#fff3cd"></td>':'')
      +'<td class="mc '+RC(tRoas)+'" style="background:#fff3cd">'+MC(tRoas,tP,tS,tR,tCvr,null,tCtr,tMp>0?tS/tMp:0)+'</td>'
      +subCells;
    trs.push(tr);
  }
  // 2) 개별 소재 rows — 세트 row 와 동일한 컬럼 구조
  displayList.forEach(c=>{
    const tr=document.createElement('tr');
    tr.className='creative-expanded';
    tr.setAttribute('data-parent',adsetId);
    const anEsc=(c.an||'').replace(/"/g,'&quot;');
    const accNum=String(c.acc||'').replace(/^act_/,'');
    const idCell=accNum
      ? '<a href="https://adsmanager.facebook.com/adsmanager/manage/ads/edit/standalone?act='+accNum+'&selected_ad_ids='+c.id+'&nav_source=no_referrer" target="_blank" rel="noopener noreferrer" style="color:#1877f2;text-decoration:none" title="광고 (Meta Ads Manager) — 좌측 상단 검토 탭 클릭" onclick="event.stopPropagation()">'+c.id+' 👁</a>'
      : c.id;
    const cells=dd.map(d=>{const r=c.d[d];const yd=d===yDay?' col-yday':'';if(!r||!r.spend)return'<td class="'+yd+'" style="background:#fff8e1"></td>';const ctr=r.impressions>0?(r.unique_clicks||0)/r.impressions*100:0;return'<td class="mc '+RC(r.roas)+yd+'" style="background:#fff8e1">'+MC(r.roas,r.profit,r.spend,r.revenue,r.cvr,null,ctr,r.results_mp>0?r.spend/r.results_mp:0)+'</td>'}).join('');
    tr.innerHTML=(showAcc?'<td class="fx fxa" style="background:#fff8e1"></td>':'')
      +'<td class="fx fx0" style="background:#fff8e1"></td>'
      +'<td class="fx fx1" style="background:#fff8e1;padding-left:24px" title="'+anEsc+'"><span style="color:#888">┗</span> '+(c.an||'')+'</td>'
      +'<td class="idc" style="font-size:9px;background:#fff8e1">'+idCell+'</td>'
      +(showChg?'<td style="background:#fff8e1"></td><td style="background:#fff8e1"></td>':'')
      +'<td class="mc '+RC(c._roas)+'" style="background:#fff8e1">'+MC(c._roas,c._p,c._s,c._r,c._cvr,null,c._ctr,c._mp>0?c._s/c._mp:0)+'</td>'
      +cells;
    trs.push(tr);
  });
  const frag=document.createDocumentFragment();
  for(let i=0;i<trs.length;i++)frag.appendChild(trs[i]);
  anchorRow.parentNode.insertBefore(frag,anchorRow.nextSibling);
  // sticky fx1 의 left 오프셋을 세트 row 와 동일하게 맞춤 — 앵커 테이블 기준(대만 탭 등 분리 뷰 대응)
  const _tbl=anchorRow.closest('table');
  _fixSticky(_tbl||document.getElementById('tTbl'),trs);
  if(caret)caret.textContent='▼';
}

// 전체 소재 보기 — '더 보기' 클릭 시 호출
function showAllCreatives(adsetId){
  const row=document.querySelector('tr[data-adset-row="'+CSS.escape(adsetId)+'"]');
  if(!row)return;
  row.dataset.crAll='1';
  // 기존 expansion 제거 후 재펼침
  document.querySelectorAll('tr.creative-expanded[data-parent="'+CSS.escape(adsetId)+'"]').forEach(r=>r.remove());
  toggleAdsetCreatives(adsetId,row);
}

// ===== TREND PRODUCT (추이차트_상품별) =====
function renderTrendProduct(){
  const unit=(document.getElementById('tpUnit')||{}).value||'day';
  const isWeek=unit==='week';
  const days=parseInt(document.getElementById('tpDays').value);
  const dd=DATES.slice(0,days);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  const wkLabel=wk=>{const m=new Date(wk);const s=new Date(m.getTime()+6*864e5);return(m.getMonth()+1)+'/'+m.getDate()+'~'+(s.getMonth()+1)+'/'+s.getDate()};
  const colKey=d=>isWeek?WM(d):d;
  const cols=[...new Set(dd.map(colKey))];          // 최신순(좌측이 최근). 일간=날짜, 주간=주(월) 키
  const sumCols=isWeek?cols:cols.slice(0,7);         // 요약열 범위: 일간=최근7일, 주간=전체주
  const recentCol=cols[0];
  const sumLabel=isWeek?'전체':'7일';
  // Aggregate by product / rowId per column
  const ensure=(o,k)=>{if(!o[k])o[k]={s:0,r:0,p:0,mp:0,uc:0,imp:0};return o[k]};
  const prodCol={};const prodSort={};const byA={};
  // 세트필터: 키워드 입력 시 캠페인/세트명/ID 포함 세트만 (상품별 종합·소계도 필터 결과 기준)
  const tpKw=(document.getElementById('tpFilter')?.value||'').trim().toLowerCase();
  AD.forEach(r=>{if(!dd.includes(r.date))return;
    if(tpKw){const _nm=((r.campaign_name||'')+' '+(MODE==='cr'?(r.ad_name||''):(r.adset_name||''))+' '+rowId(r)).toLowerCase();if(!_nm.includes(tpKw))return}
    const ck=colKey(r.date);const p=r.product||'기타';
    if(!prodCol[p])prodCol[p]={};const pc=ensure(prodCol[p],ck);
    pc.s+=r.spend;pc.r+=r.revenue;pc.p+=r.profit;pc.mp+=r.results_mp;pc.uc+=r.unique_clicks;pc.imp+=(r.impressions||0);
    if(prodSort[p]===undefined)prodSort[p]=0;
    if(isWeek){if(ck===recentCol)prodSort[p]+=r.spend}else{if(r.date===yDay)prodSort[p]+=r.spend}
    const rid=rowId(r);
    if(!byA[rid])byA[rid]={cn:r.campaign_name,an:MODE==='cr'?(r.ad_name||''):(r.adset_name||''),id:rid,product:p,d:{}};
    const b=ensure(byA[rid].d,ck);
    b.s+=r.spend;b.r+=r.revenue;b.p+=r.profit;b.mp+=r.results_mp;b.uc+=r.unique_clicks;b.imp+=(r.impressions||0);
  });
  const aggCols=src=>{const o={s:0,r:0,p:0,mp:0,uc:0,imp:0};sumCols.forEach(ck=>{const t=src[ck];if(t){o.s+=t.s;o.r+=t.r;o.p+=t.p;o.mp+=t.mp;o.uc+=t.uc;o.imp+=t.imp}});return o};
  // 정렬 기준용 현재 일예산 (2026-08-20: 지출 순 → 예산 순으로 변경)
  const BUD=curBudMap(AD);
  const prodBud={};Object.values(byA).forEach(a=>{a._bud=BUD[a.id]||0;prodBud[a.product]=(prodBud[a.product]||0)+a._bud});
  // 상품 정렬 — 예산순이면 예산 합↓, 그 외(지출순·카테고리)는 기존대로 전날/최근주 지출↓
  const _byP={};Object.keys(prodCol).forEach(k=>{_byP[k]={bud:prodBud[k]||0,yS:prodSort[k]||0}});
  const sortedProds=orderProdKeys(_byP);
  // Detail items + summary metrics
  const allItems=Object.values(byA).map(a=>{const sm=aggCols(a.d);a._sm=sm;a._roas=sm.s>0?sm.r/sm.s*100:0;a._cvr=sm.uc>0&&sm.mp>0?sm.mp/sm.uc*100:0;a._ctr=sm.imp>0?sm.uc/sm.imp*100:0;a._cpm=sm.imp>0?sm.s/sm.imp*1000:0;a._sortV=isWeek?((a.d[recentCol]&&a.d[recentCol].s)||0):((a.d[yDay]&&a.d[yDay].s)||0);return a});
  // Cell renderer (aggregated bucket)
  const cell=(t,ck)=>{const yd=(!isWeek&&ck===yDay)?' col-yday':'';
    if(!t||!t.s)return'<td class="'+yd.trim()+'"></td>';
    const roas=t.s>0?t.r/t.s*100:0;const cvr=t.uc>0&&t.mp>0?t.mp/t.uc*100:0;const cpm=t.imp>0?t.s/t.imp*1000:0;const ctr=t.imp>0?t.uc/t.imp*100:0;
    return'<td class="mc '+RC(roas)+yd+'">'+MC(roas,t.p,t.s,t.r,cvr,cpm,ctr)+'</td>'};
  // Headers
  const ths=cols.map(ck=>{if(isWeek)return'<th style="min-width:var(--cw)">'+wkLabel(ck)+'</th>';const w=WD(ck);const yd=ck===yDay?' col-yday':'';return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(ck)+'('+w+')</th>'}).join('');
  const colSpan=cols.length+4;
  // Build table
  let h='<thead><tr><th style="min-width:200px;text-align:left">상품</th><th style="min-width:200px;text-align:left">'+rowNameLabel()+'</th><th style="min-width:130px">'+rowIdLabel()+'</th><th>'+sumLabel+'</th>'+ths+'</tr></thead><tbody>';
  // ── 상단: 상품별 종합 성과 ──
  h+='<tr><td colspan="'+colSpan+'" class="prod-header">📊 상품별 종합 성과</td></tr>';
  sortedProds.forEach(prod=>{
    const sm=aggCols(prodCol[prod]);const roasS=sm.s>0?sm.r/sm.s*100:0;const cvrS=sm.uc>0&&sm.mp>0?sm.mp/sm.uc*100:0;const cpmS=sm.imp>0?sm.s/sm.imp*1000:0;const ctrS=sm.imp>0?sm.uc/sm.imp*100:0;
    const cells=cols.map(ck=>cell(prodCol[prod][ck],ck)).join('');
    const cnt=Object.keys(byA).filter(k=>byA[k].product===prod).length;
    h+='<tr class="sr" style="background:#f0f4ff"><td class="fx fx0" style="background:#f0f4ff;font-weight:700">📦 '+prod+'</td><td class="fx fx1" style="background:#f0f4ff;font-size:10px;color:#888">'+cnt+'개</td><td></td><td class="mc '+RC(roasS)+'">'+MC(roasS,sm.p,sm.s,sm.r,cvrS,cpmS,ctrS)+'</td>'+cells+'</tr>';
  });
  h+='<tr><td colspan="'+colSpan+'" style="height:12px"></td></tr>';
  // ── 하단: 상품별 상세 ──
  sortedProds.forEach(prod=>{
    const items=allItems.filter(a=>a.product===prod).sort((a,b)=>tSortMode()==='budget'
      ?(((b._bud||0)-(a._bud||0))||(b._sortV-a._sortV))
      :(b._sortV-a._sortV));
    if(!items.length)return;
    const sm=aggCols(prodCol[prod]);const roasS=sm.s>0?sm.r/sm.s*100:0;const cpmS=sm.imp>0?sm.s/sm.imp*1000:0;
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+items.length+'개) '+(isWeek?'최근주':'전날')+' '+money(prodSort[prod]||0)+' · '+sumLabel+' ROAS '+roasS.toFixed(0)+'%</td></tr>';
    // Product subtotal
    const pCells=cols.map(ck=>cell(prodCol[prod][ck],ck)).join('');
    h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">'+prod+' 소계</td><td class="fx fx1" style="background:#e8e8e8"></td><td></td><td class="mc '+RC(roasS)+'">'+MC(roasS,sm.p,sm.s,sm.r,0,cpmS)+'</td>'+pCells+'</tr>';
    // Individual rows
    items.forEach(a=>{
      const cells=cols.map(ck=>cell(a.d[ck],ck)).join('');
      const hl=hlClass(a.id);const ck=' clickable" data-id="'+a.id+'" onclick="showCP(\''+a.id+'\',this)"';
      h+='<tr><td class="fx fx0 '+hl+ck+'>'+((a.cn||'').slice(0,25))+'</td><td class="fx fx1 '+hl+ck+'>'+((a.an||'').slice(0,25))+'</td><td class="'+hl+ck+' style="font-size:9px">'+a.id+'</td><td class="mc '+RC(a._roas)+'">'+MC(a._roas,a._sm.p,a._sm.s,a._sm.r,a._cvr,a._cpm,a._ctr)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';document.getElementById('tpTbl').innerHTML=h;
}

// ===== CHANGE =====
function renderChange(){
  // 보기: 일별(최근30일) / 주별(최근16주,월요일기준 WM) / 월별(최근6개월). 증감률=직전 기간 대비 지출 변화.
  const view=document.getElementById('cView')?.value||'day';
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  const pkey=d=>view==='day'?d:view==='week'?WM(d):d.slice(0,7);
  const weekLabel=mon=>{const s=new Date(mon);const e=new Date(mon);e.setDate(e.getDate()+6);return(s.getMonth()+1)+'/'+s.getDate()+'~'+(e.getMonth()+1)+'/'+e.getDate()};
  const colLabel=k=>view==='day'?DK(k):view==='week'?weekLabel(k):k.slice(2);
  const d7=DATES.slice(0,7);   // '7일' 요약 컬럼(고정 KPI, 보기 무관)
  // 세트별: 기간(period)별 지출·매출 + 최근일 원본(7일 KPI용)
  const byA={};
  AD.forEach(r=>{const rid=rowId(r);if(!byA[rid])byA[rid]={cn:r.campaign_name,an:MODE==='cr'?(r.ad_name||''):(r.adset_name||''),id:rid,p:{},d:{}};const o=byA[rid].p[pkey(r.date)]||(byA[rid].p[pkey(r.date)]={s:0,r:0});o.s+=r.spend;o.r+=r.revenue;byA[rid].d[r.date]=r});
  let list=Object.values(byA).map(a=>{let s=0,rv=0;d7.forEach(d=>{if(a.d[d]){s+=a.d[d].spend;rv+=a.d[d].revenue}});a._s=s;a._r=rv;a._roas=s>0?rv/s*100:0;return a}).sort((a,b)=>b._s-a._s);
  // 세트필터: 키워드 입력 시 캠페인/세트명/ID 포함 세트만 (종합 합계도 필터 결과 기준)
  const tKw=(document.getElementById('cFilter')?.value||'').trim().toLowerCase();
  if(tKw)list=list.filter(a=>((a.cn||'')+' '+(a.an||'')+' '+(a.id||'')).toLowerCase().includes(tKw));
  // 컬럼(기간) — 최신순
  const limit=view==='day'?30:view==='week'?16:6;
  const cols=(view==='day'?DATES.slice():[...new Set(AD.map(r=>pkey(r.date)))].sort().reverse()).slice(0,limit);
  const ths=cols.map(k=>{const yd=(view==='day'&&k===yDay)?' col-yday':'';return'<th class="'+yd+'" style="min-width:var(--cw)">'+colLabel(k)+'</th>'}).join('');
  const totP={};cols.forEach(k=>{let s=0,r=0;list.forEach(a=>{const o=a.p[k];if(o){s+=o.s;r+=o.r}});totP[k]={s,r}});
  let h='<thead><tr><th style="min-width:200px;text-align:left">캠페인</th><th style="min-width:200px;text-align:left">'+rowNameLabel()+'</th><th>'+rowIdLabel()+'</th><th>7일</th>'+ths+'</tr></thead><tbody>';
  const ts=list.reduce((a,x)=>a+x._s,0),tr=list.reduce((a,x)=>a+x._r,0);
  h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">종합</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8"><div class="r">ROAS</div><div class="p">증감률</div><div class="s">지출금액</div><div class="rv">매출</div></td><td class="mc">'+MC(ts>0?tr/ts*100:0,tr-ts,ts,tr,0)+'</td>';
  cols.forEach((k,i)=>{const x=totP[k];const roas=x.s>0?x.r/x.s*100:0;let chg=0;if(i<cols.length-1){const pr=totP[cols[i+1]];if(pr&&pr.s>0)chg=(x.s-pr.s)/pr.s*100}const yd=(view==='day'&&k===yDay)?' col-yday':'';
  h+='<td class="mc '+RC(roas)+yd+'"><div class="r">'+roas.toFixed(0)+'</div><div class="'+(chg>0?'p':'p neg')+'">'+(chg>0?'+':'')+chg.toFixed(1)+'%</div><div class="s">-'+money(x.s)+'</div><div class="rv">'+money(x.r)+'</div></td>'});
  h+='</tr>';
  list.forEach(a=>{
    const cells=cols.map((k,i)=>{const o=a.p[k];const yd=(view==='day'&&k===yDay)?' col-yday':'';if(!o||!o.s)return'<td class="'+yd+'"></td>';const roas=o.s>0?o.r/o.s*100:0;let chg=0;if(i<cols.length-1){const pr=a.p[cols[i+1]];if(pr&&pr.s>0)chg=(o.s-pr.s)/pr.s*100}
    return'<td class="mc '+RC(roas)+yd+'"><div class="r">'+roas.toFixed(0)+'</div><div class="'+(chg>0?'p':'p neg')+'">'+(chg>0?'+':'')+chg.toFixed(1)+'%</div><div class="s">-'+money(o.s)+'</div><div class="rv">'+money(o.r)+'</div></td>'}).join('');
    h+='<tr><td class="fx fx0">'+(a.cn||'').slice(0,25)+'</td><td class="fx fx1">'+(a.an||'').slice(0,25)+'</td><td style="font-size:9px">'+a.id+'</td><td class="mc">'+MC(a._roas,a._r-a._s,a._s,a._r,0)+'</td>'+cells+'</tr>';
  });
  h+='</tbody>';document.getElementById('cTbl').innerHTML=h;
}

// ===== WEEKLY =====
// 디멘드젠 ad_group/캠페인명 → 상품 추출 (주간종합 상품 분해용). 매칭 없으면 '기타'.
const _DG_PROD_KW=['무당','무녀','29금궁합','속궁합','29금','궁합','솔로','재회','환승','도화','커리어','팩폭','재물','임신','집착','바람기','구미호','욕망','신점','관상','사주'];
function _dgProduct(name){const s=String(name||'');for(const p of _DG_PROD_KW){if(s.includes(p))return p}return '기타'}
// 디멘드젠(GGDG_TIGHT) → AD 유사 행 (spend/revenue/profit/results_mp/product/클릭·노출)
//   클릭·노출=Google Ads API clicks/impressions (메타의 unique_clicks 와 달리 전체 클릭 — CVR/CTR 계산용)
function _dgRows(){return (GGDG_TIGHT||[]).map(r=>{const s=+r.spend||0,rv=+r.revenue||0;return {date:r.date,spend:s,revenue:rv,profit:rv-s,results_mp:+r.purchase_count||0,unique_clicks:+r.clicks||0,impressions:+r.impressions||0,product:_dgProduct(r.ad_group_name||r.campaign_name)}})}
// 주간종합 '밴스드 포함' 합산용 — 대만 밴스드(VN_TW_ACC) 행을 USD 환산(vnTwUsdRows)한 뒤
//   상품명을 캠페인명에서 복원하고 글로벌 상품 캐논(무당/shaman 등)까지 태워 AD 와 스키마를 맞춘다.
//   ★ 국가필터가 대만·전체가 아니면 빈 배열 — 밴스드 대만 물량이 홍콩·일본 컬럼에 섞이면 안 된다
//     (매출탭 renderGlobalRevenue 의 twOK 와 동일 기준).
function _vnTwWeeklyRows(){
  if(COUNTRY!=='ALL'&&COUNTRY!=='TW')return [];
  return canonGLRows(vnTwUsdRows().map(r=>({...r,product:vnProduct(r)||'기타'})));
}
function renderWeekly(){
  const mode=document.getElementById('wMode').value;
  //   ★ 소스 선택지는 모드마다 다르다(_syncWeeklySource 가 셀렉트를 다시 그린다).
  //     · 국내(kr): 'kr' 국내메타 / 'dg' 디멘드젠 / 'both' 둘 합산
  //     · 글로벌(gl): 'glv' 글로벌메타+밴스드대만(기본) / 'kr' 글로벌메타 단독
  //     · 밴스드·국내소재: 선택 없음 → 'kr'(그 모드의 AD)
  //     디멘드젠(GGDG_TIGHT)은 국내 원화 테이블이라 kr 밖에서는 절대 섞이지 않게 막는다.
  const _sv=(document.getElementById('wSource')||{}).value||'';
  const src=MODE==='kr'?(_sv||'kr'):(MODE==='gl'?(_sv==='kr'?'kr':'glv'):'kr');
  // 소스 데이터셋: 메타=AD(모드별 재빌드), 디멘드젠=GGDG_TIGHT, 밴스드=_vnTwWeeklyRows().
  //   디멘드젠 테이블은 lazy 로드 → 없으면 로딩 트리거 후 재렌더.
  let ROWS;
  if(src==='dg'||src==='both'){
    if(!GGDG_TIGHT.length&&!(window._BIG_LOADED&&window._BIG_LOADED.ggdgkr)){
      ensureBigTable('ggdgkr').then(()=>{const at=document.querySelector('.tab.active');if(at&&at.dataset.t==='weekly')renderWeekly()});
      document.getElementById('wBlocks').innerHTML='<div style="padding:24px;color:#888;text-align:center">구글 디멘드젠 데이터 로딩 중…</div>';
      return;
    }
    ROWS=(src==='dg')?_dgRows():AD.concat(_dgRows());
  }else if(src==='glv'){
    ROWS=AD.concat(_vnTwWeeklyRows());
  }else{
    ROWS=AD;
  }
  // Aggregate by week, month, and also by product
  const byW={},byM={},byWP={},byMP={};
  ROWS.forEach(r=>{
    const wk=WM(r.date),mk=r.date.slice(0,7),prod=r.product||'기타';
    const uc=r.unique_clicks||0,imp=r.impressions||0,mp=r.results_mp||0;
    // Week total
    if(!byW[wk])byW[wk]={s:0,r:0,p:0,mp:0,uc:0,imp:0};
    byW[wk].s+=r.spend;byW[wk].r+=r.revenue;byW[wk].p+=r.profit;byW[wk].mp+=mp;byW[wk].uc+=uc;byW[wk].imp+=imp;
    // Month total
    if(!byM[mk])byM[mk]={s:0,r:0,p:0,mp:0,uc:0,imp:0};
    byM[mk].s+=r.spend;byM[mk].r+=r.revenue;byM[mk].p+=r.profit;byM[mk].mp+=mp;byM[mk].uc+=uc;byM[mk].imp+=imp;
    // Week × product
    const wpk=wk+'|'+prod;if(!byWP[wpk])byWP[wpk]={s:0,r:0,p:0,mp:0,uc:0,imp:0,prod};
    byWP[wpk].s+=r.spend;byWP[wpk].r+=r.revenue;byWP[wpk].p+=r.profit;byWP[wpk].mp+=mp;byWP[wpk].uc+=uc;byWP[wpk].imp+=imp;
    // Month × product
    const mpk=mk+'|'+prod;if(!byMP[mpk])byMP[mpk]={s:0,r:0,p:0,mp:0,uc:0,imp:0,prod};
    byMP[mpk].s+=r.spend;byMP[mpk].r+=r.revenue;byMP[mpk].p+=r.profit;byMP[mpk].mp+=mp;byMP[mpk].uc+=uc;byMP[mpk].imp+=imp;
  });
  const months=Object.keys(byM).sort().reverse(),weeks=Object.keys(byW).sort().reverse();

  function getProds(prefix,map){
    return Object.entries(map).filter(([k])=>k.startsWith(prefix+'|')).map(([k,v])=>v).sort((a,b)=>b.s-a.s);
  }

  function kpiRow(d,bold,indent){
    const roas=d.s>0?d.r/d.s*100:0;
    const cvr=d.uc>0&&d.mp>0?d.mp/d.uc*100:0;
    const ctr=d.imp>0?d.uc/d.imp*100:0;
    const fw=bold?'font-weight:700;font-size:13px':'font-weight:400;font-size:12px';
    const pl=indent?'padding-left:20px':'';
    return'<div style="display:grid;grid-template-columns:repeat(8,1fr);border-bottom:1px solid #eee">'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">지출</div><div style="'+fw+';color:#d00">'+money(d.s)+'</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">매출</div><div style="'+fw+';color:#00d">'+money(d.r)+'</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">이익</div><div style="'+fw+';color:'+(d.p>=0?'#16a34a':'#dc2626')+'">'+money(d.p)+'</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">ROAS</div><div style="'+fw+'">'+roas.toFixed(1)+'%</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">구매</div><div style="'+fw+'">'+F(d.mp)+'</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">CVR</div><div style="'+fw+'">'+cvr.toFixed(2)+'%</div></div>'+
      '<div style="padding:6px;text-align:center;border-right:1px solid #eee"><div style="font-size:9px;color:#999">CTR</div><div style="'+fw+'">'+ctr.toFixed(2)+'%</div></div>'+
      '<div style="padding:6px;text-align:center"><div style="font-size:9px;color:#999">예산비중</div><div style="'+fw+'">100%</div></div>'+
    '</div>';
  }

  function blk(label,cls,d,prods){
    const roas=d.s>0?d.r/d.s*100:0;
    const isMonth=cls==='month';
    const hdrBg=isMonth?'background:#1a2744;color:#fff':'background:#555;color:#fff';
    const roasColor=roas>=100?'#4ade80':'#f87171';
    let out='<div style="border-bottom:1px solid #ddd;margin-bottom:2px">';
    out+='<div style="'+hdrBg+';padding:8px 14px;font-weight:700;font-size:'+(isMonth?'13px':'11px')+';display:flex;justify-content:space-between;align-items:center"><span>'+label+'</span><span style="font-size:11px">ROAS <span style="color:'+roasColor+'">'+roas.toFixed(0)+'%</span> · 이익 '+money(d.p)+'</span></div>';
    // 종합 KPI row
    out+=kpiRow(d,true,false);
    // 상품별 rows
    if(prods&&prods.length){
      prods.forEach(pd=>{
        const pRoas=pd.s>0?pd.r/pd.s*100:0;
        const rc=pRoas>=100?'#16a34a':'#dc2626';
        const pShare=d.s>0?pd.s/d.s*100:0;
        out+='<div style="display:grid;grid-template-columns:repeat(8,1fr);border-bottom:1px solid #f0f0f0;background:#fafafa">'+
          '<div style="padding:4px 6px;text-align:left;border-right:1px solid #eee;font-size:11px;font-weight:600;color:#333;padding-left:12px;display:flex;align-items:center">📦 '+pd.prod+'</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px;color:#00d">'+money(pd.r)+'</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px;color:'+(pd.p>=0?'#16a34a':'#dc2626')+'">'+money(pd.p)+'</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px;color:'+rc+'">'+pRoas.toFixed(0)+'%</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px">'+F(pd.mp)+'</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px">'+(pd.uc>0&&pd.mp>0?(pd.mp/pd.uc*100).toFixed(2)+'%':'')+'</div>'+
          '<div style="padding:4px 6px;text-align:center;border-right:1px solid #eee;font-size:11px">'+(pd.imp>0?(pd.uc/pd.imp*100).toFixed(2)+'%':'')+'</div>'+
          '<div style="padding:4px 6px;text-align:center;font-size:11px;font-weight:600;color:#555">'+(pShare>0?pShare.toFixed(1)+'%':'')+'</div>'+
        '</div>';
      });
    }
    out+='</div>';
    return out;
  }

  let html='<div style="max-width:800px;margin:0 auto">';
  if(mode==='month'){
    months.forEach(mk=>{html+=blk(mk.slice(0,4)+'년 '+parseInt(mk.slice(5))+'월','month',byM[mk],getProds(mk,byMP))});
  }else if(mode==='week'){
    weeks.forEach(wk=>{const we=new Date(new Date(wk).getTime()+6*864e5);html+=blk((new Date(wk).getMonth()+1)+'/'+new Date(wk).getDate()+'~'+(we.getMonth()+1)+'/'+we.getDate(),'week',byW[wk],getProds(wk,byWP))});
  }else{
    months.forEach(mk=>{
      html+=blk(mk.slice(0,4)+'년 '+parseInt(mk.slice(5))+'월','month',byM[mk],getProds(mk,byMP));
      weeks.filter(w=>w.slice(0,7)===mk).forEach(wk=>{const we=new Date(new Date(wk).getTime()+6*864e5);html+=blk((new Date(wk).getMonth()+1)+'/'+new Date(wk).getDate()+'~'+(we.getMonth()+1)+'/'+we.getDate(),'week',byW[wk],getProds(wk,byWP))});
    });
  }
  html+='</div>';
  document.getElementById('wBlocks').innerHTML=html;
}

// ===== BUDGET =====
function renderBudget(){
  const dd=DATES.slice(0,30);const dpd={};
  AD.forEach(r=>{if(!dd.includes(r.date))return;const k=r.date+'|'+r.product;if(!dpd[k])dpd[k]={s:0,r:0};dpd[k].s+=r.spend;dpd[k].r+=r.revenue});
  const ths=dd.map(d=>'<th>'+DK(d)+'</th>').join('');
  let h='<thead><tr><th class="rh"></th>'+ths+'</tr></thead><tbody>';
  h+='<tr><td class="rh">전체 쓴돈</td>'+dd.map(d=>'<td style="color:#d00;text-align:right">'+F(DAILY[d]?.s)+'</td>').join('')+'</tr>';
  h+='<tr><td class="rh">전체 번돈</td>'+dd.map(d=>'<td style="color:#00d;text-align:right">'+F(DAILY[d]?.r)+'</td>').join('')+'</tr>';
  h+='<tr><td class="rh">순이익</td>'+dd.map(d=>{const p=(DAILY[d]?.r||0)-(DAILY[d]?.s||0);return'<td style="text-align:right;color:'+(p>=0?'green':'red')+'">'+F(p)+'</td>'}).join('')+'</tr>';
  h+='<tr><td class="rh">ROAS</td>'+dd.map(d=>{const x=DAILY[d];const r=x&&x.s>0?x.r/x.s*100:0;return'<td class="'+RC(r)+'">'+r.toFixed(1)+'</td>'}).join('')+'</tr>';
  h+='</tbody>';document.getElementById('bTbl').innerHTML=h;
}

// ===== DATE TAB (날짜 구간 합산 지원) =====
// 헬퍼: "어제 하루" 버튼 — 시작/종료 모두 어제로 설정
function dtSetToday(){
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  const selStart=document.getElementById('dtStart');const selEnd=document.getElementById('dtEnd');
  if(DATES.includes(yDay)){selStart.value=yDay;selEnd.value=yDay}
  else if(DATES.length){selStart.value=DATES[0];selEnd.value=DATES[0]}
  renderDateTab();
}

// 뷰스루 비중 = (메타구매 - 클릭구매)/메타구매. results_meta_click 없으면(구버전 데이터) 공백.
function _mvPct(meta, click, has){
  meta=meta||0;
  if(!has||meta<=0)return '';
  let v=(meta-(click||0))/meta*100;
  v=v<0?0:(v>100?100:v);
  return v.toFixed(0)+'%';
}
function renderDateTab(){
  const selStart=document.getElementById('dtStart');
  const selEnd=document.getElementById('dtEnd');
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // 옵션 초기화 — DATES 가 바뀔 때마다 다시 만든다.
  //   ⚠️ 예전엔 '최초 1회'(options.length 로 판정)만 채웠다. 그래서
  //     ① 캐시로 먼저 그린 뒤 fresh 데이터가 도착해 renderDateTab 이 다시 불려도 날짜 목록이 옛날 그대로 남고
  //     ② 국내→글로벌 모드 전환처럼 DATES 가 통째로 바뀌는 경우에도 갱신되지 않았다
  //   → 새로고침 전까지 드롭다운이 옛 마지막 날짜에 고착됐다(2026-08-16: 글로벌 날짜탭이 8/6 까지만 나온 건).
  //   선택값은 살려두되, 새 목록에 없는 날짜면 기본값(어제)으로 돌린다.
  const dtSig=DATES.length+'|'+(DATES[0]||'')+'|'+(DATES[DATES.length-1]||'');
  if(selStart.dataset.sig!==dtSig){
    const prevS=selStart.value,prevE=selEnd.value;
    selStart.innerHTML='';selEnd.innerHTML='';
    DATES.forEach(d=>{
      const o1=document.createElement('option');o1.value=d;o1.textContent=DK(d)+' ('+WD(d)+')';selStart.appendChild(o1);
      const o2=document.createElement('option');o2.value=d;o2.textContent=DK(d)+' ('+WD(d)+')';selEnd.appendChild(o2);
    });
    selStart.dataset.sig=dtSig;selEnd.dataset.sig=dtSig;
    // 기본값: 시작/종료 모두 어제 (없으면 가장 최근 날짜)
    const def=DATES.includes(yDay)?yDay:DATES[0];
    selStart.value=DATES.includes(prevS)?prevS:def;
    selEnd.value=DATES.includes(prevE)?prevE:def;
  }
  let sd=selStart.value||DATES[0];
  let ed=selEnd.value||DATES[0];
  // 시작일이 종료일보다 크면 자동으로 swap
  if(sd>ed){[sd,ed]=[ed,sd];selStart.value=sd;selEnd.value=ed}
  // 하이라이트: 날짜탭은 추이차트 하이라이트(HIGHLIGHTS=현재 세트별 마킹)를 그대로 따라간다.
  //   AI봇(오늘의퍼포먼스봇) 마킹은 adset_highlights(HIGHLIGHTS)에만 있고 perfTbl.highlight엔 없어서,
  //   날짜별 r.highlight를 쓰면 추이차트와 어긋난다. → 세트별 현재 마킹만 사용(추이차트 이름색과 항상 동일).
  // 안내 문구
  const dayCount=Math.round((new Date(ed)-new Date(sd))/864e5)+1;
  const dtInfo=document.getElementById('dtInfo');
  if(sd===ed){dtInfo.textContent='단일일자: '+DK(sd)+' ('+WD(sd)+')'}
  else{dtInfo.textContent=DK(sd)+' ~ '+DK(ed)+' ('+dayCount+'일 통합)'}

  // 구간 내 row 필터링
  const inRangeRows=AD.filter(r=>r.date>=sd&&r.date<=ed);

  let rows;
  let isRange=(sd!==ed);
  if(!isRange){
    // 단일 일자 — 원본 row 그대로
    rows=inRangeRows.sort((a,b)=>b.profit-a.profit);
  }else{
    // 구간 합산 (rowId 기준)
    const byId={};
    inRangeRows.forEach(r=>{
      const rid=rowId(r);if(!rid)return;
      if(!byId[rid]){
        byId[rid]={
          // 저장/링크용: 구간 내 가장 최근 날짜
          date:r.date,
          adset_id:r.adset_id,ad_id:r.ad_id,
          campaign_name:r.campaign_name,adset_name:r.adset_name,ad_name:r.ad_name,
          product:r.product,
          // 합산 지표
          spend:0,revenue:0,profit:0,
          impressions:0,reach:0,unique_clicks:0,
          results_meta:0,results_meta_click:0,results_mp:0,_hasClick:false,
          // 최근값 유지
          budget:r.budget||0,
          highlight:r.highlight||null,
          memo:r.memo||null,
          _latestDate:r.date
        };
      }
      const a=byId[rid];
      a.spend+=(r.spend||0);
      a.revenue+=(r.revenue||0);
      a.profit+=(r.profit||0);
      a.impressions+=(r.impressions||0);
      a.reach+=(r.reach||0);
      a.unique_clicks+=(r.unique_clicks||0);
      a.results_meta+=(r.results_meta||0);
      a.results_meta_click+=(r.results_meta_click||0);if(r.results_meta_click!=null)a._hasClick=true;
      a.results_mp+=(r.results_mp||0);
      // 가장 최근 날짜의 값으로 예산/하이라이트/메모/이름 갱신
      if(r.date>=a._latestDate){
        a._latestDate=r.date;a.date=r.date;
        a.budget=r.budget||0;
        a.highlight=r.highlight||null;
        a.memo=r.memo||null;
        a.campaign_name=r.campaign_name;
        a.adset_name=r.adset_name;
        a.ad_name=r.ad_name;
        a.product=r.product;
      }
    });
    // 파생 지표 재계산 (합산 후)
    rows=Object.values(byId).map(a=>{
      a.cpm=a.impressions>0?a.spend/a.impressions*1000:0;
      a.cost_per_result=a.results_mp>0?a.spend/a.results_mp:0;
      a.cost_per_click=a.unique_clicks>0?a.spend/a.unique_clicks:0;
      a.unique_ctr=a.impressions>0?a.unique_clicks/a.impressions*100:0;
      a.frequency=a.reach>0?a.impressions/a.reach:0;
      a.roas=a.spend>0?a.revenue/a.spend*100:0;
      a.cvr=a.unique_clicks>0&&a.results_mp>0?a.results_mp/a.unique_clicks*100:0;
      // 메타 보고 구매ROAS는 구간 합산 불가 (원본에 meta revenue 없음) → null
      a.purchase_roas_meta=null;
      return a;
    }).sort((a,b)=>b.profit-a.profit);
  }

  // 필터: 캠페인 · 세트(소재)명 · ID · 상품에 키워드가 포함된 행만 표시.
  //   종합 row·'메타에 예산 적용' 대상(DT_ROWS) 모두 필터 결과 기준 — 화면에 보이는 것과 항상 일치.
  const dtKw=((document.getElementById('dtFilter')||{}).value||'').trim().toLowerCase();
  if(dtKw){
    rows=rows.filter(r=>((r.campaign_name||'')+' '+(r.adset_name||'')+' '+(r.ad_name||'')+' '+(r.product||'')+' '+(rowId(r)||'')).toLowerCase().includes(dtKw));
    dtInfo.textContent+=' · 🔍"'+dtKw+'" '+rows.length+'개';
  }

  // 종합 row 계산
  const totS=rows.reduce((a,b)=>a+(b.spend||0),0);
  const totR=rows.reduce((a,b)=>a+(b.revenue||0),0);
  const totP=totR-totS;
  const totImp=rows.reduce((a,b)=>a+(b.impressions||0),0);
  const totReach=rows.reduce((a,b)=>a+(b.reach||0),0);
  const totUC=rows.reduce((a,b)=>a+(b.unique_clicks||0),0);
  const totMP=rows.reduce((a,b)=>a+(b.results_mp||0),0);
  const totMeta=rows.reduce((a,b)=>a+(b.results_meta||0),0);
  const totMetaClick=rows.reduce((a,b)=>a+(b.results_meta_click||0),0);
  const anyClick=rows.some(b=>b._hasClick||b.results_meta_click!=null);
  const totRoas=totS>0?totR/totS*100:0;
  const totCvr=totUC>0&&totMP>0?totMP/totUC*100:0;
  const totCpm=totImp>0?totS/totImp*1000:0;
  const totCpa=totMP>0?totS/totMP:0;
  const totCpc=totUC>0?totS/totUC:0;
  const totCtr=totImp>0?totUC/totImp*100:0;
  const totFreq=totReach>0?totImp/totReach:0;

  let h='<thead><tr>';
  h+='<th style="text-align:left">캠페인</th>';
  h+='<th style="text-align:left">'+rowNameLabel()+'</th>';
  h+='<th>'+rowIdLabel()+'</th>';
  h+='<th class="h-meta">지출</th>';
  h+='<th class="h-meta">결과당비용</th>';
  h+='<th class="h-meta">구매ROAS(메타)</th>';
  h+='<th class="h-meta">CPM</th>';
  h+='<th class="h-meta">도달</th>';
  h+='<th class="h-meta">노출</th>';
  h+='<th class="h-meta">고유클릭</th>';
  h+='<th class="h-meta">고유CTR</th>';
  h+='<th class="h-meta">클릭당비용</th>';
  h+='<th class="h-meta">빈도</th>';
  h+='<th class="h-meta">결과(메타)</th>';
  h+='<th class="h-meta" title="뷰스루 비중 = (메타구매 - 클릭구매)/메타구매. 높을수록 보기만하고 산 비중↑ (클릭전용 효율은 낮음)">뷰%</th>';
  h+='<th class="h-mp">결과(MP)</th>';
  h+='<th class="h-mp">매출</th>';
  h+='<th class="h-mp">이익</th>';
  h+='<th class="h-mp">ROAS</th>';
  h+='<th class="h-mp">CVR</th>';
  h+='<th class="h-budget" title="현재 메타 예산(각 세트 최신일 스냅샷) — 선택한 날짜와 무관하게 지금 값">예산</th>';
  h+='<th class="h-rate">증액률</th>';
  h+='<th class="h-result">변동예산</th>';
  h+='<th class="h-memo">메모</th>';
  h+='</tr></thead><tbody>';

  // 종합 row (맨 위)
  h+='<tr class="sr">';
  h+='<td style="text-align:left">종합 ('+rows.length+'개'+(isRange?' · '+dayCount+'일':'')+')</td>';
  h+='<td></td><td></td>';
  h+='<td style="text-align:right;color:#d00">'+money(totS)+'</td>';
  h+='<td style="text-align:right">'+money(totCpa)+'</td>';
  h+='<td></td>';
  h+='<td style="text-align:right">'+money(totCpm)+'</td>';
  h+='<td style="text-align:right">'+F(totReach)+'</td>';
  h+='<td style="text-align:right">'+F(totImp)+'</td>';
  h+='<td style="text-align:right">'+F(totUC)+'</td>';
  h+='<td style="text-align:right">'+(totCtr?totCtr.toFixed(2)+'%':'')+'</td>';
  h+='<td style="text-align:right">'+money(totCpc)+'</td>';
  h+='<td style="text-align:right">'+(totFreq?totFreq.toFixed(2):'')+'</td>';
  h+='<td style="text-align:right">'+F(totMeta)+'</td>';
  h+='<td style="text-align:right;color:#888">'+_mvPct(totMeta,totMetaClick,anyClick)+'</td>';
  h+='<td style="text-align:right;font-weight:700">'+F(totMP)+'</td>';
  h+='<td style="text-align:right;color:#00d">'+money(totR)+'</td>';
  h+='<td style="text-align:right;color:'+(totP>=0?'green':'red')+'">'+money(totP)+'</td>';
  h+='<td class="'+RC(totRoas)+'">'+P(totRoas)+'</td>';
  h+='<td>'+P(totCvr,2)+'</td>';
  h+='<td></td><td></td><td></td><td></td>';
  h+='</tr>';

  // 예산 컬럼 = '현재 메타 예산' — 각 세트의 최신일 예산 스냅샷(선택일과 무관하게 현재값 표시).
  //   과거일의 그날 예산이 아니라 지금 메타 값을 보여줘 Ads Manager와 일치. (추이차트의 일자별
  //   과거 예산 복원은 증감 테두리용으로 그대로 유지 → 날짜탭 표시만 변경)
  const curBud={};
  AD.forEach(r=>{const rid=rowId(r);if(!rid)return;const p=curBud[rid];if(!p||r.date>p.d)curBud[rid]={d:r.date,b:+r.budget||0}});

  // 데이터 rows
  rows.forEach(r=>{
    const rid=rowId(r);let hl=HIGHLIGHTS[rid];const hlCls=hl&&HL_CONFIG[hl]?HL_CONFIG[hl].cls:'';
    const hlLabel=hl&&HL_CONFIG[hl]?HL_CONFIG[hl].label:'';const hlPct=hl&&HL_CONFIG[hl]?HL_CONFIG[hl].pct:null;
    const cbud=curBud[rid]?curBud[rid].b:(+r.budget||0);  // 현재(최신일) 예산
    let resultB='';if(cbud&&hlPct!==null){resultB=hl==='off'?'OFF':money(Math.round(cbud*(1+hlPct/100)))}
    // durable(daily_memos) 우선, 없으면 perfTbl.memo — 글로벌 지연적재로 perfTbl에 안 써진 메모도 표시.
    const _dm=DMEMO[_dmKey(MODE,r.date,rid)];
    const mv=((_dm!=null?_dm:r.memo)||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const name2=(MODE==='cr'?(r.ad_name||''):(r.adset_name||'')).slice(0,30);
    h+='<tr>';
    h+='<td style="text-align:left" class="'+hlCls+'">'+(r.campaign_name||'').slice(0,30)+'</td>';
    h+='<td style="text-align:left" class="'+hlCls+' clickable" data-id="'+rid+'" onclick="showCP(\''+rid+'\',this)" title="클릭해 증감액 마킹 — 추이차트와 연동됩니다"'+'>'+name2+'</td>';
    h+='<td class="'+hlCls+'" style="font-size:9px">'+rid+'</td>';
    h+='<td style="text-align:right;color:#d00">'+money(r.spend)+'</td>';
    h+='<td style="text-align:right">'+money(r.cost_per_result)+'</td>';
    h+='<td style="text-align:right">'+(r.purchase_roas_meta?r.purchase_roas_meta.toFixed(2):'')+'</td>';
    h+='<td style="text-align:right">'+money(r.cpm)+'</td>';
    h+='<td style="text-align:right">'+F(r.reach)+'</td>';
    h+='<td style="text-align:right">'+F(r.impressions)+'</td>';
    h+='<td style="text-align:right">'+F(r.unique_clicks)+'</td>';
    h+='<td style="text-align:right">'+(r.unique_ctr?r.unique_ctr.toFixed(2)+'%':'')+'</td>';
    h+='<td style="text-align:right">'+money(r.cost_per_click)+'</td>';
    h+='<td style="text-align:right">'+(r.frequency?r.frequency.toFixed(2):'')+'</td>';
    h+='<td style="text-align:right">'+F(r.results_meta)+'</td>';
    h+='<td style="text-align:right;color:#888">'+_mvPct(r.results_meta,r.results_meta_click,(r._hasClick||r.results_meta_click!=null))+'</td>';
    h+='<td style="text-align:right;font-weight:600">'+F(r.results_mp)+'</td>';
    h+='<td style="text-align:right;color:#00d">'+money(r.revenue)+'</td>';
    h+='<td style="text-align:right;color:'+(r.profit>=0?'green':'red')+'">'+money(r.profit)+'</td>';
    h+='<td class="'+(r.roas?RC(r.roas):'')+'">'+P(r.roas)+'</td>';
    h+='<td>'+P(r.cvr,2)+'</td>';
    h+='<td style="text-align:right" title="현재 메타 예산(최신 스냅샷)">'+money(cbud)+'</td>';
    h+='<td class="'+hlCls+' clickable" data-id="'+rid+'" onclick="showCP(\''+rid+'\',this)" title="클릭해 증감액 마킹 — 추이차트와 연동됩니다"'+' style="text-align:center;font-weight:600">'+(hlLabel||'<span style="color:#ccc;font-weight:400">+</span>')+'</td>';
    h+='<td class="'+hlCls+'" style="text-align:right;font-weight:600">'+resultB+'</td>';
    // 메모는 가장 최근 날짜(r.date)에 저장됨
    h+='<td class="memo-cell"><textarea class="memo-input" placeholder="메모" data-date="'+r.date+'" data-id="'+rid+'" onkeydown="if(event.key===\'Enter\'&&(event.ctrlKey||event.metaKey)){event.preventDefault();this.blur()}" onblur="saveMemo(this.dataset.date,this.dataset.id,this.value,this)">'+mv+'</textarea><span class="memo-saved">✓</span></td>';
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('dtTbl').innerHTML=h;
  DT_ROWS=rows;  // '메타에 예산 적용' 버튼이 화면에 실제로 보이는 세트만 대상으로 삼기 위해 보관
  abSyncBtn();
}

// ===== 메타 예산 적용 (날짜탭 → Edge Function) =====
// 날짜탭의 '변동예산'을 실제 메타 예산에 반영한다. 브라우저는 '어떤 세트를 어떤 마킹으로'만
// 보내고, 현재 예산 조회·계산·쓰기는 전부 Edge Function 이 한다 —
//   ① Meta 쓰기 토큰을 공개 HTML 에 둘 수 없고,
//   ② 날짜탭 '예산'은 파이프라인 스냅샷+통화환산을 거친 표시용 값이라 계산 근거로 못 쓴다
//      (서버가 적용 시점에 메타에서 현재값을 다시 읽는다).
var DT_ROWS=[];        // var — renderDateTab 이 이 선언보다 먼저 실행돼도 TDZ 에 걸리지 않게
var GGDG_ROWS=[];      // 구글 디멘드젠 탭에 실제로 보이는 광고그룹 (버튼·모달 대상)
let AB_SRC='meta';     // 'meta'(날짜탭) | 'google'(구글 디멘드젠 탭) — 대상·호출 함수가 갈린다
let AB_PLAN=null;      // dry-run 결과 (확인 버튼이 이걸 그대로 적용)
let AB_BUSY=false;
const AB_FN=SB_URL+'/functions/v1/apply-budget';
// 구글은 광고그룹 예산이 없어 ±% 가 캠페인 예산을 바꾼다 → 서버를 따로 둔다
const AB_FN_G=SB_URL+'/functions/v1/apply-budget-google';
const AB_MODES={kr:'국내',gl:'글로벌',vn:'밴스드'};

// 날짜탭에 보이는 행 중 증감액이 마킹된 세트 (중복 제거)
// src 를 명시하면 그 소스로 센다 — 버튼 개수 갱신(abSyncBtn/abSyncBtnG)이 서로를 침범하지 않게.
function abTargets(src){
  const S=src||AB_SRC;
  const seen=new Set(),out=[];
  if(S==='google'){
    (GGDG_ROWS||[]).forEach(o=>{
      const id=String(o.id||'');if(!id||seen.has(id))return;
      const tag=HIGHLIGHTS[id];if(!tag||!HL_CONFIG[tag])return;
      seen.add(id);
      out.push({adset_id:id,ad_account_id:'',tag:tag,name:(o.name||''),campaign:(o.camp||'')});
    });
    return out;
  }
  if(!AB_MODES[MODE])return[];
  (DT_ROWS||[]).forEach(r=>{
    const id=rowId(r);if(!id||seen.has(id))return;
    const tag=HIGHLIGHTS[id];if(!tag||!HL_CONFIG[tag])return;
    seen.add(id);
    out.push({adset_id:id,ad_account_id:String(r.ad_account_id||''),tag:tag,
              name:(r.adset_name||''),campaign:(r.campaign_name||'')});
  });
  return out;
}
// 소재별(cr)은 세트가 아니라 광고 단위라 예산 개념이 없다 → 버튼 자체를 숨긴다
// 구글 디멘드젠 탭 버튼
function abSyncBtnG(){
  const b=document.getElementById('abBtnG');if(!b)return;
  const n=abTargets('google').length;
  b.disabled=!n;b.style.opacity=n?'1':'.45';
  b.textContent='⚡ 구글에 예산 적용'+(n?' ('+n+')':'');
}
function abSyncBtn(){
  const b=document.getElementById('abBtn');if(!b)return;
  if(!AB_MODES[MODE]){b.style.display='none';return}
  const n=abTargets('meta').length;
  b.style.display='';b.disabled=!n;b.style.opacity=n?'1':'.45';
  b.textContent='⚡ 메타에 예산 적용'+(n?' ('+n+')':'');
}

async function abAuthHeaders(){
  const {data}=await SBC.auth.getSession();
  const tk=data&&data.session?data.session.access_token:'';
  if(!tk)throw new Error('로그인이 필요합니다. 새로고침 후 다시 로그인해 주세요.');
  return {'Authorization':'Bearer '+tk,'Content-Type':'application/json','apikey':SB_KEY};
}
// ids 를 주면 그 세트만 보낸다 → 서버가 그 부분집합만으로 다시 계획을 세운다.
// (CBO 충돌도 한쪽만 보내면 자연히 풀린다)
async function abCall(dryRun,ids){
  const pick=ids?new Set(ids):null;
  const items=abTargets().filter(t=>!pick||pick.has(t.adset_id))
    .map(t=>({adset_id:t.adset_id,ad_account_id:t.ad_account_id,tag:t.tag}));
  const g=AB_SRC==='google';
  const r=await fetch(g?AB_FN_G:AB_FN,{method:'POST',headers:await abAuthHeaders(),
    body:JSON.stringify({mode:g?'gd':MODE,dryRun:dryRun,items:items})});
  const j=await r.json().catch(()=>({}));
  if(!r.ok||j.ok===false)throw new Error(j.error||('서버 오류 ('+r.status+')'));
  return j;
}

// 최소통화단위(cents 등) → 사람이 읽는 금액. offset 은 서버가 통화별로 실어 보낸다.
function abAmt(v,off,ccy){
  if(v===''||v==null)return'';
  const n=Number(v);if(!isFinite(n))return String(v);
  const a=n/(off||1);
  return a.toLocaleString('ko-KR',{maximumFractionDigits:(off||1)>1?2:0})+(ccy?' '+ccy:'');
}
function abEsc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

// 적용 대상으로 고를 수 있는 행인가. 실제로 바뀌는 게 있거나, 선택을 줄이면 풀리는 CBO 충돌.
function abSelectable(p){
  if(p.conflict)return true;
  return !p.error&&!!p.field&&p.after!==p.before;
}
function abRender(plan,applied){
  const meta={};abTargets().forEach(t=>meta[t.adset_id]=t);
  const ckTh=applied?'':'<th class="ab-ckc"><input type="checkbox" id="abAll" onclick="abToggleAll(this)" title="전체 선택"></th>';
  const g=AB_SRC==='google';
  let h='<table><thead><tr>'+ckTh+'<th>'+(g?'광고그룹':'세트')+'</th><th>'+(g?'캠페인':'계정')+'</th>'
       +'<th>마킹</th><th>대상</th><th>항목</th>'
       +'<th>현재</th><th>→</th><th>변경 후</th><th>'+(applied?'결과':'비고')+'</th></tr></thead><tbody>';
  plan.forEach(p=>{
    const m=meta[p.adset_id]||{};
    const noChange=!p.error&&(!p.field||p.after===p.before);
    const cls=(p.conflict||p.redo)?'ab-conf':(p.error?'ab-err':((applied?!p.applied:noChange)?'ab-skip':''));
    const lbl=(HL_CONFIG[p.tag]||{}).label||p.tag;
    const hlc=(HL_CONFIG[p.tag]||{}).cls||'';   // 날짜탭과 같은 색을 마킹·변경후 칸에 입힌다
    let last;
    if(p.error)last='⚠ '+abEsc(p.error);
    else if(applied)last=p.applied?'✅ 적용됨':'— 변경 없음';
    else last=abEsc(p.note||'');
    if(!p.error&&p.scope==='campaign')last='<span class="ab-warn">⚠ '+(g?'캠페인 예산':'CBO')+'</span> '+last;
    if(!p.error&&p.redo)last='<span class="ab-warn">⚠ 재적용</span> '+last;   // 오늘 이미 적용 — 또 하면 ±% 가 복리
    const cur=p.field==='status'?abEsc(p.before):abAmt(p.before,p.offset,p.currency);
    const nxt=p.field==='status'?abEsc(p.after):abAmt(p.after,p.offset,p.currency);
    // CBO 충돌·오늘 재적용은 기본 해제 — 어느 쪽을 살릴지, 두 번 올릴지는 사람이 정해야 한다
    const sel=abSelectable(p),nobulk=!!(p.conflict||p.redo);
    const ckTd=applied?'':'<td class="ab-ckc"><input type="checkbox" class="ab-ck" data-id="'+abEsc(p.adset_id)+'"'
      +(nobulk?' data-nobulk="1"':'')+(sel?'':' disabled')+(sel&&!nobulk?' checked':'')
      +' onclick="abSelChanged()"></td>';
    // 이름 옆에 ID — Ads Manager·날짜탭과 대조할 때 쓰므로 그대로 복사 가능하게 둔다
    const nm=p.adset_name||m.name||'';
    const nameCell=nm?abEsc(nm)+' <span class="ab-id">'+abEsc(p.adset_id)+'</span>':'<span class="ab-id">'+abEsc(p.adset_id)+'</span>';
    h+='<tr class="'+cls+'">'+ckTd
      +'<td class="ab-name">'+nameCell+'<div style="color:#aaa;font-size:9px">'+abEsc(m.campaign||'')+'</div></td>'
      +'<td>'+abEsc(g?(p.campaign_name||m.campaign||''):accName(p.ad_account_id))+'</td>'
      +'<td class="'+hlc+'" style="text-align:center;font-weight:600">'+abEsc(lbl)+'</td>'
      +'<td>'+(p.scope==='campaign'?'캠페인':(p.scope==='adset'?'세트':(p.scope==='adgroup'?'광고그룹':'')))+'</td>'
      +'<td>'+abEsc(p.field==='status'?'상태':(p.field==='lifetime_budget'?'총예산':(p.field?'일예산':'')))+'</td>'
      +'<td style="text-align:right">'+cur+'</td><td style="text-align:center;color:#aaa">→</td>'
      +'<td class="'+hlc+'" style="text-align:right;font-weight:600">'+nxt+'</td>'
      +'<td>'+last+'</td></tr>';
  });
  h+='</tbody></table><div class="ab-sum" id="abSum"></div>';
  document.getElementById('abBody').innerHTML=h;
  if(applied)abRenderSum(plan.filter(p=>p.applied),true);
  else abSelChanged();
}

// ── 증감 합계 (표 하단) ──
// '얼마를 내리고 얼마를 올려서 결국 총예산이 얼마 움직이는가'를 승인 직전에 보여준다.
// 규칙:
//  · 통화가 섞일 수 있어(국내=KRW·글로벌=USD) 통화별로 나눠 더한다. 환산은 하지 않는다.
//  · CBO(캠페인 예산)는 한 캠페인이 여러 세트 행으로 나올 수 있어 대상 단위로 한 번만 센다
//    — 안 그러면 같은 예산 변경이 중복으로 더해진다.
//  · OFF(상태 변경)도 감액으로 센다 — 세트를 끄면 그 세트 예산만큼 하루 지출이 사라지므로
//    '결국 예산이 얼마 움직이는가'에는 포함돼야 한다. 서버(apply-budget)가 내려주는
//    off_budget(세트 자체 예산, ABO)을 −금액으로 더한다.
//    단 CBO(off_scope='campaign')는 세트를 꺼도 캠페인 예산이 남아 다른 세트로 재분배되므로
//    금액에 넣지 않고 건수만 따로 적는다. 구글(디멘드젠)도 광고그룹 예산이 없어 같은 처리.
//  · 집계 대상은 '선택된 행'(적용 후에는 '실제 적용된 행') — 실제로 일어날 일만 더한다.
function abSumKey(p){
  return p.scope==='campaign'
    ? 'c|'+(p.ad_account_id||'')+'|'+(p.campaign_name||p.adset_id)
    : 'a|'+p.adset_id;
}
function abSumCalc(rows){
  const by={},seen=new Set();let offN=0,offCut=0,dup=0;
  const bucket=c=>by[c]||(by[c]={inc:0,dec:0,ni:0,nd:0,base:0});
  rows.forEach(p=>{
    const k=abSumKey(p);
    if(seen.has(k)){dup++;return}
    seen.add(k);
    if(p.field==='status'){
      if(p.after===p.before)return;             // '이미 중단됨' — 실제로 바뀌는 게 없다
      const b=Number(p.off_budget||0)/(p.offset||1);
      if(b>0){                                   // ABO: 세트 예산이 통째로 빠진다 → 감액
        const o=bucket(p.currency||'');
        o.base+=b;o.dec-=b;o.nd++;offCut++;
      }else offN++;                              // CBO·구글: 끄는 건 맞지만 예산은 남는다
      return;
    }
    if(!p.field)return;
    const b=Number(p.before),a=Number(p.after);
    if(!isFinite(a)||!isFinite(b))return;
    const off=p.offset||1,d=(a-b)/off;
    if(!d)return;
    const o=bucket(p.currency||'');
    o.base+=b/off;                       // 변경 대상들의 '현재' 예산 합 — 변동폭의 체감 기준
    if(d>0){o.inc+=d;o.ni++}else{o.dec+=d;o.nd++}
  });
  return{by:by,offN:offN,offCut:offCut,dup:dup};
}
function abSumFmt(v){return Math.abs(v).toLocaleString('ko-KR',{maximumFractionDigits:2})}
function abRenderSum(rows,applied){
  const el=document.getElementById('abSum');if(!el)return;
  const r=abSumCalc(rows),ccys=Object.keys(r.by);
  if(!ccys.length&&!r.offN){el.innerHTML='';return}
  let h='';
  ccys.forEach(c=>{
    const o=r.by[c],net=o.inc+o.dec;
    const pct=o.base>0?(net/o.base*100):null;
    h+='<div class="ab-sum-r"><span class="ab-ccy">'+abEsc(c||'—')+'</span>'
      +'<span class="ab-up">▲ 증액 '+(o.inc?'+'+abSumFmt(o.inc):'0')+'</span><span class="ab-note">'+o.ni+'건</span>'
      +'<span class="ab-dn">▼ 감액 '+(o.dec?'−'+abSumFmt(o.dec):'0')+'</span><span class="ab-note">'+o.nd+'건</span>'
      +'<span class="ab-note">· 현재 합 '+abSumFmt(o.base)+'</span>'
      +'<span class="ab-net" style="color:'+(net>0?'#0a7d32':(net<0?'#c00':'#555'))+'">'
      +(net===0?'변동 없음(증액·감액 상쇄)'
        :((applied?'실제 변동':'최종 변동')+' '+(net>0?'+':'−')+abSumFmt(net)+' '+abEsc(c)
          +(pct===null?'':' <span class="ab-note">('+(pct>0?'+':'−')+Math.abs(pct).toFixed(1)+'%)</span>')))
      +'</span></div>';
  });
  const notes=[];
  if(r.offCut)notes.push('OFF '+r.offCut+'건은 세트 예산 전액을 감액으로 반영');
  if(r.offN)notes.push('OFF '+r.offN+'건은 캠페인 예산(CBO·구글)이 남아 금액 미반영 — 중단되면 소진은 멈춤');
  if(r.dup)notes.push('같은 캠페인 예산(CBO) 중복 '+r.dup+'건은 1회만 반영');
  if(notes.length)h+='<div class="ab-sum-r"><span class="ab-note">※ '+notes.join(' · ')+'</span></div>';
  el.innerHTML=h;
}

// ── 선택 ──
function abSelBoxes(){return[...document.querySelectorAll('#abBody .ab-ck:not(:disabled)')]}
// 전체선택 대상에서 CBO 충돌·오늘 재적용 행은 뺀다 — 둘 다 켜면 서버가 양쪽을 막고,
// 재적용은 예산이 복리로 오르므로 사람이 직접 골라야 한다
function abBulkBoxes(){return abSelBoxes().filter(b=>!b.dataset.nobulk)}
function abSelIds(){return abSelBoxes().filter(b=>b.checked).map(b=>b.dataset.id)}
function abToggleAll(el){
  abBulkBoxes().forEach(b=>b.checked=el.checked);
  if(!el.checked)abSelBoxes().forEach(b=>b.checked=false);
  abSelChanged();
}
function abSelChanged(){
  if(!AB_PLAN)return;
  const boxes=abSelBoxes(),n=abSelIds().length;
  const bulk=abBulkBoxes(),bn=bulk.filter(b=>b.checked).length;
  const all=document.getElementById('abAll');
  if(all){all.checked=bulk.length>0&&bn===bulk.length;all.indeterminate=n>0&&bn<bulk.length}
  const errN=AB_PLAN.filter(p=>p.error&&!p.conflict).length;
  const go=document.getElementById('abGo');
  go.disabled=!n;
  go.textContent=n?('확인 — '+n+'건 메타에 적용'):'적용할 세트를 선택하세요';
  // 표 하단 증감 합계 — 체크를 바꿀 때마다 '선택된 것만' 다시 더한다
  const selSet=new Set(abSelIds().map(String));
  abRenderSum(AB_PLAN.filter(p=>selSet.has(String(p.adset_id))),false);
  document.getElementById('abMsg').innerHTML=boxes.length
    ? '선택 <b>'+n+'</b> / 적용 가능 '+boxes.length+'건'
      +(errN?' · <span style="color:#a00">'+errN+'건 오류(제외)</span>':'')
      +' — 되돌리려면 Ads Manager 에서 직접 수정해야 합니다'
    : '반영할 변경이 없습니다'+(errN?' · <span style="color:#a00">'+errN+'건 오류</span>':'');
}

// 예산 적용 전 비밀번호 확인. index.html 은 공개 소스라 이 값 자체는 비밀이 될 수 없다 —
// 실수로 눌러 광고비가 바뀌는 것을 막는 절차이고, 실제 권한은 Supabase Auth 로그인 +
// Edge Function 의 JWT 검증이 본다.
const AB_PW='0000';
let AB_PW_RESOLVE=null;
function abPwAsk(){
  return new Promise(res=>{
    AB_PW_RESOLVE=res;
    const el=document.getElementById('abPwInput');
    el.value='';document.getElementById('abPwErr').textContent='';
    document.getElementById('abPwMask').classList.add('show');
    setTimeout(()=>el.focus(),30);
  });
}
function abPwDone(ok){
  document.getElementById('abPwMask').classList.remove('show');
  const r=AB_PW_RESOLVE;AB_PW_RESOLVE=null;if(r)r(ok);
}
function abPwOk(){
  const el=document.getElementById('abPwInput');
  if(el.value===AB_PW){abPwDone(true);return}
  document.getElementById('abPwErr').textContent='비밀번호가 맞지 않습니다.';
  el.value='';el.focus();
}
function abPwCancel(){abPwDone(false)}
document.getElementById('abPwMask').addEventListener('click',e=>{if(e.target.id==='abPwMask')abPwCancel()});   // abMask 와 동일한 배경 클릭 닫기

function abOpenGoogle(){return abOpen('google')}
async function abOpen(src){
  AB_SRC=(src==='google')?'google':'meta';
  const g=AB_SRC==='google';
  if(!g&&!AB_MODES[MODE]){alert('예산 적용은 국내·글로벌·밴스드 세트 탭에서만 가능합니다.');return}
  const t=abTargets();
  if(!t.length){alert('증감액이 마킹된 '+(g?'광고그룹':'세트')+'이 없습니다.\n'
    +(g?'구글 디멘드젠 표에서 캠페인·세트 칸을 클릭해':'추이차트나 날짜탭에서')+' 먼저 +20/+10/-10/-20/OFF 를 마킹해 주세요.');return}
  if(!await abPwAsk())return;   // 취소하면 dry-run 조회조차 하지 않는다
  AB_PLAN=null;
  document.getElementById('abMask').classList.add('show');
  document.getElementById('abTitle').textContent=g?'⚡ 구글 예산 적용':'⚡ 메타 예산 적용';
  document.getElementById('abSub').textContent=(g?'구글 디멘드젠':AB_MODES[MODE])+' · '+t.length+'개 '+(g?'광고그룹':'세트');
  document.getElementById('abBody').innerHTML='<div style="padding:24px;text-align:center;color:#888">메타에서 현재 예산 확인 중…</div>';
  document.getElementById('abMsg').textContent='';
  const go=document.getElementById('abGo');go.disabled=true;
  go.textContent='확인 — '+(g?'구글':'메타')+'에 적용';
  try{
    const j=await abCall(true);
    AB_PLAN=j.plan||[];
    abRender(AB_PLAN,false);   // 체크박스 상태에 따라 abSelChanged 가 푸터·버튼을 채운다
  }catch(err){
    document.getElementById('abBody').innerHTML='<div style="padding:24px;color:#a00">⚠ '+abEsc(err.message||err)+'</div>';
  }
}
async function abApply(){
  if(AB_BUSY||!AB_PLAN)return;
  const ids=abSelIds();
  if(!ids.length)return;
  const picked=AB_PLAN.filter(p=>ids.includes(p.adset_id));
  const lines=picked
    .map(p=>' · '+((p.adset_name||p.adset_id).slice(0,34))+'  '+((HL_CONFIG[p.tag]||{}).label||p.tag)
      +(p.redo?'   ⚠ 오늘 이미 적용됨':'')).join('\n');
  const redoN=picked.filter(p=>p.redo).length;
  const gg=AB_SRC==='google';
  if(!confirm('선택한 '+ids.length+'개 '+(gg?'광고그룹':'세트')+'의 '+(gg?'구글':'메타')+' 예산·상태를 실제로 변경합니다.\n\n'+lines
    +(redoN?'\n\n⚠ 이 중 '+redoN+'개는 오늘 이미 적용된 세트입니다. 지금 올라간 예산 기준으로 또 계산되어 증감이 복리로 걸립니다.':'')
    +'\n\n실행 후에는 Ads Manager 에서 직접 되돌려야 합니다. 진행할까요?'))return;
  AB_BUSY=true;
  const go=document.getElementById('abGo');go.disabled=true;go.textContent='적용 중…';
  try{
    const j=await abCall(false,ids);
    abRender(j.plan||[],true);
    document.getElementById('abMsg').innerHTML='✅ <b>'+(j.applied||0)+'건</b> 적용'
      +((j.failed||0)?' · <span style="color:#a00">'+j.failed+'건 실패</span>':'')
      +' — 선택한 세트만 표시됩니다. 기록은 budget_apply_log 에 남고, 대시보드 예산 컬럼은 다음 파이프라인 실행 후 갱신됩니다.';
    document.getElementById('abCancel').textContent='닫기';
    go.style.display='none';
    AB_PLAN=null;   // 같은 계획을 두 번 적용하지 못하게 (복리 적용 방지)
  }catch(err){
    document.getElementById('abMsg').innerHTML='<span style="color:#a00">⚠ '+abEsc(err.message||err)+'</span>';
    abSelChanged();
  }
  AB_BUSY=false;
}
function abClose(){
  if(AB_BUSY)return;
  document.getElementById('abMask').classList.remove('show');
  document.getElementById('abGo').style.display='';
  document.getElementById('abCancel').textContent='취소';
  AB_PLAN=null;
}
document.getElementById('abMask').addEventListener('click',e=>{if(e.target.id==='abMask')abClose()});

// ===== DATE PRODUCT =====
const COUNTRY_LIST=['대만','홍콩','일본','싱가폴','싱가포르','멕시코','미국','한국','중국','베트남','태국','인도네시아','필리핀','말레이시아','인도'];
function extractCountry(cn){if(!cn)return'기타';for(const c of COUNTRY_LIST){if(cn.startsWith(c))return c}const p=cn.split('_')[0];return p||'기타'}
// 캠페인명이 국가 접두사로 시작하지 않으면(예: '무당_…','솔로_…') split 결과가 상품명이 됨.
// 그 경우 레코드의 country 필드(글로벌 파이프라인이 분류한 실제 국가)로 합산한다.
function countryOf(r){const c=extractCountry(r.campaign_name);if(COUNTRY_LIST.includes(c))return c;if(r.country&&COUNTRY_LIST.includes(r.country))return r.country;if(r.country)return r.country;return c}
function renderDateProduct(){
  const sel=document.getElementById('dpSel');
  const per=(document.getElementById('dpPeriod')||{}).value||'day';
  // 기간(하루/주간/월간) 바뀌면 날짜 드롭다운 옵션 재구성
  if(sel.dataset.per!==per||!sel.options.length){
    sel.innerHTML='';sel.dataset.per=per;let opts=[];
    if(per==='week'){opts=[...new Set(DATES.map(WM))].sort().reverse().map(w=>{const m=new Date(w),s=new Date(m.getTime()+6*864e5);return[w,(m.getMonth()+1)+'/'+m.getDate()+'~'+(s.getMonth()+1)+'/'+s.getDate()+' 주']})}
    else if(per==='month'){opts=[...new Set(DATES.map(d=>d.slice(0,7)))].sort().reverse().map(m=>[m,m.slice(0,4)+'.'+m.slice(5,7)+' 월'])}
    else{opts=DATES.map(d=>[d,DK(d)+' ('+WD(d)+')'])}
    opts.forEach(o=>{const e=document.createElement('option');e.value=o[0];e.textContent=o[1];sel.appendChild(e)});
  }
  const key=sel.value||(sel.options[0]&&sel.options[0].value)||'';
  const rows=AD.filter(per==='week'?(r=>WM(r.date)===key):per==='month'?(r=>r.date.slice(0,7)===key):(r=>r.date===key));
  const byP={};rows.forEach(r=>{if(!byP[r.product])byP[r.product]={product:r.product,spend:0,revenue:0,profit:0,mp:0,uc:0,imp:0,ids:new Set()};byP[r.product].spend+=r.spend;byP[r.product].revenue+=r.revenue;byP[r.product].profit+=r.profit;byP[r.product].mp+=r.results_mp;byP[r.product].uc+=r.unique_clicks;byP[r.product].imp+=(r.impressions||0);byP[r.product].ids.add(r.adset_id||r.adset_name)});
  const list=Object.values(byP).sort((a,b)=>b.spend-a.spend);
  const totS=rows.reduce((a,b)=>a+b.spend,0);
  let h='<thead><tr><th style="text-align:left">상품</th><th>세트수</th><th>지출</th><th>매출</th><th>이익</th><th>ROAS</th><th>CTR</th><th>CPM</th><th>CVR</th><th>비중</th></tr></thead><tbody>';
  list.forEach(p=>{const roas=p.spend>0?p.revenue/p.spend*100:0;const cvr=p.uc>0&&p.mp>0?p.mp/p.uc*100:0;const ratio=totS>0?p.spend/totS*100:0;const ctr=p.imp>0?p.uc/p.imp*100:0;const cpm=p.imp>0?p.spend/p.imp*1000:0;
  h+='<tr><td style="text-align:left;font-weight:600">'+p.product+'</td><td>'+p.ids.size+'</td><td style="text-align:right;color:#d00">'+money(p.spend)+'</td><td style="text-align:right;color:#00d">'+money(p.revenue)+'</td><td style="text-align:right;color:'+(p.profit>=0?'green':'red')+'">'+money(p.profit)+'</td><td class="'+RC(roas)+'">'+roas.toFixed(1)+'%</td><td>'+ctr.toFixed(2)+'%</td><td style="text-align:right">'+money(cpm)+'</td><td>'+cvr.toFixed(2)+'%</td><td>'+ratio.toFixed(1)+'%</td></tr>'});
  h+='</tbody>';document.getElementById('dpTbl').innerHTML=h;
  // === 나라별 (글로벌 모드 전용) ===
  const cWrap=document.getElementById('dpCountryWrap');
  const pLabel=document.getElementById('dpProductLabel');
  if(MODE==='gl'){
    cWrap.style.display='';pLabel.style.display='';
    const byC={};
    rows.forEach(r=>{const c=countryOf(r);if(!byC[c])byC[c]={country:c,spend:0,revenue:0,profit:0,mp:0,uc:0,imp:0,ids:new Set(),cmp:new Set()};byC[c].spend+=r.spend;byC[c].revenue+=r.revenue;byC[c].profit+=r.profit;byC[c].mp+=r.results_mp;byC[c].uc+=r.unique_clicks;byC[c].imp+=(r.impressions||0);byC[c].ids.add(r.adset_id||r.adset_name);if(r.campaign_name)byC[c].cmp.add(r.campaign_name)});
    const cList=Object.values(byC).sort((a,b)=>b.spend-a.spend);
    let ch='<thead><tr><th style="text-align:left;background:#4476b8;color:#fff">국가</th><th style="background:#4476b8;color:#fff">캠페인수</th><th style="background:#4476b8;color:#fff">세트수</th><th style="background:#4476b8;color:#fff">지출</th><th style="background:#4476b8;color:#fff">매출</th><th style="background:#4476b8;color:#fff">이익</th><th style="background:#4476b8;color:#fff">ROAS</th><th style="background:#4476b8;color:#fff">CTR</th><th style="background:#4476b8;color:#fff">CPM</th><th style="background:#4476b8;color:#fff">CVR</th><th style="background:#4476b8;color:#fff">비중</th></tr></thead><tbody>';
    cList.forEach(p=>{const roas=p.spend>0?p.revenue/p.spend*100:0;const cvr=p.uc>0&&p.mp>0?p.mp/p.uc*100:0;const ratio=totS>0?p.spend/totS*100:0;const ctr=p.imp>0?p.uc/p.imp*100:0;const cpm=p.imp>0?p.spend/p.imp*1000:0;
    ch+='<tr><td style="text-align:left;font-weight:600">'+p.country+'</td><td>'+p.cmp.size+'</td><td>'+p.ids.size+'</td><td style="text-align:right;color:#d00">'+money(p.spend)+'</td><td style="text-align:right;color:#00d">'+money(p.revenue)+'</td><td style="text-align:right;color:'+(p.profit>=0?'green':'red')+'">'+money(p.profit)+'</td><td class="'+RC(roas)+'">'+roas.toFixed(1)+'%</td><td>'+ctr.toFixed(2)+'%</td><td style="text-align:right">'+money(cpm)+'</td><td>'+cvr.toFixed(2)+'%</td><td>'+ratio.toFixed(1)+'%</td></tr>'});
    // 합계 행
    const tS=cList.reduce((a,b)=>a+b.spend,0),tR=cList.reduce((a,b)=>a+b.revenue,0),tP=cList.reduce((a,b)=>a+b.profit,0),tMp=cList.reduce((a,b)=>a+b.mp,0),tUc=cList.reduce((a,b)=>a+b.uc,0),tImp=cList.reduce((a,b)=>a+b.imp,0),tCnt=cList.reduce((a,b)=>a+b.ids.size,0),tCmp=new Set();cList.forEach(p=>p.cmp.forEach(x=>tCmp.add(x)));
    const tRoas=tS>0?tR/tS*100:0;const tCvr=tUc>0&&tMp>0?tMp/tUc*100:0;const tCtr=tImp>0?tUc/tImp*100:0;const tCpm=tImp>0?tS/tImp*1000:0;
    ch+='<tr style="background:#e8e8e8;font-weight:700"><td style="text-align:left">합계</td><td>'+tCmp.size+'</td><td>'+tCnt+'</td><td style="text-align:right;color:#d00">'+money(tS)+'</td><td style="text-align:right;color:#00d">'+money(tR)+'</td><td style="text-align:right;color:'+(tP>=0?'green':'red')+'">'+money(tP)+'</td><td class="'+RC(tRoas)+'">'+tRoas.toFixed(1)+'%</td><td>'+tCtr.toFixed(2)+'%</td><td style="text-align:right">'+money(tCpm)+'</td><td>'+tCvr.toFixed(2)+'%</td><td>100.0%</td></tr>';
    ch+='</tbody>';document.getElementById('dpCountryTbl').innerHTML=ch;
    // === 나라별 × 상품별 교차 ===
    const cpWrap=document.getElementById('dpCPWrap');
    cpWrap.style.display='';
    const byCP={};
    rows.forEach(r=>{const c=countryOf(r);const k=c+' '+r.product;if(!byCP[k])byCP[k]={country:c,product:r.product,spend:0,revenue:0,profit:0,mp:0,uc:0,imp:0,ids:new Set()};const o=byCP[k];o.spend+=r.spend;o.revenue+=r.revenue;o.profit+=r.profit;o.mp+=r.results_mp;o.uc+=r.unique_clicks;o.imp+=(r.impressions||0);o.ids.add(r.adset_id||r.adset_name)});
    const cpList=Object.values(byCP).sort((a,b)=>a.country===b.country?b.spend-a.spend:0).sort((a,b)=>{const sa=cList.findIndex(x=>x.country===a.country),sb=cList.findIndex(x=>x.country===b.country);return sa===sb?b.spend-a.spend:sa-sb});
    let cph='<thead><tr><th style="text-align:left;background:#4476b8;color:#fff">국가</th><th style="text-align:left;background:#4476b8;color:#fff">상품</th><th style="background:#4476b8;color:#fff">세트수</th><th style="background:#4476b8;color:#fff">지출</th><th style="background:#4476b8;color:#fff">매출</th><th style="background:#4476b8;color:#fff">이익</th><th style="background:#4476b8;color:#fff">ROAS</th><th style="background:#4476b8;color:#fff">CTR</th><th style="background:#4476b8;color:#fff">CPM</th><th style="background:#4476b8;color:#fff">CVR</th><th style="background:#4476b8;color:#fff">비중</th></tr></thead><tbody>';
    let prevC=null;
    cpList.forEach(p=>{const roas=p.spend>0?p.revenue/p.spend*100:0;const cvr=p.uc>0&&p.mp>0?p.mp/p.uc*100:0;const ratio=totS>0?p.spend/totS*100:0;const ctr=p.imp>0?p.uc/p.imp*100:0;const cpm=p.imp>0?p.spend/p.imp*1000:0;
    const cLabel=(p.country!==prevC)?p.country:'';prevC=p.country;
    cph+='<tr><td style="text-align:left;font-weight:700;color:#4476b8">'+cLabel+'</td><td style="text-align:left;font-weight:600">'+p.product+'</td><td>'+p.ids.size+'</td><td style="text-align:right;color:#d00">'+money(p.spend)+'</td><td style="text-align:right;color:#00d">'+money(p.revenue)+'</td><td style="text-align:right;color:'+(p.profit>=0?'green':'red')+'">'+money(p.profit)+'</td><td class="'+RC(roas)+'">'+roas.toFixed(1)+'%</td><td>'+ctr.toFixed(2)+'%</td><td style="text-align:right">'+money(cpm)+'</td><td>'+cvr.toFixed(2)+'%</td><td>'+ratio.toFixed(1)+'%</td></tr>'});
    cph+='</tbody>';document.getElementById('dpCPTbl').innerHTML=cph;
  }else{
    cWrap.style.display='none';pLabel.style.display='none';
    const cpWrap=document.getElementById('dpCPWrap');if(cpWrap)cpWrap.style.display='none';
  }
}

// ===== STRIPE =====
function renderStripe(){
  if(!STRIPE_DATA.length){document.getElementById('sTbl').innerHTML='<tr><td>Stripe 데이터 없음</td></tr>';return}
  const mode=document.getElementById('sMode')?.value||'daily';
  if(mode==='daily')return renderStripeDaily();
  if(mode==='weekly')return renderStripeWeekly();
  return renderStripeMonthly();
}
function renderStripeDaily(){
  const byDate={};STRIPE_DATA.forEach(r=>{if(!byDate[r.date])byDate[r.date]={};byDate[r.date][r.country]=r});
  const dates=[...new Set(STRIPE_DATA.map(r=>r.date))].sort().reverse();
  let h='<thead><tr><th>날짜</th><th>요일</th><th>대만 (USD)</th><th>홍콩 (USD)</th><th>일본 (USD)</th><th>태국 (USD)</th><th>싱가포르 (USD)</th><th>합계</th><th>USD/KRW</th></tr></thead><tbody>';
  dates.forEach(d=>{const bd=byDate[d]||{};const tw=bd['대만']?.revenue_usd||0;const hk=bd['홍콩']?.revenue_usd||0;const jp=bd['일본']?.revenue_usd||0;const th=bd['태국']?.revenue_usd||0;const sg=bd['싱가포르']?.revenue_usd||0;
  h+='<tr><td>'+DK(d)+'</td><td>'+WD(d)+'</td><td style="text-align:right">$'+F(tw)+'</td><td style="text-align:right">$'+F(hk)+'</td><td style="text-align:right">$'+F(jp)+'</td><td style="text-align:right">$'+F(th)+'</td><td style="text-align:right">$'+F(sg)+'</td><td style="text-align:right;font-weight:700">$'+F(tw+hk+jp+th+sg)+'</td><td>'+(bd['대만']?.usd_krw_rate||0).toFixed(2)+'</td></tr>'});
  h+='</tbody>';document.getElementById('sTbl').innerHTML=h;
}
// 주(월요일 시작) 단위로 국가별 USD 합산
function renderStripeWeekly(){
  const cs=['대만','홍콩','일본','태국','싱가포르'];
  const grp={};
  STRIPE_DATA.forEach(r=>{
    const dt=new Date(r.date);const wd=dt.getDay();const diff=dt.getDate()-wd+(wd===0?-6:1);const mon=new Date(dt);mon.setDate(diff);const mk=mon.toISOString().split('T')[0];
    if(!grp[mk])grp[mk]={대만:0,홍콩:0,일본:0,태국:0,싱가포르:0};
    grp[mk][r.country]=(grp[mk][r.country]||0)+(r.revenue_usd||0);
  });
  const keys=Object.keys(grp).sort().reverse();
  let h='<thead><tr><th>주 (월~일)</th><th>대만 (USD)</th><th>홍콩 (USD)</th><th>일본 (USD)</th><th>태국 (USD)</th><th>싱가포르 (USD)</th><th>합계</th></tr></thead><tbody>';
  keys.forEach(mk=>{const g=grp[mk];const sun=new Date(mk);sun.setDate(sun.getDate()+6);const sunK=sun.toISOString().split('T')[0];
    const tot=cs.reduce((s,c)=>s+(g[c]||0),0);
    h+='<tr><td>'+DK(mk)+' ~ '+DK(sunK)+'</td><td style="text-align:right">$'+F(g['대만'])+'</td><td style="text-align:right">$'+F(g['홍콩'])+'</td><td style="text-align:right">$'+F(g['일본'])+'</td><td style="text-align:right">$'+F(g['태국'])+'</td><td style="text-align:right">$'+F(g['싱가포르'])+'</td><td style="text-align:right;font-weight:700">$'+F(tot)+'</td></tr>'});
  h+='</tbody>';document.getElementById('sTbl').innerHTML=h;
}
// 월 단위로 국가별 USD 합산
function renderStripeMonthly(){
  const cs=['대만','홍콩','일본','태국','싱가포르'];
  const grp={};
  STRIPE_DATA.forEach(r=>{const mk=r.date.slice(0,7);
    if(!grp[mk])grp[mk]={대만:0,홍콩:0,일본:0,태국:0,싱가포르:0};
    grp[mk][r.country]=(grp[mk][r.country]||0)+(r.revenue_usd||0);
  });
  const keys=Object.keys(grp).sort().reverse();
  let h='<thead><tr><th>월</th><th>대만 (USD)</th><th>홍콩 (USD)</th><th>일본 (USD)</th><th>태국 (USD)</th><th>싱가포르 (USD)</th><th>합계</th></tr></thead><tbody>';
  keys.forEach(mk=>{const g=grp[mk];const tot=cs.reduce((s,c)=>s+(g[c]||0),0);const p=mk.split('-');
    h+='<tr><td>'+p[0]+'/'+p[1]+'</td><td style="text-align:right">$'+F(g['대만'])+'</td><td style="text-align:right">$'+F(g['홍콩'])+'</td><td style="text-align:right">$'+F(g['일본'])+'</td><td style="text-align:right">$'+F(g['태국'])+'</td><td style="text-align:right">$'+F(g['싱가포르'])+'</td><td style="text-align:right;font-weight:700">$'+F(tot)+'</td></tr>'});
  h+='</tbody>';document.getElementById('sTbl').innerHTML=h;
}

// ===== 글로벌_매출 (국가별 USD · 일별/주간/월간 토글 · 국가 드롭다운) =====
// 국가 필터(grevCountry): ALL|TW|HK|JP|TH|SG.
//   매출=Stripe(global_stripe_daily, 캡처액−환불액 순매출 · 일본=JPY·싱가포르=SGD 통화기준/그 외=빌링주소 우선) / 지출·귀속매출=GL_AD.country(캠페인명 tw·hk 태그 기준)
//   → 분류 기준이 서로 달라 국가별 ROAS·순이익은 근사. country 태그 없는 세트 지출은 국가 선택 시 빠진다.
//   대만밴스드(vnTwUsdRows)는 TW(또는 전체)일 때만 합산.
const GREV_CC={'대만':'TW','홍콩':'HK','일본':'JP','태국':'TH','싱가포르':'SG'};
function _grevCountry(){return document.getElementById('grevCountry')?.value||'ALL'}
function renderGlobalRevenue(){
  if(!STRIPE_DATA.length){document.getElementById('grevTbl').innerHTML='<tr><td>Stripe 데이터 없음</td></tr>';return}
  const mode=document.getElementById('grevMode')?.value||'daily';
  const DAYS=['일','월','화','수','목','금','토'];
  const countries=['대만','홍콩','일본','태국','싱가포르'];
  const byDate={};STRIPE_DATA.forEach(r=>{if(!byDate[r.date])byDate[r.date]={};byDate[r.date][r.country]=r});
  // 밴스드 포함/미포함 토글
  const incVan=(document.getElementById('grevVanced')?.value||'inc')==='inc';
  // 광고 지출·귀속매출 집계 (USD) — 성분별로 분리: 글로벌(타이트 GL_AD) / 대만밴스드(KRW→USD 환산)
  //   glSpend=GL_AD 지출, vanSpend=대만밴스드 지출, metaRev=GL_AD 귀속매출, vanRev=대만밴스드 귀속매출(Mixpanel)
  const glSpendByDate={},vanSpendByDate={},metaRevByDate={},vanRevByDate={};
  const cSel=_grevCountry();                       // 국가 필터
  const twOK=(cSel==='ALL'||cSel==='TW');          // 대만밴스드는 전체·대만에서만 합산
  GL_AD.forEach(r=>{if(cSel!=='ALL'&&canonCountry(r.country)!==cSel)return;
    glSpendByDate[r.date]=(glSpendByDate[r.date]||0)+(+r.spend_usd||0);metaRevByDate[r.date]=(metaRevByDate[r.date]||0)+(+r.revenue_usd||0)});
  if(twOK)vnTwUsdRows().forEach(r=>{vanSpendByDate[r.date]=(vanSpendByDate[r.date]||0)+(+r.spend||0);vanRevByDate[r.date]=(vanRevByDate[r.date]||0)+(+r.revenue||0)});
  const allDates=[...new Set(STRIPE_DATA.map(r=>r.date))].sort();
  // 주(월요일 시작) 키
  const monKey=d=>{const dt=new Date(d);const wd=dt.getDay();const diff=dt.getDate()-wd+(wd===0?-6:1);const m=new Date(dt);m.setDate(diff);return m.toISOString().split('T')[0]};
  // 기간(컬럼) 그룹핑 — 최신순(좌측이 최근)
  const groups={};const order=[];
  allDates.forEach(d=>{const k=mode==='weekly'?monKey(d):mode==='monthly'?d.slice(0,7):d;
    if(!groups[k]){groups[k]=[];order.push(k)}groups[k].push(d)});
  const keys=order.sort().reverse();
  // 기간별 집계 (국가별 매출 / 지출성분 / 귀속매출성분 / 환율평균)
  const agg={};
  keys.forEach(k=>{const o={rev:{대만:0,홍콩:0,일본:0,태국:0,싱가포르:0},total:0,glSpend:0,vanSpend:0,metaRev:0,vanRev:0,rateSum:0,rateN:0};
    groups[k].forEach(d=>{const bd=byDate[d]||{};
      countries.forEach(c=>{const v=bd[c]?.revenue_usd||0;o.rev[c]+=v;if(cSel==='ALL'||GREV_CC[c]===cSel)o.total+=v});
      o.glSpend+=glSpendByDate[d]||0;o.vanSpend+=vanSpendByDate[d]||0;
      o.metaRev+=metaRevByDate[d]||0;o.vanRev+=vanRevByDate[d]||0;
      const rate=bd['대만']?.usd_krw_rate||bd['홍콩']?.usd_krw_rate||bd['일본']?.usd_krw_rate||bd['태국']?.usd_krw_rate||bd['싱가포르']?.usd_krw_rate||0;
      if(rate){o.rateSum+=rate;o.rateN++}});
    agg[k]=o});
  // 표시값 헬퍼 (밴스드 포함/미포함 반영)
  //   매출(국가): 대만만 미포함 시 대만밴스드 귀속매출 차감 / 종합·지출도 동일 기준
  const dispCountryRev=(o,c)=>o.rev[c]-((!incVan&&c==='대만')?o.vanRev:0);
  const dispTotal=o=>incVan?o.total:(o.total-o.vanRev);
  const dispSpend=o=>incVan?(o.glSpend+o.vanSpend):o.glSpend;
  // 컬럼 라벨/일요일 강조
  const colLabel=k=>{
    if(mode==='weekly'){const sun=new Date(k);sun.setDate(sun.getDate()+6);return DK(k).slice(3)+'~'+DK(sun.toISOString().split('T')[0]).slice(3)}
    if(mode==='monthly'){const p=k.split('-');return p[0].slice(2)+'/'+p[1]}
    return DK(k).slice(3)+'('+DAYS[new Date(k).getDay()]+')';
  };
  const isSunCol=k=>mode==='daily'&&new Date(k).getDay()===0;
  // 헤더
  let h='<thead><tr><th style="min-width:80px;text-align:left;background:#4476b8;color:#fff">국가</th>';
  keys.forEach(k=>{h+='<th style="min-width:var(--cw);'+(isSunCol(k)?'background:#ffe0e0;color:#c00':'background:#4476b8;color:#fff')+'">'+colLabel(k)+'</th>'});
  h+='<th style="background:#4476b8;color:#fff">합계</th></tr></thead><tbody>';
  // 국가별 행 (미포함 시 대만은 대만밴스드 귀속매출 차감 반영)
  //   국가를 하나 고르면 아래 '종합' 행이 곧 그 나라 매출이라 중복 행은 생략
  (cSel==='ALL'?countries:[]).forEach(c=>{
    let total=0;
    h+='<tr><td style="text-align:left;font-weight:600">'+c+'</td>';
    keys.forEach(k=>{const v=dispCountryRev(agg[k],c);total+=v;h+='<td style="text-align:right">$'+F(v)+'</td>'});
    h+='<td style="text-align:right;font-weight:700">$'+F(total)+'</td></tr>';
  });
  // USD/KRW 환율 행 (참고용 · 주간/월간은 평균)
  h+='<tr style="background:#f5f5f5"><td style="text-align:left;font-weight:600">USD/KRW'+(mode==='daily'?'':' (평균)')+'</td>';
  keys.forEach(k=>{const o=agg[k];const rate=o.rateN?o.rateSum/o.rateN:0;
    h+='<td style="text-align:right">'+(rate?rate.toFixed(2):'')+'</td>'});
  h+='<td></td></tr>';
  // 종합 행 (밴스드 미포함 시 대만밴스드 귀속매출 차감)
  const cName=cSel==='ALL'?'':(Object.keys(GREV_CC).find(k=>GREV_CC[k]===cSel)||cSel);
  h+='<tr style="border-top:2px solid #000;font-weight:700"><td style="text-align:left">종합'+(cName?' · '+cName:'')+'</td>';
  let grandTotal=0;
  keys.forEach(k=>{const t=dispTotal(agg[k]);grandTotal+=t;
    h+='<td style="text-align:right">$'+F(t)+'</td>'});
  h+='<td style="text-align:right">$'+F(grandTotal)+'</td></tr>';
  // 메타지출 행 (광고지출 USD · 포함 시 GL_AD+대만밴스드, 미포함 시 GL_AD only)
  h+='<tr style="background:#f4cccc;font-weight:700"><td style="text-align:left">메타지출</td>';
  let mtTotal=0;
  keys.forEach(k=>{const spend=dispSpend(agg[k]);mtTotal+=spend;
    h+='<td style="text-align:right">$'+F(spend)+'</td>'});
  h+='<td style="text-align:right">$'+F(mtTotal)+'</td></tr>';
  // 순이익 행 (표시 종합매출 USD - 표시 광고지출 USD)
  h+='<tr style="background:#d9ead3;font-weight:700"><td style="text-align:left">순이익</td>';
  let npTotal=0;
  keys.forEach(k=>{const np=dispTotal(agg[k])-dispSpend(agg[k]);npTotal+=np;
    const sign=np<0?'-$':'$';
    h+='<td style="text-align:right;color:'+(np>=0?'green':'red')+'">'+sign+F(Math.abs(np))+'</td>'});
  const npSign=npTotal<0?'-$':'$';
  h+='<td style="text-align:right;color:'+(npTotal>=0?'green':'red')+'">'+npSign+F(Math.abs(npTotal))+'</td></tr>';
  // ROAS 행 (표시 종합매출 USD / 표시 광고지출 USD)
  h+='<tr style="background:#cfe2f3;font-weight:700"><td style="text-align:left">ROAS</td>';
  let totalSpend=0;
  keys.forEach(k=>{const rev=dispTotal(agg[k]);const spend=dispSpend(agg[k]);totalSpend+=spend;
    const roas=spend>0?rev/spend*100:0;
    const c=spend>0?(roas>=100?'green':'red'):'#999';
    h+='<td style="text-align:right;color:'+c+'">'+(spend>0?roas.toFixed(0)+'%':'')+'</td>'});
  const totalRoas=totalSpend>0?grandTotal/totalSpend*100:0;
  const trc=totalSpend>0?(totalRoas>=100?'green':'red'):'#999';
  h+='<td style="text-align:right;color:'+trc+'">'+(totalSpend>0?totalRoas.toFixed(0)+'%':'')+'</td></tr>';
  // 오가닉비율 행 — 오가닉매출(광고 미귀속) = 표시 종합 − 메타귀속매출 − (포함 시)대만밴스드 귀속매출, 비중 %
  h+='<tr style="background:#ead1dc;font-weight:700"><td style="text-align:left">오가닉비율</td>';
  let orgRevSum=0,orgBaseSum=0;
  keys.forEach(k=>{const o=agg[k];const base=dispTotal(o);
    const organic=base-o.metaRev-(incVan?o.vanRev:0);
    orgRevSum+=organic;orgBaseSum+=base;
    const pct=base>0?organic/base*100:0;
    h+='<td style="text-align:right">'+(base>0?pct.toFixed(1)+'%':'')+'</td>'});
  const orgTot=orgBaseSum>0?orgRevSum/orgBaseSum*100:0;
  h+='<td style="text-align:right">'+(orgBaseSum>0?orgTot.toFixed(1)+'%':'')+'</td></tr>';
  // 주간합계 행 (일별 모드에서만 — 주간/월간 모드는 컬럼이 이미 묶임)
  if(mode==='daily'){
    h+='<tr style="background:#fef3cd;font-weight:700"><td style="text-align:left">주간합계</td>';
    const weekTotals={};const weekShowAt={};
    keys.forEach((d,i)=>{const mk=monKey(d);if(!weekTotals[mk])weekTotals[mk]=0;weekTotals[mk]+=dispTotal(agg[d]);
      if(weekShowAt[mk]===undefined)weekShowAt[mk]=i});
    keys.forEach((d,i)=>{const mk=monKey(d);
      if(weekShowAt[mk]===i)h+='<td style="text-align:right">$'+F(weekTotals[mk])+'</td>';
      else h+='<td></td>'});
    h+='<td></td></tr>';
  }
  // 국가를 골랐는데 그 국가로 태그된 세트 지출이 0이면 경고 — 캠페인명 국가태그(tw/hk)가 없는 세트는 국가별 집계에서 빠진다.
  if(cSel!=='ALL'&&mtTotal===0){
    h+='<tr><td colspan="'+(keys.length+2)+'" style="text-align:left;background:#fff4e5;color:#a15c00;font-size:10px;padding:6px;font-weight:400">'
      +'⚠ '+(cName||cSel)+'(으)로 태그된 세트 지출이 없습니다 — 캠페인명에 국가 태그가 없는 세트는 국가별 지출·귀속매출에서 빠집니다. 매출만 참고하세요(순이익·ROAS는 지출 0 기준).</td></tr>';
  }
  h+='</tbody>';document.getElementById('grevTbl').innerHTML=h;
  renderGlobalRevenuePeriod();
}

// 글로벌 매출탭 하단 — 시작~종료 기간의 매출합 − 지출합 − 인플 − 밴스드 몫 = 순수익.
//   매출합=Stripe 국가별 실결제 USD 합(밴스드 미포함 시 대만밴스드 귀속매출 차감),
//   지출합=GL_AD 지출(+밴스드 포함 시 대만밴스드 지출). 상단 밴스드 토글·국가 드롭다운을 그대로 반영.
//   ★ 밴스드 차감(2026-08-21): 밴스드 매출은 우리 몫이라 매출합에 그대로 두고, 대신 비용 두 개를 뺀다.
//     ① 밴스드 지출(매체비)  ② 밴스드 지출 × 12% (수수료). 인플루언서 비용과 같은 성격의 항목.
//     지출합은 '글로벌(타이트) 지출'만 — 밴스드 지출은 지출합에서 빼고 별도 항목으로 세운다.
//     '밴스드 미포함' 보기에선 매출합에서 밴스드 귀속매출이 빠지므로 비용 차감도 하지 않는다(0 표기).
//   ★ 밴스드 범위 = 대만 메타(vanced_ad_performance_daily) + 대만 구글(google_campaign_daily 의
//     TW 행 = 검색광고·디멘드젠·기타, 전부 밴스드 운영). 매출탭 채널별의 glVanR 정의와 같다.
// 밴스드 수수료율(%) — 밴스드 광고 지출에 곱해 순수익에서 뺀다.
const GREV_VAN_FEE_PCT=12;
function renderGlobalRevenuePeriod(){
  const startSel=document.getElementById('grevPStart');
  const endSel=document.getElementById('grevPEnd');
  const resEl=document.getElementById('grevPResult');
  if(!startSel||!endSel||!resEl)return;
  if(!STRIPE_DATA.length){resEl.innerHTML='<span style="color:#999">Stripe 데이터 없음</span>';return}
  const countries=['대만','홍콩','일본','태국','싱가포르'];
  const allDates=[...new Set(STRIPE_DATA.map(r=>r.date))].sort();  // 오름차순
  // 드롭다운 최초 1회 채움 (최신이 위). 기본: 시작=최신-29일, 종료=최신.
  if(!startSel.options.length){
    const opts=allDates.slice().reverse().map(d=>'<option value="'+d+'">'+DK(d)+'</option>').join('');
    startSel.innerHTML=opts;endSel.innerHTML=opts;
    const last=allDates[allDates.length-1];endSel.value=last;
    const d=new Date(last);d.setDate(d.getDate()-29);const lo=d.toISOString().slice(0,10);
    startSel.value=allDates.includes(lo)?lo:allDates[0];
  }
  // 대만 구글(밴스드)은 GCAMP(지연로드 테이블)에 있다 — 아직이면 불러온 뒤 이 박스만 다시 그린다.
  const gcampReady=(GCAMP||[]).length>0;
  if(!gcampReady)ensureBigTable('gcamp').then(()=>{if((GCAMP||[]).length)renderGlobalRevenuePeriod()});
  let sd=startSel.value||allDates[0],ed=endSel.value||allDates[allDates.length-1];
  if(sd>ed){[sd,ed]=[ed,sd];startSel.value=sd;endSel.value=ed}  // 뒤집혀 있으면 자동 swap
  const incVan=(document.getElementById('grevVanced')?.value||'inc')==='inc';
  // 날짜별 성분 맵 (renderGlobalRevenue 와 동일 소스)
  const byDate={};STRIPE_DATA.forEach(r=>{if(!byDate[r.date])byDate[r.date]={};byDate[r.date][r.country]=r});
  const glSpend={},vanSpend={},vanRev={};
  const cSel=_grevCountry();                       // 상단 국가 드롭다운 반영
  const twOK=(cSel==='ALL'||cSel==='TW');
  const shownC=countries.filter(c=>cSel==='ALL'||GREV_CC[c]===cSel);
  GL_AD.forEach(r=>{if(cSel!=='ALL'&&canonCountry(r.country)!==cSel)return;glSpend[r.date]=(glSpend[r.date]||0)+(+r.spend_usd||0)});
  // 밴스드 = 대만 메타(vanced_ad_performance_daily) + 대만 구글(검색광고·디멘드젠·기타).
  //   메타/구글을 따로 담아 두는 건 결과 줄에 내역을 쪼개 보여주기 위해서다.
  const vanSpendM_={},vanRevM_={},vanSpendG_={},vanRevG_={};
  if(twOK){
    vnTwUsdRows().forEach(r=>{vanSpendM_[r.date]=(vanSpendM_[r.date]||0)+(+r.spend||0);vanRevM_[r.date]=(vanRevM_[r.date]||0)+(+r.revenue||0)});
    const g=gTwUsdByDate();
    Object.keys(g).forEach(d=>{vanSpendG_[d]=(vanSpendG_[d]||0)+g[d].s;vanRevG_[d]=(vanRevG_[d]||0)+g[d].r});
  }
  [...new Set([...Object.keys(vanSpendM_),...Object.keys(vanRevM_),...Object.keys(vanSpendG_),...Object.keys(vanRevG_)])].forEach(d=>{
    vanSpend[d]=(vanSpendM_[d]||0)+(vanSpendG_[d]||0);
    vanRev[d]=(vanRevM_[d]||0)+(vanRevG_[d]||0);
  });
  let revSum=0,spendSum=0,dayN=0;
  allDates.forEach(dt=>{if(dt<sd||dt>ed)return;dayN++;
    const bd=byDate[dt]||{};let rev=0;shownC.forEach(c=>{rev+=bd[c]?.revenue_usd||0});
    if(!incVan)rev-=(vanRev[dt]||0);
    // 지출합 = 글로벌(타이트) 지출만. 밴스드 지출은 아래에서 별도 항목으로 뺀다.
    revSum+=rev;spendSum+=(glSpend[dt]||0)});
  const infl=Math.max(0,parseFloat(document.getElementById('grevPInfl')?.value)||0);  // 인플루언서 비용(수동 입력 $)
  // 밴스드(대만) 기간 합 — vanRev/vanSpend 는 국가 선택이 ALL·TW 일 때만 채워진다(그 외엔 0).
  let vanRevSum=0,vanSpendSum=0,vanRevM=0,vanRevG=0,vanSpM=0,vanSpG=0;
  allDates.forEach(dt=>{if(dt<sd||dt>ed)return;
    vanRevSum+=(vanRev[dt]||0);vanSpendSum+=(vanSpend[dt]||0);
    vanRevM+=(vanRevM_[dt]||0);vanRevG+=(vanRevG_[dt]||0);vanSpM+=(vanSpendM_[dt]||0);vanSpG+=(vanSpendG_[dt]||0)});
  const vanFeeAll=vanSpendSum*GREV_VAN_FEE_PCT/100;
  // 미포함 보기면 매출합에서 밴스드 귀속매출이 빠져 있으니 비용도 빼지 않는다(숫자는 참고용으로 표시).
  const vanSpendCut=incVan?vanSpendSum:0, vanFee=incVan?vanFeeAll:0;
  const net=revSum-spendSum-infl-vanSpendCut-vanFee;
  // ROAS 는 총 매체비(글로벌 + 밴스드) 기준 — 매출합에 밴스드 매출이 들어 있으므로 분모도 맞춘다.
  const spendTot=spendSum+vanSpendCut;const roas=spendTot>0?revSum/spendTot*100:0;
  const cNm=cSel==='ALL'?'':(Object.keys(GREV_CC).find(k=>GREV_CC[k]===cSel)||cSel);
  resEl.innerHTML=
    '<span style="color:#888">'+dayN+'일'+(cNm?' · '+cNm:'')+'</span> &nbsp; '
    +'매출합 <b style="color:#00d">$'+F(revSum)+'</b> &nbsp;−&nbsp; '
    +'지출합 <b style="color:#d00">$'+F(spendSum)+'</b> &nbsp;−&nbsp; '
    +'인플 <b style="color:#d00">$'+F(infl)+'</b> &nbsp;−&nbsp; '
    +'밴스드지출 <b style="color:#d00">$'+F(vanSpendCut)+'</b> &nbsp;−&nbsp; '
    +'밴스드수수료 <b style="color:#d00">$'+F(vanFee)+'</b> &nbsp;=&nbsp; '
    +'순수익 <b style="color:'+(net>=0?'green':'red')+'">'+(net<0?'-$':'$')+F(Math.abs(net))+'</b>'
    +' &nbsp;<span style="color:#888" title="총 매체비(글로벌 지출 + 밴스드 지출) 기준">(ROAS '+(spendTot>0?roas.toFixed(0)+'%':'-')+')</span>'
    // 밴스드 원자료 한 줄 — 차감액이 어디서 나온 값인지 바로 대조할 수 있게
    +'<div style="color:#888;font-size:10px;margin-top:2px">'
    +'🇹🇼 밴스드 매출 $'+F(vanRevSum)+'<span style="color:#aaa">(메타 $'+F(vanRevM)+' + 구글 $'+F(vanRevG)+')</span>'
    +' · 지출 $'+F(vanSpendSum)+'<span style="color:#aaa">(메타 $'+F(vanSpM)+' + 구글 $'+F(vanSpG)+')</span>'
    +' → 수수료 '+GREV_VAN_FEE_PCT+'% $'+F(vanFeeAll)
    +(gcampReady?'':' <span style="color:#a15c00">— 구글(google_campaign_daily) 로딩 중…</span>')
    +'<span style="color:#aaa"> · 매출은 우리 몫이라 매출합에 그대로 둡니다</span>'
    +(incVan?'':' <span style="color:#a15c00">— 밴스드 미포함 보기라 매출합에서 밴스드 매출이 빠져 있어 비용도 차감하지 않음</span>')
    +(twOK?'':' <span style="color:#a15c00">— 대만 외 국가 선택이라 밴스드 없음</span>')
    +'</div>';
}

// ===== 글로벌_주간매출 (월별+주별 USD) =====
function renderGlobalWeekly(){
  if(!STRIPE_DATA.length){document.getElementById('gweekTbl').innerHTML='<tr><td>Stripe 데이터 없음</td></tr>';return}
  // 일별 USD 집계
  const dailyUSD={};
  STRIPE_DATA.forEach(r=>{
    if(!dailyUSD[r.date])dailyUSD[r.date]=0;
    dailyUSD[r.date]+=r.revenue_usd||0;
  });
  const allDates=[...new Set(STRIPE_DATA.map(r=>r.date))].sort();
  if(!allDates.length){document.getElementById('gweekTbl').innerHTML='<tr><td>데이터 없음</td></tr>';return}
  // 월 그룹
  const monthGroups={};
  allDates.forEach(d=>{const mk=d.slice(0,7);if(!monthGroups[mk])monthGroups[mk]=[];monthGroups[mk].push(d)});
  const DAYS_EN=['MON','TUE','WED','THU','FRI','SAT','SUN'];
  let h='<thead></thead><tbody>';
  Object.keys(monthGroups).sort().reverse().forEach(mk=>{
    const yr=parseInt(mk.slice(0,4)),mn=parseInt(mk.slice(5));
    const mDates=monthGroups[mk];
    const mUSD=mDates.reduce((a,d)=>a+(dailyUSD[d]||0),0);
    // 월 헤더
    h+='<tr style="background:#4476b8;color:#fff;font-weight:700"><td colspan="10" style="padding:8px">'+yr+'년 '+mn+'월 — 실제매출 $'+Math.round(mUSD).toLocaleString()+'</td></tr>';
    h+='<tr style="background:#e8e8e8;font-weight:600"><td></td><td></td>';
    DAYS_EN.forEach(d=>h+='<td style="text-align:center">'+d+'</td>');
    h+='<td style="text-align:center;font-weight:700">주간매출</td></tr>';
    // 주차별
    const firstDay=new Date(mDates[0]);const lastDay=new Date(mDates[mDates.length-1]);
    let mon=new Date(firstDay);mon.setDate(mon.getDate()-mon.getDay()+(mon.getDay()===0?-6:1));
    let weekNum=1;
    while(mon<=lastDay){
      const sun=new Date(mon.getTime()+6*864e5);
      // 날짜 행
      h+='<tr style="background:#dce6f0;font-weight:500"><td colspan="2" style="text-align:left">'+mn+'월 '+weekNum+'주차</td>';
      let weekUSD=0;
      for(let i=0;i<7;i++){const day=new Date(mon.getTime()+i*864e5);const ds=day.toISOString().split('T')[0];const inM=ds.slice(0,7)===mk;
        h+='<td style="text-align:center;font-size:9px;color:'+(inM?'inherit':'#ccc')+'">'+(inM?ds.slice(5):'')+'</td>';
        if(inM)weekUSD+=dailyUSD[ds]||0;}
      h+='<td style="text-align:center;font-weight:600">'+mn+'월 '+weekNum+'주차</td></tr>';
      // 달러 행
      h+='<tr><td style="text-align:right;font-weight:600">$'+Math.round(weekUSD).toLocaleString()+'</td><td style="color:#888">달러</td>';
      for(let i=0;i<7;i++){const day=new Date(mon.getTime()+i*864e5);const ds=day.toISOString().split('T')[0];const v=ds.slice(0,7)===mk?(dailyUSD[ds]||0):0;
        h+='<td style="text-align:right">'+(v?'$'+Math.round(v).toLocaleString():'')+'</td>'}
      h+='<td style="text-align:right;font-weight:700">$'+Math.round(weekUSD).toLocaleString()+'</td></tr>';
      mon=new Date(sun.getTime()+864e5);weekNum++;
    }
    h+='<tr><td colspan="10" style="height:10px"></td></tr>';
  });
  h+='</tbody>';document.getElementById('gweekTbl').innerHTML=h;
}

// ===== CREATIVE RANKING (소재랭킹) =====
let crankFilters={};
function clearCrankFilters(){crankFilters={};renderCreativeRanking()}

function renderCreativeRanking(){
  if(MODE!=='cr'){document.getElementById('crTbl').innerHTML='<tr><td>소재 모드에서만 사용</td></tr>';return}
  const days=parseInt(document.getElementById('crDays').value);
  const sortKey=document.getElementById('crSort').value;
  const dd=DATES.slice(0,days);
  // Aggregate by ad_id
  const byAd={};
  AD.forEach(r=>{
    if(!dd.includes(r.date))return;
    const aid=r.ad_id;if(!aid)return;
    if(!byAd[aid])byAd[aid]={ad_id:aid,ad_name:r.ad_name||'',campaign_name:r.campaign_name||'',adset_name:r.adset_name||'',product:r.product||'',spend:0,revenue:0,profit:0,mp:0,uc:0,imp:0,days:0};
    byAd[aid].spend+=r.spend;byAd[aid].revenue+=r.revenue;byAd[aid].profit+=r.profit;
    byAd[aid].mp+=r.results_mp;byAd[aid].uc+=r.unique_clicks;byAd[aid].imp+=(r.impressions||0);
    if(r.spend>0)byAd[aid].days++;
  });
  let list=Object.values(byAd).map(a=>{
    a.roas=a.spend>0?a.revenue/a.spend*100:0;
    a.cvr=a.uc>0&&a.mp>0?a.mp/a.uc*100:0;
    a.cpm=a.imp>0?a.spend/a.imp*1000:0;
    a.cpa=a.mp>0?a.spend/a.mp:0;
    a.ctr=a.imp>0?a.uc/a.imp*100:0;
    return a;
  });
  // Apply column filters
  Object.entries(crankFilters).forEach(([col,vals])=>{
    if(vals.size>0)list=list.filter(r=>vals.has(r[col]||''));
  });
  // Sort
  if(sortKey==='roas')list.sort((a,b)=>b.roas-a.roas);
  else if(sortKey==='profit')list.sort((a,b)=>b.profit-a.profit);
  else if(sortKey==='revenue')list.sort((a,b)=>b.revenue-a.revenue);
  else if(sortKey==='cvr')list.sort((a,b)=>b.cvr-a.cvr);
  else list.sort((a,b)=>b.spend-a.spend);
  // Totals
  const totS=list.reduce((a,b)=>a+b.spend,0),totR=list.reduce((a,b)=>a+b.revenue,0),totP=totR-totS;
  const totRoas=totS>0?totR/totS*100:0;
  // Collect unique values for filterable columns
  const filterCols={product:new Set(),campaign_name:new Set(),adset_name:new Set()};
  list.forEach(r=>{filterCols.product.add(r.product);filterCols.campaign_name.add(r.campaign_name);filterCols.adset_name.add(r.adset_name)});
  // Column definitions
  const cols=[
    {key:'product',label:'상품',filterable:true,fmt:v=>v},
    {key:'campaign_name',label:'캠페인',filterable:true,fmt:v=>(v||'').slice(0,25)},
    {key:'adset_name',label:'세트',filterable:true,fmt:v=>(v||'').slice(0,25)},
    {key:'ad_name',label:'소재',filterable:false,fmt:v=>(v||'').slice(0,30)},
    {key:'ad_id',label:'소재 ID',filterable:false,fmt:v=>v,style:'font-size:9px'},
    {key:'days',label:'일수',filterable:false,fmt:v=>v},
    {key:'spend',label:'지출',filterable:false,fmt:v=>money(v),style:'text-align:right;color:#d00'},
    {key:'revenue',label:'매출',filterable:false,fmt:v=>money(v),style:'text-align:right;color:#00d'},
    {key:'profit',label:'이익',filterable:false,fmt:(v)=>money(v),styleF:v=>'text-align:right;color:'+(v>=0?'green':'red')},
    {key:'roas',label:'ROAS',filterable:false,fmt:v=>v.toFixed(1)+'%',clsF:v=>RC(v)},
    {key:'cvr',label:'CVR',filterable:false,fmt:v=>v.toFixed(2)+'%'},
    {key:'cpm',label:'CPM',filterable:false,fmt:v=>money(v),style:'text-align:right'},
    {key:'cpa',label:'CPA',filterable:false,fmt:v=>money(v),style:'text-align:right'},
    {key:'ctr',label:'CTR',filterable:false,fmt:v=>v.toFixed(2)+'%'},
    {key:'mp',label:'구매',filterable:false,fmt:v=>F(v)},
  ];
  // Header
  let h='<thead><tr>';
  cols.forEach((c,ci)=>{
    const isFiltered=crankFilters[c.key]&&crankFilters[c.key].size>0;
    if(c.filterable){
      h+='<th class="filterable'+(isFiltered?' filtered':'')+'" data-col="'+c.key+'" onclick="toggleCrankFilter(event,\''+c.key+'\','+ci+')">'+c.label+'</th>';
    }else{
      h+='<th>'+c.label+'</th>';
    }
  });
  h+='</tr></thead><tbody>';
  // Summary row
  h+='<tr class="sr"><td>종합 ('+list.length+'개)</td><td></td><td></td><td></td><td></td><td></td><td style="text-align:right;color:#d00">'+money(totS)+'</td><td style="text-align:right;color:#00d">'+money(totR)+'</td><td style="text-align:right;color:'+(totP>=0?'green':'red')+'">'+money(totP)+'</td><td class="'+RC(totRoas)+'">'+totRoas.toFixed(1)+'%</td><td></td><td></td><td></td><td></td><td></td></tr>';
  // Data rows
  list.forEach(r=>{
    h+='<tr>';
    cols.forEach(c=>{
      const v=r[c.key];
      const cls=c.clsF?c.clsF(v):'';
      const sty=c.styleF?c.styleF(v):(c.style||'');
      h+='<td class="'+cls+'" style="'+sty+'">'+c.fmt(v)+'</td>';
    });
    h+='</tr>';
  });
  h+='</tbody>';
  document.getElementById('crTbl').innerHTML=h;
}

// Column filter dropdown
let activeCrankFilter=null;
let pendingCrankRender=false;
function toggleCrankFilter(e,colKey,colIdx){
  e.stopPropagation();
  const existing=document.querySelector('.col-filter.show');
  if(existing){
    existing.remove();
    if(activeCrankFilter===colKey){activeCrankFilter=null;if(pendingCrankRender){pendingCrankRender=false;renderCreativeRanking()}return}
    if(pendingCrankRender){pendingCrankRender=false;renderCreativeRanking()}
  }
  activeCrankFilter=colKey;
  const days=parseInt(document.getElementById('crDays').value);
  const dd=DATES.slice(0,days);
  const vals=new Set();
  AD.forEach(r=>{if(dd.includes(r.date)&&r[colKey])vals.add(r[colKey])});
  const sorted=[...vals].sort();
  const selected=crankFilters[colKey]||new Set();
  const div=document.createElement('div');
  div.className='col-filter show';
  div.addEventListener('click',e=>e.stopPropagation());
  let dh='<label class="cf-all"><input type="checkbox" data-role="all" data-col="'+colKey+'" '+(selected.size===0?'checked':'')+' onchange="crankToggleAll(this)"> 전체</label>';
  sorted.forEach(v=>{
    const esc=v.replace(/'/g,"\\'").replace(/"/g,'&quot;');
    dh+='<label><input type="checkbox" data-col="'+colKey+'" value="'+esc+'" '+(selected.size===0||selected.has(v)?'checked':'')+' onchange="crankToggleItem(this)"> '+v.slice(0,30)+'</label>';
  });
  dh+='<div style="border-top:1px solid #ddd;padding:4px 10px;text-align:center"><button onclick="applyCrankFilter()" style="padding:3px 16px;border:1px solid #4285f4;border-radius:3px;background:#4285f4;color:#fff;font-size:10px;cursor:pointer;font-family:inherit">적용</button></div>';
  div.innerHTML=dh;
  const th=e.target.closest('th');
  th.style.position='relative';
  th.appendChild(div);
}
function crankToggleAll(el){
  const col=el.dataset.col;
  const boxes=el.closest('.col-filter').querySelectorAll('input[type=checkbox]:not([data-role=all])');
  boxes.forEach(b=>b.checked=el.checked);
  if(el.checked){delete crankFilters[col]}else{crankFilters[col]=new Set()}
  pendingCrankRender=true;
}
function crankToggleItem(el){
  const col=el.dataset.col;
  const dd=el.closest('.col-filter');
  const boxes=dd.querySelectorAll('input[type=checkbox]:not([data-role=all])');
  const allBox=dd.querySelector('input[data-role=all]');
  const checked=new Set();boxes.forEach(b=>{if(b.checked)checked.add(b.value)});
  if(checked.size===boxes.length){if(allBox)allBox.checked=true;delete crankFilters[col]}
  else{if(allBox)allBox.checked=false;crankFilters[col]=checked}
  pendingCrankRender=true;
}
function applyCrankFilter(){
  document.querySelectorAll('.col-filter.show').forEach(d=>d.remove());
  activeCrankFilter=null;pendingCrankRender=false;
  renderCreativeRanking();
}
document.addEventListener('click',e=>{
  if(!e.target.closest('.col-filter')&&!e.target.closest('.filterable')){
    const had=document.querySelectorAll('.col-filter.show').length>0;
    document.querySelectorAll('.col-filter.show').forEach(d=>d.remove());
    activeCrankFilter=null;activeKrankFilter=null;
    if(had&&pendingCrankRender){pendingCrankRender=false;renderCreativeRanking()}
    if(had&&pendingKrankRender){pendingKrankRender=false;renderAdsetRanking()}
  }
});

// ===== ADSET RANKING (세트랭킹 — 국내·밴스드·글로벌 공통) =====
let krankFilters={};
function clearKrankFilters(){krankFilters={};renderAdsetRanking()}

function renderAdsetRanking(){
  if(MODE!=='kr'&&MODE!=='vn'&&MODE!=='gl'){document.getElementById('krTbl').innerHTML='<tr><td>세트 모드에서만 사용</td></tr>';return}
  const days=parseInt(document.getElementById('krDays').value);
  const sortKey=document.getElementById('krSort').value;
  const dd=DATES.slice(0,days);
  const byAs={};
  AD.forEach(r=>{
    if(!dd.includes(r.date))return;
    const asid=r.adset_id;if(!asid)return;
    if(!byAs[asid])byAs[asid]={adset_id:asid,adset_name:r.adset_name||'',campaign_name:r.campaign_name||'',product:r.product||'',spend:0,revenue:0,profit:0,mp:0,uc:0,imp:0,days:0};
    byAs[asid].spend+=r.spend;byAs[asid].revenue+=r.revenue;byAs[asid].profit+=r.profit;
    byAs[asid].mp+=r.results_mp;byAs[asid].uc+=r.unique_clicks;byAs[asid].imp+=(r.impressions||0);
    if(r.spend>0)byAs[asid].days++;
  });
  let list=Object.values(byAs).map(a=>{
    a.roas=a.spend>0?a.revenue/a.spend*100:0;
    a.cvr=a.uc>0&&a.mp>0?a.mp/a.uc*100:0;
    a.cpm=a.imp>0?a.spend/a.imp*1000:0;
    a.cpa=a.mp>0?a.spend/a.mp:0;
    a.ctr=a.imp>0?a.uc/a.imp*100:0;
    return a;
  });
  Object.entries(krankFilters).forEach(([col,vals])=>{
    if(vals.size>0)list=list.filter(r=>vals.has(r[col]||''));
  });
  if(sortKey==='roas')list.sort((a,b)=>b.roas-a.roas);
  else if(sortKey==='profit')list.sort((a,b)=>b.profit-a.profit);
  else if(sortKey==='revenue')list.sort((a,b)=>b.revenue-a.revenue);
  else if(sortKey==='cvr')list.sort((a,b)=>b.cvr-a.cvr);
  else list.sort((a,b)=>b.spend-a.spend);
  const totS=list.reduce((a,b)=>a+b.spend,0),totR=list.reduce((a,b)=>a+b.revenue,0),totP=totR-totS;
  const totRoas=totS>0?totR/totS*100:0;
  const cols=[
    {key:'product',label:'상품',filterable:true,fmt:v=>v},
    {key:'campaign_name',label:'캠페인',filterable:true,fmt:v=>(v||'').slice(0,25)},
    {key:'adset_name',label:'세트',filterable:false,fmt:v=>(v||'').slice(0,30)},
    {key:'adset_id',label:'세트 ID',filterable:false,fmt:v=>v,style:'font-size:9px'},
    {key:'days',label:'일수',filterable:false,fmt:v=>v},
    {key:'spend',label:'지출',filterable:false,fmt:v=>money(v),style:'text-align:right;color:#d00'},
    {key:'revenue',label:'매출',filterable:false,fmt:v=>money(v),style:'text-align:right;color:#00d'},
    {key:'profit',label:'이익',filterable:false,fmt:v=>money(v),styleF:v=>'text-align:right;color:'+(v>=0?'green':'red')},
    {key:'roas',label:'ROAS',filterable:false,fmt:v=>v.toFixed(1)+'%',clsF:v=>RC(v)},
    {key:'cvr',label:'CVR',filterable:false,fmt:v=>v.toFixed(2)+'%'},
    {key:'cpm',label:'CPM',filterable:false,fmt:v=>money(v),style:'text-align:right'},
    {key:'cpa',label:'CPA',filterable:false,fmt:v=>money(v),style:'text-align:right'},
    {key:'ctr',label:'CTR',filterable:false,fmt:v=>v.toFixed(2)+'%'},
    {key:'mp',label:'구매',filterable:false,fmt:v=>F(v)},
  ];
  let h='<thead><tr>';
  cols.forEach(c=>{
    const isFiltered=krankFilters[c.key]&&krankFilters[c.key].size>0;
    if(c.filterable){
      h+='<th class="filterable'+(isFiltered?' filtered':'')+'" data-col="'+c.key+'" onclick="toggleKrankFilter(event,\''+c.key+'\')">'+c.label+'</th>';
    }else{h+='<th>'+c.label+'</th>'}
  });
  h+='</tr></thead><tbody>';
  h+='<tr class="sr"><td>종합 ('+list.length+'개)</td><td></td><td></td><td></td><td></td><td style="text-align:right;color:#d00">'+money(totS)+'</td><td style="text-align:right;color:#00d">'+money(totR)+'</td><td style="text-align:right;color:'+(totP>=0?'green':'red')+'">'+money(totP)+'</td><td class="'+RC(totRoas)+'">'+totRoas.toFixed(1)+'%</td><td></td><td></td><td></td><td></td><td></td></tr>';
  list.forEach(r=>{
    h+='<tr>';
    cols.forEach(c=>{
      const v=r[c.key];const cls=c.clsF?c.clsF(v):'';const sty=c.styleF?c.styleF(v):(c.style||'');
      h+='<td class="'+cls+'" style="'+sty+'">'+c.fmt(v)+'</td>';
    });
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('krTbl').innerHTML=h;
}

// ===== 🧬 복제·변형 계보 (국내·밴스드·글로벌 공통) =====
// 세트 '이름'에 남는 마커로 원본 / 복제(증액용 복사) / 변형(tROAS·결과당비용 등 최적화 변경)을
// 분류하고, 마커·날짜토큰을 걷어낸 '계보 키'로 한 가족(원본+파생)을 묶는다.
//
// ★ 판정은 adset_name 만 본다. campaign_name 의 '- 사본'은 캠페인을 복제한 흔적이라
//   세트 복제가 아니다(예: '바람기_260703_신혼 | 바람기_0613_전환캠페인 - 사본').
// ★ 날짜 토큰(260418 / 0421 / 250311)은 키에서 제거한다. 복제·변형은 원본과 날짜가
//   다른 경우가 많아(원본 260420 → 복제 260422) 날짜를 남기면 가족이 갈라진다.
// 마커를 새로 쓰기 시작하면 아래 두 배열에만 추가하면 된다.
const DV_DUP=[
  {rx:/\[\s*복제\s*\]/,tag:'복제'},
  {rx:/\s*-\s*(사본|복사본|copy)\s*$/i,tag:'사본'},
  {rx:/\s*-\s*복제증액\s*$/,tag:'복제증액'},
];
// 변형 마커는 이름 '꼬리'에 구분자와 함께 붙은 것만 인정한다.
//   (원본 소재명에 우연히 섞인 단어의 오분류 방지 — 예 '자체UGC파트너십실험'의 '실험')
const DV_VAR=[
  {rx:/[_\-\s]+troas(실험)?$/i,tag:'tROAS'},
  {rx:/[_\-\s]+구매당(비용)?(변경|전환)?$/,tag:'구매당비용'},
  {rx:/[_\-\s]+결과당비용(전환|변경|목표)?$/,tag:'결과당비용'},
  {rx:/[_\-\s]+(기존)?구매자\s*제외(실험)?$/,tag:'구매자제외'},
  {rx:/[_\-\s]+(테스트|test)$/i,tag:'테스트'},
  {rx:/[_\-\s]+전세계중국어$/,tag:'전세계중국어'},
  // 전세계한국어 = 국내 위닝 세트를 해외 한국어 인벤토리로 넓힌 변형(2026-07-21~). 한국제외는 성과가
  // 크게 달라서(CPM은 더 싸지만 CPA 악화) 태그를 따로 둔다 — 같은 계보 안에서 A/B로 비교되게.
  {rx:/[_\-\s]+전세계한국어[_\-\s(（]*한국\s*제외[)）]*$/,tag:'전세계한국어(한국제외)'},
  {rx:/[_\-\s]+전세계한국어$/,tag:'전세계한국어'},
];
const DV_EMOJI=/[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE0F}\u{200D}]/gu;
const DV_DATE=/^\d{4}(\d{2})?(\d{2})?$/;   // 0421 · 260418 · 19930920

function dvStripAll(s,rx){let p;do{p=s;s=s.replace(rx,'')}while(s!==p);return s}

// 세트 이름 → {kind:'orig'|'dup'|'var', tags:[], key, label}
function dvClassify(name){
  const s=String(name||'').trim();
  const tags=[];let kind='orig';
  const stripVar=w=>{
    for(let i=0;i<4;i++){
      let hit=null;
      for(const v of DV_VAR){if(v.rx.test(w)){w=w.replace(v.rx,'');hit=v.tag;break}}
      if(!hit)break;
      if(tags.indexOf(hit)<0)tags.push(hit);
      kind='var';
    }
    return w;
  };
  let work=stripVar(s);
  let dup=false;
  for(const d of DV_DUP){
    if(d.rx.test(work)){dup=true;if(tags.indexOf(d.tag)<0)tags.push(d.tag);work=dvStripAll(work,d.rx)}
  }
  if(dup){
    // 배수 표기(x2/x4/ x8)는 복제 마커가 있을 때만 제거 — 일반 이름의 'x2' 오제거 방지
    work=work.replace(/\s*[xX]\s*\d+/g,'');
    work=dvStripAll(dvStripAll(work,DV_DUP[0].rx),DV_DUP[1].rx);
    work=stripVar(work);   // xN 제거로 꼬리에 드러난 변형 마커 재수거(예 '[복제]…_전세계중국어x4')
  }
  // 복제+변형 동시 보유 → '복제'로 센다(증액 목적의 복제가 1차 성격, 변형은 태그로 표시)
  if(dup)kind='dup';
  const toks=work.replace(DV_EMOJI,'').split(/[_\s]+/).filter(t=>t&&!DV_DATE.test(t));
  const label=toks.join('_');
  return {kind:kind,tags:tags,key:label.toLowerCase().replace(/[^0-9a-z가-힣%]/g,''),label:label};
}

function dvBadge(tags){
  if(!tags||!tags.length)return'';
  return ' '+tags.map(t=>'<span style="display:inline-block;padding:0 4px;border-radius:7px;background:#eef2ff;color:#3730a3;font-size:8px;font-weight:700;vertical-align:1px">'+t+'</span>').join('');
}
function dvMemberHtml(m,th){
  const pass=th>0&&m.budget>=th;   // 예산 하한을 넘긴 세트 = 이 가족이 표에 오른 근거
  const bud=m.budget>0
    ?'<span style="'+(pass?'font-weight:700;color:#b45309':'color:#888')+'">예산 '+money(m.budget)+'</span>'
    :'<span style="color:#bbb">예산 -</span>';
  return '<div style="padding:3px 0;border-top:1px dotted #ddd">'
    +'<div style="font-weight:600;line-height:1.3;overflow-wrap:anywhere">'+abEsc(m.adset_name)+dvBadge(m.tags)+'</div>'
    +'<div style="font-size:9px;color:#888;font-family:Consolas,monospace">'+abEsc(m.adset_id)+'</div>'
    +'<div style="font-size:9px;color:#555">'+bud+' · 지출 '+money(m.spend)+' · 매출 '+money(m.revenue)
    +' · <span class="'+RC(m.roas)+'" style="padding:0 3px;border-radius:2px">ROAS '+m.roas.toFixed(0)+'%</span></div>'
    +'</div>';
}

// ── 버블맵(마인드맵) 렌더 ─────────────────────────────────────────────
// 원본 ●──[복제 n]──● ● ●
//      └─[변형 n]──● ●
// 버블 크기 = 지출(가족 안에서 상대), 색 = ROAS, 굵은 주황 테두리 = 일예산 하한 통과.
const DV_MAP={origX:96,hubX:250,leafX:392,textX:428,width:980,rowH:58,minH:118};
function dvRoasColor(r){
  if(!r)return'#94a3b8';
  if(r<70)return'#dc2626';if(r<100)return'#f97316';
  if(r<150)return'#84cc16';if(r<200)return'#22c55e';return'#15803d';
}
function dvR(spend,max,lo,hi){   // 지출 → 반지름(면적 비례에 가깝게 sqrt)
  if(!(max>0)||!(spend>0))return lo;
  return lo+(hi-lo)*Math.sqrt(Math.min(1,spend/max));
}
function dvCurve(x1,y1,x2,y2,col,w){
  const mx=(x1+x2)/2;
  return '<path d="M'+x1+','+y1+' C'+mx+','+y1+' '+mx+','+y2+' '+x2+','+y2+'" fill="none" stroke="'+col+'" stroke-width="'+w+'" opacity="0.55"/>';
}
// 버블 1개 + 오른쪽 라벨(이름 / 예산·지출·ROAS)
function dvBubble(m,cx,cy,r,branchCol,th,money){
  const pass=th>0&&m.budget>=th;
  const fill=dvRoasColor(m.roas);
  const nm=(m.adset_name||'');   // 이름은 자르지 않는다 — SVG 폭을 dvFamilyCard 에서 이름 길이에 맞춰 늘린다
  const tip=(m.adset_name||'')+'\n'+m.adset_id+'\n예산 '+(m.budget>0?money(m.budget):'-')
    +' · 지출 '+money(m.spend)+' · 매출 '+money(m.revenue)+' · ROAS '+m.roas.toFixed(0)+'%';
  let s='<g><title>'+abEsc(tip)+'</title>';
  s+='<circle cx="'+cx+'" cy="'+cy+'" r="'+r.toFixed(1)+'" fill="'+fill+'" fill-opacity="0.88" stroke="'+(pass?'#b45309':branchCol)+'" stroke-width="'+(pass?3:1.5)+'"/>';
  // 작은 버블도 ROAS 는 항상 찍는다(빈 원처럼 보이면 미완성으로 읽힘)
  s+='<text x="'+cx+'" y="'+(cy+(r>=20?4:3))+'" text-anchor="middle" font-size="'+(r>=22?11:(r>=17?9.5:8))+'" font-weight="700" fill="#fff">'+m.roas.toFixed(0)+'</text>';
  s+='<text x="'+DV_MAP.textX+'" y="'+(cy-3)+'" font-size="10.5" font-weight="600" fill="#1f2937">'+abEsc(nm)+dvTagText(m.tags)+'</text>';
  s+='<text x="'+DV_MAP.textX+'" y="'+(cy+10)+'" font-size="9" fill="#6b7280">'
    +(m.budget>0?'<tspan fill="'+(pass?'#b45309':'#6b7280')+'" font-weight="'+(pass?'700':'400')+'">예산 '+abEsc(money(m.budget))+'</tspan>':'예산 -')
    +' · 지출 '+abEsc(money(m.spend))+' · ROAS '+m.roas.toFixed(0)+'%</text>';
  return s+'</g>';
}
function dvTagText(tags){
  if(!tags||!tags.length)return'';
  return '<tspan font-size="9" font-weight="400" fill="#6366f1">  '+abEsc(tags.join('/'))+'</tspan>';
}
function dvHub(x,y,label,n,col){
  const w=label.length*11+26;
  return '<g><rect x="'+(x-w/2)+'" y="'+(y-10)+'" width="'+w+'" height="20" rx="10" fill="'+col+'"/>'
    +'<text x="'+x+'" y="'+(y+4)+'" text-anchor="middle" font-size="10" font-weight="700" fill="#fff">'+label+' '+n+'</text></g>';
}

// 세트 이름은 중간에서 끊지 않는다 — 글로벌 세트명이 길어 '…' 로 잘리면 어느 세트인지 구분이 안 된다.
// 잎 라벨은 SVG 폭을 이름에 맞춰 늘려서(카드가 overflow-x:auto) 전부 보이게 하고,
// 가운데 정렬인 원본 캡션은 카드 왼쪽을 넘지 않도록 여러 줄로 접는다.
function dvTextW(s,fs){   // 문자폭 추정: 한글·CJK 1em, 그 외 0.55em
  let w=0;
  for(const ch of String(s||'')){const c=ch.codePointAt(0);w+=(c>0x1100&&!(c>=0x2000&&c<0x2600))?1:0.55}
  return w*fs;
}
function dvWrapText(s,maxW,fs){   // 토큰(공백·_·|·[]·/) 경계 우선, 그래도 길면 문자 단위로 접는다
  s=String(s||'');
  if(!s)return[];
  const toks=s.match(/[^\s_|\[\]\/]+|[\s_|\[\]\/]+/g)||[s];
  const lines=[];let cur='';
  const flush=()=>{if(cur)lines.push(cur);cur=''};
  toks.forEach(t=>{
    if(dvTextW(cur+t,fs)<=maxW){cur+=t;return}
    flush();
    let rest=t;
    while(dvTextW(rest,fs)>maxW&&rest.length>1){
      let i=1;while(i<rest.length&&dvTextW(rest.slice(0,i+1),fs)<=maxW)i++;
      lines.push(rest.slice(0,i));rest=rest.slice(i);
    }
    cur=rest;
  });
  flush();
  return lines.map(l=>l.replace(/^\s+|\s+$/g,'')).filter(l=>l.length);
}

function dvFamilyCard(f,budOn,budMin,money){
  const dup=f.dup,vr=f.var,leaves=dup.length+vr.length;
  const maxSp=Math.max.apply(null,[].concat(f.orig,dup,vr).map(m=>m.spend).concat([1]));
  const oR=f.orig.length?dvR(f.orig[0].spend,maxSp,20,34):22;
  // 원본 캡션(버블 아래·가운데 정렬)은 카드 왼쪽을 넘을 수 없으니 폭에 맞춰 줄바꿈한다.
  const capW=(DV_MAP.origX-6)*2;
  const capLines=f.orig.length?dvWrapText(f.orig[0].adset_name||'',capW,9.5)
                              :dvWrapText(f.label||'',capW,8.5);
  const capH=f.orig.length?(13+capLines.length*11+(f.orig.length>1?11:0)+6)
                          :(15+capLines.length*10+6);
  // 캡션 줄수만큼 카드 높이를 늘려 아래로 잘리지 않게 한다.
  const H=Math.max(DV_MAP.minH,26+leaves*DV_MAP.rowH,2*(oR+capH+4));
  const oy=H/2;
  // 잎 라벨(전체 이름)이 들어갈 만큼 가로폭 확보 — 넘치면 카드가 가로 스크롤된다.
  let W=DV_MAP.width;
  [].concat(dup,vr).forEach(m=>{
    const w=DV_MAP.textX+dvTextW(m.adset_name||'',10.5)
      +((m.tags&&m.tags.length)?dvTextW('  '+m.tags.join('/'),9):0)+18;
    if(w>W)W=w;
  });
  W=Math.ceil(W);
  let s='<svg width="'+W+'" height="'+H+'" style="display:block">';
  // 가지별 y 배치: 복제 위, 변형 아래
  const ys=[];for(let i=0;i<leaves;i++)ys.push(26+i*DV_MAP.rowH+DV_MAP.rowH/2-8);
  const dupYs=ys.slice(0,dup.length),varYs=ys.slice(dup.length);
  // 원본 → 허브 → 잎
  [[dup,dupYs,'#1a73e8','🧬 복제'],[vr,varYs,'#7c3aed','🔀 변형']].forEach(([arr,yy,col,lab])=>{
    if(!arr.length)return;
    const hy=(yy[0]+yy[yy.length-1])/2;
    s+=dvCurve(DV_MAP.origX+oR,oy,DV_MAP.hubX-((lab.length*11+26)/2),hy,col,3);
    yy.forEach((y,i)=>{
      const r=dvR(arr[i].spend,maxSp,13.5,26);
      s+=dvCurve(DV_MAP.hubX+((lab.length*11+26)/2),hy,DV_MAP.leafX-r,y,col,1.6);
    });
    s+=dvHub(DV_MAP.hubX,hy,lab,arr.length+'개',col);
    yy.forEach((y,i)=>{s+=dvBubble(arr[i],DV_MAP.leafX,y,dvR(arr[i].spend,maxSp,13.5,26),col,budOn?budMin:0,money)});
  });
  // 원본 버블
  if(f.orig.length){
    // 캡션은 버블 중심 기준 가운데 정렬 — 길면 자르지 말고 여러 줄로 접어 이름 전체를 보여준다.
    const o=f.orig[0];
    s+='<g><title>'+abEsc((o.adset_name||'')+'\n'+o.adset_id+'\n예산 '+(o.budget>0?money(o.budget):'-')+' · 지출 '+money(o.spend)+' · ROAS '+o.roas.toFixed(0)+'%')+'</title>';
    s+='<circle cx="'+DV_MAP.origX+'" cy="'+oy+'" r="'+oR.toFixed(1)+'" fill="'+dvRoasColor(o.roas)+'" fill-opacity="0.95" stroke="#0f172a" stroke-width="2"/>';
    s+='<text x="'+DV_MAP.origX+'" y="'+(oy+4)+'" text-anchor="middle" font-size="12" font-weight="700" fill="#fff">'+o.roas.toFixed(0)+'</text>';
    capLines.forEach((ln,i)=>{
      s+='<text x="'+DV_MAP.origX+'" y="'+(oy+oR+13+i*11)+'" text-anchor="middle" font-size="9.5" font-weight="700" fill="#1f2937">'+abEsc(ln)+'</text>';
    });
    const by=oy+oR+13+capLines.length*11;
    s+='<text x="'+DV_MAP.origX+'" y="'+by+'" text-anchor="middle" font-size="8.5" fill="#6b7280">'+abEsc(o.budget>0?'예산 '+money(o.budget):'예산 -')+'</text>';
    if(f.orig.length>1)s+='<text x="'+DV_MAP.origX+'" y="'+(by+11)+'" text-anchor="middle" font-size="8.5" fill="#b91c1c">동명 원본 +'+(f.orig.length-1)+'</text>';
    s+='</g>';
  }else{
    s+='<g><title>'+abEsc('원본 미확인 — 추정 계보명 '+f.label)+'</title>'
      +'<circle cx="'+DV_MAP.origX+'" cy="'+oy+'" r="22" fill="#f8fafc" stroke="#c60" stroke-width="2" stroke-dasharray="4 3"/>'
      +'<text x="'+DV_MAP.origX+'" y="'+(oy+4)+'" text-anchor="middle" font-size="14" fill="#c60">?</text>'
      +'<text x="'+DV_MAP.origX+'" y="'+(oy+37)+'" text-anchor="middle" font-size="9" font-weight="700" fill="#c60">원본 미확인</text>'
      +capLines.map((ln,i)=>'<text x="'+DV_MAP.origX+'" y="'+(oy+48+i*10)+'" text-anchor="middle" font-size="8.5" fill="#94a3b8">'+abEsc(ln)+'</text>').join('')
      +'</g>';
  }
  s+='</svg>';
  const oo=f.orig[0];
  const head='<div style="display:flex;align-items:baseline;gap:8px;margin-bottom:2px;flex-wrap:wrap">'
    +'<span style="background:#1a2744;color:#fff;font-size:9.5px;font-weight:700;padding:1px 7px;border-radius:9px">'+abEsc(f.product||'-')+'</span>'
    +(oo?'<span style="font-size:11.5px;font-weight:700;color:#111">'+abEsc(oo.adset_name)+'</span>'
        +'<span style="font-size:9px;color:#9ca3af;font-family:Consolas,monospace">'+abEsc(oo.adset_id)+'</span>'
       :'<span style="font-size:11.5px;font-weight:700;color:#c60">(원본 미확인) '+abEsc(f.label)+'</span>')
    +'<span style="font-size:9.5px;color:#6b7280">복제 '+f.dup.length+' · 변형 '+f.var.length
    +' · 가족 지출 '+abEsc(money(f.spend))+' · 매출 '+abEsc(money(f.revenue))+'</span>'
    +'<span class="'+RC(f.roas)+'" style="font-size:9.5px;padding:0 5px;border-radius:3px;font-weight:700">ROAS '+f.roas.toFixed(0)+'%</span></div>';
  return '<div style="border:1px solid #e5e7eb;border-radius:10px;padding:9px 12px 4px 12px;margin-bottom:10px;background:#fff;overflow-x:auto">'+head+s+'</div>';
}

function dvRenderMap(list,budOn,budMin){
  const box=document.getElementById('dvMap');
  if(!list.length){box.innerHTML='<div style="padding:20px;color:#888;font-size:12px">조건에 맞는 가족이 없습니다.</div>';return}
  const legend='<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:9.5px;color:#555;margin-bottom:8px;padding:6px 10px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px">'
    +'<b style="color:#111">읽는 법</b>'
    +'<span>버블 크기 = 지출</span>'
    +'<span>숫자 = ROAS%</span>'
    +'<span>색 '+[['#dc2626','~70'],['#f97316','~100'],['#84cc16','~150'],['#22c55e','~200'],['#15803d','200+']].map(c=>'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:'+c[0]+';margin:0 2px 0 6px;vertical-align:-1px"></span>'+c[1]).join('')+'</span>'
    +(budOn?'<span style="color:#b45309;font-weight:700">굵은 주황 테두리 = 일예산 '+money(budMin)+' 이상</span>':'')
    +'<span style="color:#888">버블에 마우스를 올리면 전체 이름·ID</span></div>';
  box.innerHTML=legend+list.map(f=>dvFamilyCard(f,budOn,budMin,money)).join('');
}

// 일예산 하한 기본값 — 모드 통화 기준(국내·밴스드 ₩40만 / 글로벌은 대략 환산한 $300).
// 모드를 바꾸면 해당 모드 기본값으로 되돌리고, 같은 모드 안에서 고친 값은 유지한다.
const DV_BUDGET_DEF={kr:400000,vn:400000,gl:300};
let dvBudgetMode=null;
function dvSyncBudget(){
  const el=document.getElementById('dvBudget');
  if(dvBudgetMode!==MODE){dvBudgetMode=MODE;el.value=DV_BUDGET_DEF[MODE]!=null?DV_BUDGET_DEF[MODE]:0}
  document.getElementById('dvBudgetCur').textContent=MODE==='gl'?'USD':'원';
  return Math.max(0,parseFloat(el.value)||0);
}

function renderDupVar(){
  const tbl=document.getElementById('dvTbl');
  if(MODE!=='kr'&&MODE!=='vn'&&MODE!=='gl'){
    tbl.innerHTML='<tr><td>세트 모드(국내·밴스드·글로벌)에서만 사용</td></tr>';
    document.getElementById('dvMap').innerHTML='<div style="padding:20px;color:#888;font-size:12px">세트 모드(국내·밴스드·글로벌)에서만 사용</div>';
    return;
  }
  const days=parseInt(document.getElementById('dvDays').value);
  const sortKey=document.getElementById('dvSort').value;
  const kw=(document.getElementById('dvFilter').value||'').trim().toLowerCase();
  const showOrphan=document.getElementById('dvOrphan').checked;
  const budMin=dvSyncBudget();
  const dd={};DATES.slice(0,days).forEach(d=>dd[d]=1);

  // 1) 세트 단위 집계 (세트랭킹과 동일한 방식)
  //    예산은 일자별로 오르내리므로 '기간 내 최고 일예산'으로 본다 — 감액·중단된 뒤에도
  //    그 세트가 어느 규모까지 밀어봤는지가 복제·변형 판단에 필요한 정보라서.
  const byAs={};
  AD.forEach(r=>{
    if(!dd[r.date])return;
    const id=r.adset_id;if(!id)return;
    if(!byAs[id])byAs[id]={adset_id:id,adset_name:r.adset_name||'',product:r.product||'',spend:0,revenue:0,budget:0,last:''};
    const a=byAs[id];
    a.spend+=r.spend;a.revenue+=r.revenue;
    const b=+r.budget||0;if(b>a.budget)a.budget=b;
    if(!a.adset_name&&r.adset_name)a.adset_name=r.adset_name;
    if(r.date>a.last)a.last=r.date;
  });

  // 2) 계보 키로 가족 묶기
  const fam={};
  Object.values(byAs).forEach(a=>{
    const c=dvClassify(a.adset_name);
    if(!c.key)return;                       // 이름이 날짜·마커뿐이면 계보 판정 불가 → 제외
    a.tags=c.tags;a.roas=a.spend>0?a.revenue/a.spend*100:0;
    if(!fam[c.key])fam[c.key]={key:c.key,label:c.label,product:'',orig:[],dup:[],var:[]};
    const f=fam[c.key];
    f[c.kind].push(a);
    if(!f.product&&a.product)f.product=a.product;
    if(!f.label||c.label.length>f.label.length)f.label=c.label;
  });

  // 3) 파생(복제·변형)이 하나라도 있는 가족만 — 사용자가 보려는 건 '변형된 것'뿐
  let list=Object.values(fam).filter(f=>f.dup.length||f.var.length);
  const famTotal=list.length;
  // 4) 일예산 하한 — 복제·변형 중 예산이 하한 이상인 세트가 하나도 없으면 표에 올리지 않는다.
  //    ★ 원본 예산은 자격 판단에 넣지 않는다("복제든 변형이든 … 하나도 없으면" 규칙).
  //    예산 데이터가 아예 없는 모드에서는 전부 사라지지 않게 필터를 자동 해제한다.
  const anyBudget=Object.values(byAs).some(a=>a.budget>0);
  const budOn=budMin>0&&anyBudget;
  if(budOn)list=list.filter(f=>f.dup.concat(f.var).some(m=>m.budget>=budMin));
  const budCut=famTotal-list.length;
  const bySpend=(x,y)=>y.spend-x.spend;
  list.forEach(f=>{
    f.orig.sort(bySpend);f.dup.sort(bySpend);f.var.sort(bySpend);
    f.spend=[].concat(f.orig,f.dup,f.var).reduce((s,m)=>s+m.spend,0);
    f.revenue=[].concat(f.orig,f.dup,f.var).reduce((s,m)=>s+m.revenue,0);
    f.roas=f.spend>0?f.revenue/f.spend*100:0;
    f.orphan=!f.orig.length;
  });
  const orphanN=list.filter(f=>f.orphan).length;
  if(!showOrphan)list=list.filter(f=>!f.orphan);
  if(kw)list=list.filter(f=>[].concat(f.orig,f.dup,f.var).some(m=>(m.adset_name||'').toLowerCase().indexOf(kw)>=0)||(f.product||'').toLowerCase().indexOf(kw)>=0);
  if(sortKey==='dup')list.sort((a,b)=>b.dup.length-a.dup.length||b.spend-a.spend);
  else if(sortKey==='var')list.sort((a,b)=>b.var.length-a.var.length||b.spend-a.spend);
  else if(sortKey==='total')list.sort((a,b)=>(b.dup.length+b.var.length)-(a.dup.length+a.var.length)||b.spend-a.spend);
  else list.sort(bySpend);

  const dupN=list.reduce((s,f)=>s+f.dup.length,0),varN=list.reduce((s,f)=>s+f.var.length,0);
  document.getElementById('dvInfo').innerHTML='파생 가족 '+famTotal+'개 중 표시 '+list.length+'개'
    +(budOn?' <span style="color:#b45309">(일예산 '+money(budMin)+' 미만만 있는 '+budCut+'개 제외)</span>':'')
    +(!anyBudget&&budMin>0?' <span style="color:#c00">(예산 데이터 없음 → 예산 조건 미적용)</span>':'')
    +' · 원본 미확인 '+orphanN+' · 복제 '+dupN+' · 변형 '+varN;

  // 보기 전환: 버블맵(마인드맵) / 표
  const view=document.getElementById('dvView').value;
  document.getElementById('dvTblWrap').style.display=(view==='table')?'':'none';
  document.getElementById('dvMap').style.display=(view==='table')?'none':'';
  if(view!=='table'){dvRenderMap(list,budOn,budMin);return}

  let h='<thead><tr>'
    +'<th style="min-width:60px">상품</th>'
    +'<th style="min-width:230px">원본 세트</th>'
    +'<th style="min-width:250px">🧬 복제 (증액용 복사)'+(budOn?'<br><span style="font-weight:400;color:#b45309">일예산 '+money(budMin)+'↑ 굵게</span>':'')+'</th>'
    +'<th style="min-width:250px">🔀 변형 (tROAS·결과당비용 등)'+(budOn?'<br><span style="font-weight:400;color:#b45309">일예산 '+money(budMin)+'↑ 굵게</span>':'')+'</th>'
    +'<th style="min-width:95px">가족 합계</th>'
    +'</tr></thead><tbody>';
  h+='<tr class="sr"><td>종합 ('+list.length+'가족)</td>'
    +'<td>원본 확인 '+list.filter(f=>!f.orphan).length+' · 미확인 '+list.filter(f=>f.orphan).length+'</td>'
    +'<td>복제 '+dupN+'개</td><td>변형 '+varN+'개</td>'
    +'<td style="text-align:right">'+money(list.reduce((s,f)=>s+f.spend,0))+'</td></tr>';

  list.forEach(f=>{
    const cellSty='font-size:10px;text-align:left;vertical-align:top;max-width:330px;overflow-wrap:anywhere';
    h+='<tr style="vertical-align:top">';
    h+='<td style="font-size:10px;font-weight:600;text-align:left;vertical-align:top">'+abEsc(f.product||'-')+'</td>';
    // 원본
    if(f.orig.length){
      h+='<td style="'+cellSty+'">'+f.orig.map(m=>dvMemberHtml(m,0)).join('')
        +(f.orig.length>1?'<div style="font-size:9px;color:#a00;margin-top:2px">※ 동명 원본 '+f.orig.length+'개(계정 이동·재생성 추정)</div>':'')
        +'</td>';
    }else{
      h+='<td style="'+cellSty+';color:#999"><div style="font-weight:600;color:#c60">(원본 미확인)</div>'
        +'<div style="font-size:9px;line-height:1.4;margin-top:2px">추정 계보명<br><b style="color:#666">'+abEsc(f.label)+'</b></div>'
        +'<div style="font-size:9px;margin-top:2px">원본이 조회 기간 밖이거나 이름이 크게 바뀐 경우</div></td>';
    }
    // 복제 / 변형
    [['dup','#1a73e8'],['var','#7c3aed']].forEach(([k,col])=>{
      const arr=f[k];
      if(!arr.length){h+='<td style="font-size:10px;color:#bbb;vertical-align:top">—</td>';return}
      h+='<td style="'+cellSty+'"><div style="font-weight:700;color:'+col+';font-size:10px">'+arr.length+'개</div>'
        +arr.map(m=>dvMemberHtml(m,budOn?budMin:0)).join('')+'</td>';
    });
    h+='<td style="font-size:10px;text-align:right;vertical-align:top">'+money(f.spend)
      +'<div style="font-size:9px;color:#888">매출 '+money(f.revenue)+'</div>'
      +'<div class="'+RC(f.roas)+'" style="font-size:9px;padding:0 3px;border-radius:2px;display:inline-block;margin-top:2px">ROAS '+f.roas.toFixed(0)+'%</div></td>';
    h+='</tr>';
  });
  h+='</tbody>';
  tbl.innerHTML=h;
}

let activeKrankFilter=null;
let pendingKrankRender=false;
function toggleKrankFilter(e,colKey){
  e.stopPropagation();
  const existing=document.querySelector('.col-filter.show');
  if(existing){
    existing.remove();
    if(activeKrankFilter===colKey){activeKrankFilter=null;if(pendingKrankRender){pendingKrankRender=false;renderAdsetRanking()}return}
    if(pendingKrankRender){pendingKrankRender=false;renderAdsetRanking()}
  }
  activeKrankFilter=colKey;
  const days=parseInt(document.getElementById('krDays').value);
  const dd=DATES.slice(0,days);
  const vals=new Set();
  AD.forEach(r=>{if(dd.includes(r.date)&&r[colKey])vals.add(r[colKey])});
  const sorted=[...vals].sort();
  const selected=krankFilters[colKey]||new Set();
  const div=document.createElement('div');div.className='col-filter show';
  div.addEventListener('click',e=>e.stopPropagation());
  let dh='<label class="cf-all"><input type="checkbox" data-role="all" data-col="'+colKey+'" '+(selected.size===0?'checked':'')+' onchange="krankToggleAll(this)"> 전체</label>';
  sorted.forEach(v=>{
    const esc=v.replace(/'/g,"\\'").replace(/"/g,'&quot;');
    dh+='<label><input type="checkbox" data-col="'+colKey+'" value="'+esc+'" '+(selected.size===0||selected.has(v)?'checked':'')+' onchange="krankToggleItem(this)"> '+v.slice(0,30)+'</label>';
  });
  dh+='<div style="border-top:1px solid #ddd;padding:4px 10px;text-align:center"><button onclick="applyKrankFilter()" style="padding:3px 16px;border:1px solid #4285f4;border-radius:3px;background:#4285f4;color:#fff;font-size:10px;cursor:pointer;font-family:inherit">적용</button></div>';
  div.innerHTML=dh;
  const th=e.target.closest('th');th.style.position='relative';th.appendChild(div);
}
function krankToggleAll(el){
  const col=el.dataset.col;
  const boxes=el.closest('.col-filter').querySelectorAll('input[type=checkbox]:not([data-role=all])');
  boxes.forEach(b=>b.checked=el.checked);
  if(el.checked){delete krankFilters[col]}else{krankFilters[col]=new Set()}
  pendingKrankRender=true;
}
function krankToggleItem(el){
  const col=el.dataset.col;
  const dd=el.closest('.col-filter');
  const boxes=dd.querySelectorAll('input[type=checkbox]:not([data-role=all])');
  const allBox=dd.querySelector('input[data-role=all]');
  const checked=new Set();boxes.forEach(b=>{if(b.checked)checked.add(b.value)});
  if(checked.size===boxes.length){if(allBox)allBox.checked=true;delete krankFilters[col]}
  else{if(allBox)allBox.checked=false;krankFilters[col]=checked}
  pendingKrankRender=true;
}
function applyKrankFilter(){
  document.querySelectorAll('.col-filter.show').forEach(d=>d.remove());
  activeKrankFilter=null;pendingKrankRender=false;
  renderAdsetRanking();
}

// ===== SEARCH =====
let searchMatches=[],searchIdx=-1;
function searchRun(){const q=document.getElementById('searchInput').value.trim();const info=document.getElementById('searchInfo');
document.querySelectorAll('mark.sh').forEach(m=>{m.parentNode.replaceChild(document.createTextNode(m.textContent),m);m.parentNode?.normalize?.()});
searchMatches=[];searchIdx=-1;if(!q){info.textContent='';return}
const panel=document.querySelector('.panel.active');if(!panel)return;
const walker=document.createTreeWalker(panel,NodeFilter.SHOW_TEXT,null,false);const nodes=[];
while(walker.nextNode()){const n=walker.currentNode;if(['SCRIPT','STYLE','INPUT','TEXTAREA'].includes(n.parentNode.tagName))continue;if(n.textContent.toLowerCase().includes(q.toLowerCase()))nodes.push(n)}
const re=new RegExp('('+q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','gi');
nodes.forEach(n=>{const span=document.createElement('span');span.innerHTML=n.textContent.replace(re,'<mark class="sh">$1</mark>');n.parentNode.replaceChild(span,n)});
searchMatches=panel.querySelectorAll('mark.sh');info.textContent=searchMatches.length?'1/'+searchMatches.length:'0건';
if(searchMatches.length){searchIdx=0;searchHighlight()}}
function searchHighlight(){searchMatches.forEach((m,i)=>m.classList.toggle('active',i===searchIdx));if(searchMatches[searchIdx])searchMatches[searchIdx].scrollIntoView({behavior:'smooth',block:'center'});document.getElementById('searchInfo').textContent=(searchIdx+1)+'/'+searchMatches.length}
function searchNav(dir){if(!searchMatches.length)return;searchIdx=(searchIdx+dir+searchMatches.length)%searchMatches.length;searchHighlight()}
function searchClear(){document.getElementById('searchInput').value='';document.querySelectorAll('mark.sh').forEach(m=>{m.parentNode.replaceChild(document.createTextNode(m.textContent),m);m.parentNode?.normalize?.()});searchMatches=[];searchIdx=-1;document.getElementById('searchInfo').textContent=''}
let sTimer;document.addEventListener('DOMContentLoaded',()=>{const si=document.getElementById('searchInput');if(si){si.addEventListener('input',()=>{clearTimeout(sTimer);sTimer=setTimeout(searchRun,300)});si.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();searchNav(e.shiftKey?-1:1)}if(e.key==='Escape')searchClear()})}});
document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&e.key==='f'){e.preventDefault();document.getElementById('searchInput')?.focus()}});

// ===== CHANNEL REVENUE (매출 탭 - 채널별 추이차트) =====
// 국내모드(KR)에서만 의미. 권역 드롭다운(chrScope: 전체/국내/글로벌)으로 상세 채널을 필터.
// 전체 종합은 항상 맨 위 고정. 국내 종합은 국내 탭, 글로벌 종합은 글로벌 탭에만 표시.
//   국내 종합:  매출=국내 실결제[HIST_REVENUE(2026-03-31 이전, 시트 포트원+페이플+토스) / toss_daily_revenue(2026-04-01 이후)],
//               지출=국내메타+밴스드+네이버+구글+구글디멘드젠
//   글로벌 종합: 매출=Stripe revenue_usd × usd_krw_rate(KRW, 실결제), 지출=대만밴스드+글로벌메타(GL_AD)
//               └ 상세행 '글로벌'=GL_AD 귀속 지출+매출(메타), '대만 밴스드'=VN_TW 귀속 — Stripe(실결제)와 구분
//   전체 종합:  국내 종합 + 글로벌 종합 (매출·지출 모두 합산, 중복 없음)
//   국내 메타:  ad_performance_daily              (KR_AD)
//   밴스드:     vanced_ad_performance_daily       (VN_AD, 대만계정 제외)
//   대만 밴스드: vanced_ad_performance_daily       (VN_TW_ACC 단독, KRW)
//   글로벌:     타이트 글로벌 지출(GL_AD.spend_usd) + Stripe 매출(revenue_usd), USD→KRW 환산 (대만밴스드 제외)
//   네이버:     naver_sa_daily.cost_vat (지출) + naver_daily_mp.revenue (매출: Mixpanel attribution)
//   구글:       google_ads_daily                  (GOOGLE_ADS)
// HIST_REVENUE: 매출 대시보드 시트(1uiMN2bBNOt4qU9H86JzxMs_D6PeaIWOtz3IMtJqu4aI)에서 추출한
//   2025-09-01 ~ 2026-03-31 일별 매출 합계 (포트원+페이플+토스). 토스 단독 데이터는 PG 마이그레이션 이전
//   기간이라 거의 0이므로 시트값으로 대체.
const HIST_REVENUE={"2025-09-01":7483000,"2025-09-02":6441600,"2025-09-03":5852000,"2025-09-04":5476500,"2025-09-05":5879500,"2025-09-06":4138400,"2025-09-07":8896000,"2025-09-08":6452100,"2025-09-09":5371200,"2025-09-10":6821400,"2025-09-11":5598900,"2025-09-12":6611500,"2025-09-13":7182100,"2025-09-14":8416700,"2025-09-15":6254600,"2025-09-16":7283800,"2025-09-17":8364500,"2025-09-18":7780600,"2025-09-19":7285800,"2025-09-20":8484600,"2025-09-21":8723000,"2025-09-22":7427100,"2025-09-23":8258910,"2025-09-24":8684906,"2025-09-25":7390027,"2025-09-26":7491000,"2025-09-27":10266000,"2025-09-28":12476110,"2025-09-29":10654200,"2025-09-30":8924500,"2025-10-01":10485500,"2025-10-02":8036000,"2025-10-03":11903900,"2025-10-04":12940300,"2025-10-05":14910000,"2025-10-06":14563800,"2025-10-07":14223100,"2025-10-08":13754100,"2025-10-09":14540700,"2025-10-10":12921300,"2025-10-11":15786700,"2025-10-12":15084000,"2025-10-13":9808000,"2025-10-14":12183300,"2025-10-15":11238000,"2025-10-16":11174700,"2025-10-17":9885500,"2025-10-18":11321700,"2025-10-19":14913400,"2025-10-20":10597600,"2025-10-21":9675700,"2025-10-22":9788500,"2025-10-23":8744400,"2025-10-24":9410400,"2025-10-25":11411600,"2025-10-26":14018800,"2025-10-27":11364300,"2025-10-28":10304200,"2025-10-29":9922300,"2025-10-30":10750388,"2025-10-31":8777688,"2025-11-01":10291366,"2025-11-02":13387686,"2025-11-03":11009398,"2025-11-04":12890810,"2025-11-05":10442476,"2025-11-06":11035208,"2025-11-07":12182154,"2025-11-08":12645842,"2025-11-09":14659730,"2025-11-10":11060918,"2025-11-11":13250918,"2025-11-12":11413064,"2025-11-13":12377242,"2025-11-14":13331520,"2025-11-15":15954686,"2025-11-16":20480174,"2025-11-17":13882286,"2025-11-18":11529142,"2025-11-19":10253054,"2025-11-20":10399410,"2025-11-21":12958698,"2025-11-22":12992588,"2025-11-23":16022286,"2025-11-24":13726206,"2025-11-25":10248974,"2025-11-26":11974020,"2025-11-27":13496310,"2025-11-28":12890510,"2025-11-29":13565688,"2025-11-30":15433264,"2025-12-01":12742084,"2025-12-02":12700120,"2025-12-03":11498760,"2025-12-04":12328430,"2025-12-05":12808310,"2025-12-06":16744196,"2025-12-07":27379040,"2025-12-08":20897280,"2025-12-09":18133380,"2025-12-10":17217126,"2025-12-11":17864594,"2025-12-12":17247642,"2025-12-13":23888816,"2025-12-14":29031350,"2025-12-15":26823690,"2025-12-16":26833820,"2025-12-17":25637308,"2025-12-18":27906094,"2025-12-19":26883278,"2025-12-20":31507950,"2025-12-21":37463586,"2025-12-22":26974730,"2025-12-23":24996220,"2025-12-24":25389208,"2025-12-25":27744568,"2025-12-26":25605376,"2025-12-27":29843948,"2025-12-28":35863234,"2025-12-29":26532358,"2025-12-30":26950008,"2025-12-31":29830862,"2026-01-01":45048664,"2026-01-02":33473488,"2026-01-03":35732118,"2026-01-04":50036734,"2026-01-05":34452915,"2026-01-06":31823992,"2026-01-07":33943848,"2026-01-08":31062336,"2026-01-09":30122222,"2026-01-10":37994052,"2026-01-11":46455368,"2026-01-12":38148264,"2026-01-13":34129222,"2026-01-14":32977196,"2026-01-15":31031874,"2026-01-16":30198316,"2026-01-17":37744146,"2026-01-18":46571042,"2026-01-19":39784314,"2026-01-20":32577406,"2026-01-21":32970392,"2026-01-22":30321722,"2026-01-23":28804026,"2026-01-24":34814196,"2026-01-25":42843947,"2026-01-26":32206386,"2026-01-27":32412938,"2026-01-28":31198624,"2026-01-29":29426508,"2026-01-30":30380534,"2026-01-31":38476036,"2026-02-01":45162278,"2026-02-02":38030440,"2026-02-03":32748636,"2026-02-04":30915336,"2026-02-05":31717142,"2026-02-06":27371024,"2026-02-07":37374600,"2026-02-08":40860766,"2026-02-09":33921942,"2026-02-10":29681976,"2026-02-11":29454007,"2026-02-12":33686593,"2026-02-13":37304407,"2026-02-14":48118986,"2026-02-15":61805984,"2026-02-16":73735492,"2026-02-17":79577124,"2026-02-18":83248004,"2026-02-19":57115298,"2026-02-20":46217732,"2026-02-21":48098678,"2026-02-22":56279730,"2026-02-23":39310572,"2026-02-24":32853254,"2026-02-25":34569902,"2026-02-26":25772054,"2026-02-27":25938354,"2026-02-28":29267578,"2026-03-01":51146742,"2026-03-02":34576538,"2026-03-03":39274582,"2026-03-04":36383805,"2026-03-05":30051238,"2026-03-06":33046923,"2026-03-07":39834161,"2026-03-08":50627354,"2026-03-09":37617534,"2026-03-10":34404694,"2026-03-11":32824700,"2026-03-12":31751188,"2026-03-13":38570448,"2026-03-14":47196672,"2026-03-15":62309874,"2026-03-16":46967070,"2026-03-17":43084451,"2026-03-18":44367597,"2026-03-19":44145878,"2026-03-20":43998480,"2026-03-21":50151914,"2026-03-22":56221862,"2026-03-23":39829976,"2026-03-24":36443656,"2026-03-25":32672296,"2026-03-26":30640700,"2026-03-27":27170608,"2026-03-28":39756230,"2026-03-29":40218408,"2026-03-30":32924460,"2026-03-31":32252564};
function renderChannelRevenue(){
  const view=document.getElementById('chrView')?.value||'daily';
  if(view==='hourly'){ensureKrRev4h(_chrevHourly);return}   // 시간별: 4시간 버킷(별도 데이터·매출만)
  // 디멘드젠(타이트) 행 = GGDG_TIGHT(lazy) → 신선 로드 전이면 트리거 후 완료 시 재렌더(표+도넛).
  //   캐시(GGDG_TIGHT.length>0)가 있어도 세션 첫 진입엔 재검증(stale-while-revalidate, 국내탭과 동일).
  if(!(window._BIG_LOADED&&window._BIG_LOADED.ggdgkr)){
    ensureBigTable('ggdgkr').then(()=>{const at=document.querySelector('.tab.active');if(at&&at.dataset.t==='chrev'){renderChannelRevenue();renderChannelDonut();}});
  }
  // 틱톡 행 = 구글시트(TIKTOK). 미로드면 스냅샷으로 먼저 그리고, 시트 도착 후 재렌더(표+도넛).
  if(typeof TT_LOADED!=='undefined'&&!TT_LOADED){
    loadTiktok().then(()=>{const at=document.querySelector('.tab.active');if(at&&at.dataset.t==='chrev'){renderChannelRevenue();renderChannelDonut();}});
  }
  // 구글 5분할 행 = google_campaign_daily(lazy). 도착 전엔 구 소스(시트 검색광고+[Tight]DG)로 폴백 렌더.
  if(!(window._BIG_LOADED&&window._BIG_LOADED.gcamp)){
    ensureBigTable('gcamp').then(()=>{const at=document.querySelector('.tab.active');if(at&&at.dataset.t==='chrev'){renderChannelRevenue();renderChannelDonut();}});
  }
  // CRM 채널에 MP ch=kakao 매출을 합치려면 kr_channel_revenue_4h(채널='카카오')가 필요 → 먼저 로드.
  ensureKrRev4h(()=>{
    const count=parseInt(document.getElementById('chrDays').value);
    const channels=_chrevChannels();
    if(view==='daily')_chrevDaily(channels,count);
    else _chrevPeriod(channels,view,count);
  });
}

// ===== 시간별(4시간 버킷) 국내 채널 매출 — kr_channel_revenue_4h (Mixpanel 귀속, 매출만) =====
// 일별/주별/월별과 달리 지출·ROAS 없음(4시간 단위 채널 지출 원천이 없음). 셀=매출(₩)만.
// 시트기반 채널(구글검색·네이버파워링크)은 일단위라 제외 — 여기 '네이버'는 Mixpanel 귀속분(utm/referrer/ch=naver).
let KR_REV4H=[],_KR_REV4H_LOADED=false;
function ensureKrRev4h(cb){
  if(_KR_REV4H_LOADED){cb&&cb();return}
  sbQ('kr_channel_revenue_4h','select=*&order=date.desc,bucket.desc')
    .then(d=>{KR_REV4H=d||[];_KR_REV4H_LOADED=true;cb&&cb();})
    .catch(()=>{KR_REV4H=[];cb&&cb();});   // 실패(테이블 미생성 등)는 캐시 안 함 → 다음 선택 시 재시도
}
function _chrevHourly(){
  const days=parseInt(document.getElementById('chrDays')?.value)||3;
  const scope=document.getElementById('chrScope')?.value||'all';
  const BK=[0,4,8,12,16,20];
  const bkLabel=b=>String(b).padStart(2,'0')+'-'+String(b+4).padStart(2,'0');
  // 밴스드 제외 시 밴스드 채널 행을 빼고 종합에서도 제외 (일별 _chrevChannels 와 동일 규칙)
  const incVan=(document.getElementById('chrVanced')?.value||'inc')==='inc';
  const DOM=['국내 메타','밴스드 국내','네이버','디멘드젠(타이트)','메타(기타)'].filter(c=>incVan||c!=='밴스드 국내');
  const GL=['대만 밴스드','글로벌(밴스드 제외)'].filter(c=>incVan||c!=='대만 밴스드');
  // lut[date][bucket][channel] = {r:매출, c:건수, s:지출}
  const lut={};
  (KR_REV4H||[]).forEach(row=>{const d=row.date,b=+row.bucket,c=row.channel;if(!lut[d])lut[d]={};if(!lut[d][b])lut[d][b]={};const o=lut[d][b][c]||(lut[d][b][c]={r:0,c:0,s:0});o.r+=(+row.revenue||0);o.c+=(+row.purchase_count||0);o.s+=(+row.spend||0)});
  const cell=(d,b,c)=>((lut[d]||{})[b]||{})[c]||{r:0,c:0,s:0};
  const sumOf=(d,b,list)=>{let r=0,c=0,s=0;list.forEach(ch=>{const v=cell(d,b,ch);r+=v.r;c+=v.c;s+=v.s});return{r,c,s}};
  // 셀: 지출 있으면 추이차트처럼 ROAS/순이익/매출/지출, 없으면(네이버·구글) 매출+건수.
  const CELLrev=(rev,cnt)=>(!rev&&!cnt)?'':'<div class="rv" style="font-size:11px;font-weight:700">'+moneyKRW(rev)+'</div><div class="cv" style="color:#888">'+(cnt||0)+'건</div>';
  const INNER=(v)=>{if(v.s>0){const roas=v.r/v.s*100;return{cls:RC(roas),html:MC_CH(roas,v.r-v.s,v.s,v.r)}}return{cls:'',html:CELLrev(v.r,v.c)}};
  // scope 별 행 구성 (일별 _chrevChannels 와 동일 규칙): 전체종합 상단 고정 + 권역종합/채널
  const mk=ch=>({name:ch,get:(d,b)=>cell(d,b,ch)});
  const sumAll={name:'전체 종합',sum:true,get:(d,b)=>sumOf(d,b,DOM.concat(GL))};
  // 날짜(최근→과거), 각 날짜 내 버킷도 최근(20)→과거(0) → 최신이 왼쪽(일별과 동일 방향)
  const today=new Date();const dates=[];
  for(let i=0;i<days;i++){const d=new Date(today);d.setDate(today.getDate()-i);dates.push(d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'))}
  const cols=[];
  dates.forEach(d=>{[...BK].reverse().forEach(b=>{cols.push({date:d,bucket:b,key:d+'#'+b,label:DK(d).slice(3)+' '+bkLabel(b),dayStart:b===20})})});
  // 채널 나열 = ①권역(국내→글로벌) ②소속(우리→밴스드) 그룹 → 그룹 안에서 표시기간 매출 내림차순
  //   (일별 뷰 _chrevSortByRev 와 동일 규칙, 종합행 상단 고정 / 그룹 첫 행은 점선 구분선)
  const revOfCh=ch=>{let r=0;cols.forEach(col=>{r+=cell(col.date,col.bucket,ch).r});return r};
  const IS_VAN=n=>n==='밴스드 국내'||n==='밴스드 구글'||n==='대만 밴스드';
  const grpOf=ch=>(DOM.indexOf(ch)>=0?'dom':'gl')+(IS_VAN(ch)?'_van':'_us');
  const gRank=ch=>CHR_GRP_ORDER.indexOf(grpOf(ch));
  const byRev=list=>[...list].sort((a,b)=>gRank(a)-gRank(b)||revOfCh(b)-revOfCh(a));
  const mkRows=list=>{let prev=null;return byRev(list).map(ch=>{const r=mk(ch);r.grp=grpOf(ch);r.grpTop=(prev!==null&&r.grp!==prev);prev=r.grp;return r})};
  let rows;
  if(scope==='dom')rows=[sumAll,{name:'국내 종합',sum:true,get:(d,b)=>sumOf(d,b,DOM)},...mkRows(DOM)];
  else if(scope==='gl')rows=[sumAll,{name:'글로벌 종합',sum:true,get:(d,b)=>sumOf(d,b,GL)},...mkRows(GL)];
  else rows=[sumAll,...mkRows(DOM.concat(GL))];
  // 헤더 (버킷 20이 각 날짜 블록의 왼쪽 시작 → 구분선)
  const ths=cols.map(col=>'<th style="min-width:var(--cw);'+(col.dayStart?'border-left:2px solid #9bb5d4;':'')+'">'+col.label+'</th>').join('');
  let h='<thead><tr>'+CHR_TH_GRP+'<th style="min-width:100px;text-align:left">채널</th>'+ths+'</tr></thead><tbody>';
  const grpCells=_chrGrpCells(rows);
  rows.forEach((row,ri)=>{
    const nameBg=row.sum?'#c5d6ea':'#dce6f0';
    h+='<tr'+(row.sum?' class="sr" style="border-top:2px solid #9bb5d4"':(row.grpTop?' style="border-top:2px dashed #b9c9db"':''))+'>'+grpCells[ri]+'<td class="fx fx0" style="background:'+nameBg+';font-weight:700;padding:4px 6px">'+row.name+'</td>';
    cols.forEach(col=>{const ix=INNER(row.get(col.date,col.bucket));const bg=row.sum?'background:#eef3f9;':'';h+='<td class="mc '+(row.sum?'':ix.cls)+'" style="'+bg+(col.dayStart?'border-left:2px solid #9bb5d4;':'')+'">'+ix.html+'</td>'});
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('chrTbl').innerHTML=h;
  requestAnimationFrame(()=>_fixSticky(document.getElementById('chrTbl')));
  // 하단 누적 막대차트 (채널별 매출) — 종합행 제외, scope 반영
  const chList=(scope==='dom'?byRev(DOM):scope==='gl'?byRev(GL):byRev(DOM.concat(GL)));
  const channels=chList.map(c=>({name:c,get:k=>{const p=k.split('#');return {s:0,r:cell(p[0],+p[1],c).r}}}));
  const periods=cols.map(col=>({label:col.label,dates:[col.key]}));
  _chrevChart(channels,periods);
}

function _chrevChannels(){
  // 채널별 일자 합산
  const byDateKR={},byDateVN={},byDateVNTW={},byDateGL={},byDateStripe={},byDatePL={},byDateGG={},byDateGGDG={},byDateTOSS={};
  KR_AD.forEach(r=>{if(!byDateKR[r.date])byDateKR[r.date]={s:0,r:0};byDateKR[r.date].s+=(r.spend||0);byDateKR[r.date].r+=(r.revenue||0)});
  // 밴스드: 국내 밴스드 메타(2계정)와 대만 밴스드(VN_TW_ACC)를 분리 — 대만은 해외라 별도 row 로 표시
  VN_AD.forEach(r=>{const m=String(r.ad_account_id||'')===VN_TW_ACC?byDateVNTW:byDateVN;if(!m[r.date])m[r.date]={s:0,r:0};m[r.date].s+=(r.spend||0);m[r.date].r+=(r.revenue||0)});
  // 글로벌(메타) 상세: 타이트 글로벌 GL_AD 의 지출(spend_usd)+귀속매출(revenue_usd), USD→KRW 환산. 대만 밴스드는 별도 row.
  GL_AD.forEach(r=>{const rate=usdKrwRateAt(r.date)||1380;if(!byDateGL[r.date])byDateGL[r.date]={s:0,r:0};byDateGL[r.date].s+=(+r.spend_usd||0)*rate;byDateGL[r.date].r+=(+r.revenue_usd||0)*rate});
  // Stripe 실 결제 매출: 글로벌 종합 매출로만 사용 (대만밴스드/글로벌메타 귀속매출과 구분 위해 상세 채널엔 미포함)
  STRIPE_DATA.forEach(r=>{const rate=(+r.usd_krw_rate||usdKrwRateAt(r.date))||1380;byDateStripe[r.date]=(byDateStripe[r.date]||0)+(+r.revenue_usd||0)*rate});
  // 네이버 파워링크 (시트 '00. 네이버/구글 Daily'): 지출=브랜드+일반, 매출=총 구매전환값.
  //   브랜드/일반은 지출·매출 모두 시트에 따로 있어 매출탭에서 2개 채널로 쪼갠다.
  const byDatePLb={},byDatePLg={};
  NAVER_PL.forEach(r=>{
    byDatePL[r.date]={s:(r.cost_vat||0),r:(r.revenue||0)};
    byDatePLb[r.date]={s:(+r.brand_cost||0),  r:(+r.brand_revenue||0)};
    byDatePLg[r.date]={s:(+r.general_cost||0),r:(+r.general_revenue||0)};
  });
  GOOGLE_ADS.forEach(r=>{byDateGG[r.date]={s:(r.cost_vat||0),r:(r.revenue||0)}});
  // 디멘드젠(타이트) = 국내탭 '🟢 구글 디멘드젠' 종합과 동일 소스(google_demandgen_campaign_daily: 지출=Ads API·매출=MP귀속).
  //   구 소스 GOOGLE_DG(google_demandgen_daily, 시트 지출·구글전환값)는 매출 기준이 달라 폐기.
  GGDG_TIGHT.forEach(r=>{if(!byDateGGDG[r.date])byDateGGDG[r.date]={s:0,r:0};byDateGGDG[r.date].s+=(+r.spend||0);byDateGGDG[r.date].r+=(+r.revenue||0)});
  // 틱톡(국내) = 🎵 틱톡 탭과 동일 소스(구글시트 '틱톡 캠페인 추이차트' → TIKTOK, 미로드 시 스냅샷).
  //   지출=틱톡 광고관리자 실지출, 매출=MP 결제완료(utm_id 귀속 — 광고그룹ID/캠페인ID 혼재). 국내 집행만 있어 국내 채널로 둔다.
  const byDateTT={};
  (typeof TIKTOK!=='undefined'?TIKTOK:[]).forEach(r=>{if(!byDateTT[r.date])byDateTT[r.date]={s:0,r:0};byDateTT[r.date].s+=(+r.spend||0);byDateTT[r.date].r+=(+r.revenue||0)});
  // 구글(전 캠페인) = google_campaign_daily → (국가 × 유형 × 소유) 버킷.
  //   지출=Ads API campaign×date(전 유형), 매출=MP utm_campaign(=campaign.id) 귀속·비KRW는 KRW 환산.
  //   국내 디멘드젠만 [Tight](우리)/[Vanced](밴스드)로 분리 — 나머지 구글 캠페인은 전부 밴스드 운영.
  //   대만(캠페인명 TW 태그)은 글로벌 권역으로 보낸다(대만 결제는 Stripe 실결제라 글로벌 종합과 정합).
  //   ★ 미로드/실패(gcOK=false)면 구 소스(시트 검색광고 google_ads_daily + [Tight]DG)로 폴백 — 조용한 0 방지.
  const gB={};
  const _gkey=r=>{
    const c=(r.country==='TW')?'TW':'KR', t=r.channel_type||'';
    if(t==='SEARCH')return c+'_SEARCH';
    if(t==='DEMAND_GEN')return c==='KR'?('KR_DG_'+(r.owner==='tight'?'T':'V')):'TW_DG';
    if(t==='PERFORMANCE_MAX'&&c==='KR')return 'KR_PMAX';
    return c+'_ETC';   // GDN(DISPLAY)·VIDEO·대만 PMAX 등 — 지출이 사라지지 않게 '기타'로 보존
  };
  (GCAMP||[]).forEach(r=>{const k=_gkey(r);const m=gB[k]||(gB[k]={});const o=m[r.date]||(m[r.date]={s:0,r:0});o.s+=(+r.spend||0);o.r+=(+r.revenue||0)});
  const gcOK=(GCAMP||[]).length>0;
  const gGet=k=>(d=>((gB[k]||{})[d]||{s:0,r:0}));
  const gHas=k=>Object.keys(gB[k]||{}).length>0;
  const gSum=(d,keys)=>{let s=0,r=0;keys.forEach(k=>{const v=(gB[k]||{})[d];if(v){s+=v.s;r+=v.r}});return{s,r}};
  // KR_ETC(GDN·동영상 등 미분류)는 매출탭에서 제외 — 행·종합 양쪽에서 함께 빼야 채널 합 = 종합이 맞는다.
  const G_KR_ALL=['KR_SEARCH','KR_DG_T','KR_DG_V','KR_PMAX'];
  const G_KR_VAN=['KR_SEARCH','KR_DG_V','KR_PMAX'];   // 국내 구글 중 밴스드 운영분
  const G_TW_ALL=['TW_SEARCH','TW_DG','TW_ETC'];               // 대만 구글은 전부 밴스드 운영
  // CRM(알림톡): alimtalk_daily_campaign 을 일자별로 합산. 매출=Σrev(귀속), 지출=Σcost(발송비용=sent×13원).
  const byDateCRM={};
  ALIMTALK.forEach(r=>{if(!byDateCRM[r.date])byDateCRM[r.date]={s:0,r:0};byDateCRM[r.date].s+=(+r.cost||0);byDateCRM[r.date].r+=(+r.rev||0)});
  // MP ch=kakao 매출도 CRM 에 포함 — kr_channel_revenue_4h 채널='카카오' 를 일자별 합산(매출만, 지출 0).
  // + 네이버(MP 귀속, ch=naver 포함)·메타(기타) 일자합 — 네이버/메타 광고 추정분을 오가닉에서 분리(2026-07-17).
  const byDateNvMP={},byDateMetaEtc={};
  (KR_REV4H||[]).forEach(r=>{
    if(r.channel==='카카오'){if(!byDateCRM[r.date])byDateCRM[r.date]={s:0,r:0};byDateCRM[r.date].r+=(+r.revenue||0)}
    else if(r.channel==='네이버'){byDateNvMP[r.date]=(byDateNvMP[r.date]||0)+(+r.revenue||0)}
    else if(r.channel==='메타(기타)'){byDateMetaEtc[r.date]=(byDateMetaEtc[r.date]||0)+(+r.revenue||0)}
  });
  TOSS_DAILY.forEach(r=>{byDateTOSS[r.date]=(r.net_amount||r.total_amount||0)});
  // 권역별 종합 — 매출은 실 결제액(국내=시트/Toss, 글로벌=Stripe), 지출은 해당 권역 채널 합산
  // 밴스드 포함/제외 토글 — 글로벌 매출탭(grevVanced)과 같은 규칙: 종합 실결제 매출에서 밴스드
  //   귀속매출을 빼고, 지출에서 밴스드 지출을 뺀다. 밴스드 채널 = 국내 2개(밴스드 국내=메타,
  //   밴스드 구글=google_ads_daily) + 글로벌 1개(대만 밴스드=VN_TW_ACC).
  const incVan=(document.getElementById('chrVanced')?.value||'inc')==='inc';
  // 국내 구글 지출·매출 — 신규 소스(전 유형 합) 또는 구 소스(시트 검색광고 + [Tight]DG)
  const gKR=d=>gcOK?gSum(d,G_KR_ALL):{s:((byDateGG[d]||{s:0}).s||0)+((byDateGGDG[d]||{s:0}).s||0),
                                       r:((byDateGG[d]||{r:0}).r||0)+((byDateGGDG[d]||{r:0}).r||0)};
  const gKRvan=d=>gcOK?gSum(d,G_KR_VAN):{s:((byDateGG[d]||{s:0}).s||0),r:((byDateGG[d]||{r:0}).r||0)};
  const gTW=d=>gcOK?gSum(d,G_TW_ALL):{s:0,r:0};
  const vanDomS=d=>((byDateVN[d]||{s:0}).s||0)+gKRvan(d).s;
  const vanDomR=d=>((byDateVN[d]||{r:0}).r||0)+gKRvan(d).r;
  const domRevAll=d=>(HIST_REVENUE[d]!=null?HIST_REVENUE[d]:(byDateTOSS[d]||0));
  const domRev=d=>incVan?domRevAll(d):domRevAll(d)-vanDomR(d);
  const domSpendAll=d=>{const k=byDateKR[d]||{s:0},v=byDateVN[d]||{s:0},n=byDatePL[d]||{s:0},cm=byDateCRM[d]||{s:0},tt=byDateTT[d]||{s:0};return k.s+v.s+n.s+gKR(d).s+cm.s+tt.s};  // 국내메타+밴스드+네이버+구글(전유형)+CRM발송비용+틱톡
  const domSpend=d=>incVan?domSpendAll(d):domSpendAll(d)-vanDomS(d);
  const glRevAll=d=>(byDateStripe[d]||0);  // 글로벌 종합 매출 = Stripe revenue_usd × usd_krw_rate (KRW, 실 결제액)
  // 글로벌 밴스드 귀속매출 = 대만 밴스드(메타) + 대만 구글 — 밴스드 제외 시 종합·잔여행에서 함께 차감
  const glVanR=d=>((byDateVNTW[d]||{r:0}).r||0)+gTW(d).r;
  const glRev=d=>incVan?glRevAll(d):glRevAll(d)-glVanR(d);
  const glSpendAll=d=>{const vt=byDateVNTW[d]||{s:0},gl=byDateGL[d]||{s:0};return vt.s+gl.s+gTW(d).s};  // 대만밴스드 + 글로벌(타이트) + 대만구글. 서로 중복 없음
  const glSpend=d=>incVan?glSpendAll(d):(byDateGL[d]||{s:0}).s;
  // 권역별 종합 행. 전체 종합은 항상 맨 위 고정, 국내/글로벌 종합은 해당 권역 탭에서만 표시
  const sumDom={name:'국내 종합', sum:true, get:d=>({s:domSpend(d),r:domRev(d)})};
  const sumGl ={name:'글로벌 종합',sum:true, get:d=>({s:glSpend(d),r:glRev(d)})};
  const sumAll={name:'전체 종합', sum:true, get:d=>({s:domSpend(d)+glSpend(d),r:domRev(d)+glRev(d)})};
  // 구글 상세 행 — 국내는 유형별(디멘드젠은 타이트/밴스드 분리), 대만은 글로벌 권역으로.
  //   van:true = 밴스드 운영(구분 컬럼 그룹 + 밴스드 제외 토글 대상).
  const gRowsKR=gcOK
    ? [['구글 국내 디멘드젠(타이트)','KR_DG_T',false],
       ['구글 국내 디멘드젠(밴스드)','KR_DG_V',true],
       ['구글 국내 검색광고','KR_SEARCH',true],
       ['구글 PMAX','KR_PMAX',true]]
       // '구글 기타(GDN·동영상)'(KR_ETC)는 사용자 요청으로 매출탭에서 제외(2026-08-11).
       //   행뿐 아니라 아래 G_KR_ALL·G_KR_VAN(종합 지출·밴스드 차감)에서도 빼서 채널 합 = 종합이 유지된다.
       //   → 국내 종합 지출은 그만큼 작아지고(=구글 실집행액보다 과소), KR_ETC 귀속매출은 오가닉으로 잡힌다.
       //   대만 '구글 대만 기타'(TW_ETC)는 그대로 유지.
        .filter(x=>gHas(x[1])).map(x=>({name:x[0],get:gGet(x[1]),van:x[2]}))
    : [{name:'밴스드 구글',get:d=>byDateGG[d]||{s:0,r:0},van:true},
       {name:'디멘드젠(타이트)',get:d=>byDateGGDG[d]||{s:0,r:0}}];
  const gRowsTW=gcOK
    ? [['구글 대만 검색광고','TW_SEARCH'],['구글 대만 디멘드젠','TW_DG'],['구글 대만 기타','TW_ETC']]
        .filter(x=>gHas(x[1])).map(x=>({name:x[0],get:gGet(x[1]),van:true}))
    : [];
  // 권역별 상세 채널
  const domChannels=[
    {name:'국내 메타',  get:d=>byDateKR[d]||{s:0,r:0}},
    {name:'밴스드 국내',get:d=>byDateVN[d]||{s:0,r:0}},
    // 네이버 = 브랜드검색 / 일반검색어 2행 (시트 '00. 네이버/구글 Daily' 파워링크 섹션).
    //   지출 = 각 kind 시트값 그대로.
    //   매출 = 종전 규칙(합계 = max(시트 전환값 합, MP 귀속 ch=naver))을 유지하되 kind 로 안분한다.
    //     · 시트는 네이버 전환추적分만 커버해 MP 보다 낮게 잡히므로, MP 초과분을 시트 매출 비율로 나눠 더한다
    //       (시트 매출이 0이면 지출 비율, 그것도 0이면 브랜드/일반 반반).
    //     · 합계가 종전과 동일해서 오가닉(=실결제−채널귀속)에 영향 없음. 검증 근거는 2026-07-17 메모 참고.
    ...(function(){
      const nvSplit=(d,kind)=>{
        const b=byDatePLb[d]||{s:0,r:0}, g=byDatePLg[d]||{s:0,r:0};
        const sheetSum=b.r+g.r, tot=Math.max(sheetSum,byDateNvMP[d]||0);
        const mine=kind==='b'?b:g;
        let share;
        if(sheetSum>0)      share=mine.r/sheetSum;
        else if(b.s+g.s>0)  share=mine.s/(b.s+g.s);
        else                share=0.5;
        return {s:mine.s, r:tot*share};
      };
      return [{name:'네이버 브랜드검색',get:d=>nvSplit(d,'b')},
              {name:'네이버 일반검색어',get:d=>nvSplit(d,'g')}];
    })(),
    ...gRowsKR,
    {name:'틱톡',get:d=>byDateTT[d]||{s:0,r:0}},   // 시트(TIKTOK): 지출=광고관리자 실지출, 매출=MP utm_id 귀속
    {name:'CRM',get:d=>byDateCRM[d]||{s:0,r:0}},  // 알림톡: 매출=귀속, 지출=발송비용
    // 메타(기타): 세트 귀속 불가한 Meta 광고 흔적 결제(만료세트 utm/ch=meta·ins utm소실/후행24h 크로스셀).
    //   kr_channel_revenue_4h 채널='메타(기타)' 일자합(매출만). 세트별 크로스셀 백필로 이미 국내메타에
    //   귀속된 결제는 파이프라인에서 제외되어 국내메타 행과 이중계상 없음(2026-07-17).
    {name:'메타(기타)',revOnly:true,get:d=>({s:0,r:byDateMetaEtc[d]||0})},
  ].filter(c=>incVan||(c.name!=='밴스드 국내'&&!c.van));
  // 글로벌(밴스드 제외) = 글로벌 전체(종합) − 밴스드(대만 밴스드 메타 + 대만 구글).
  //   매출=Stripe 실결제−밴스드귀속, 지출=글로벌 전체지출−밴스드지출(=GL_AD) → 상세행 합=글로벌 종합.
  //   ★ 계산은 항상 '전체(All)' helper 기준 — 밴스드 제외 모드에서 glSpend/glRev 를 쓰면 이중 차감된다.
  const glExcRow={name:incVan?'글로벌(밴스드 제외)':'글로벌',get:d=>{const vt=byDateVNTW[d]||{s:0,r:0};return {s:glSpendAll(d)-vt.s-gTW(d).s, r:glRevAll(d)-glVanR(d)};}};
  const glChannels=incVan
    ? [{name:'대만 밴스드',get:d=>byDateVNTW[d]||{s:0,r:0},van:true},...gRowsTW,glExcRow]   // 대만밴스드=VN_TW_ACC 단독(KRW)
    : [glExcRow];
  // 행 그룹 태그 — 나열 순서를 권역(국내→글로벌) → 소속(우리→밴스드) 로 묶기 위한 키.
  //   실제 정렬/구분선은 _chrevSortByRev 가 처리(그룹 안에서만 매출 내림차순).
  const IS_VAN=c=>c.van===true||c.name==='밴스드 국내'||c.name==='밴스드 구글'||c.name==='대만 밴스드';
  domChannels.forEach(c=>{c.grp=IS_VAN(c)?'dom_van':'dom_us'});
  glChannels.forEach(c=>{c.grp=IS_VAN(c)?'gl_van':'gl_us'});
  const scope=document.getElementById('chrScope')?.value||'all';
  // 권역별 상세 채널 + 종합매출 함수
  let head,detail,totalRev;
  if(scope==='dom'){head=[sumAll,sumDom];detail=domChannels;totalRev=domRev;}
  else if(scope==='gl'){head=[sumAll,sumGl];detail=glChannels;totalRev=glRev;}
  else{head=[sumAll];detail=domChannels.concat(glChannels);totalRev=d=>domRev(d)+glRev(d);}
  // 오가닉 = 종합 실결제 매출(국내=Toss/시트, 글로벌=Stripe) − 채널 귀속 매출 합 = 미귀속·직접유입.
  //   (2026-07-17: 네이버 행 max(시트,MP)·메타(기타) 신설로 네이버/메타 광고 추정분은 오가닉에서 제외 —
  //    잔여 오가닉의 대부분은 무컨텍스트 서버이벤트(PDF/이용권 등)와 MP 미계측 Toss 결제)
  // 지출 없음(s:0). 채널 맨 아래에 표시. (글로벌은 글로벌(밴스드제외)가 잔여라 오가닉≈0)
  // 글로벌 단독은 '글로벌(밴스드 제외)'가 이미 잔여라 오가닉≈0 → 생략. 국내/전체만 오가닉 추가.
  if(scope==='gl')return [...head,...detail];
  const organic={name:'오가닉',revOnly:true,get:d=>{let r=0;detail.forEach(c=>{r+=(c.get(d).r||0)});return {s:0,r:totalRev(d)-r};}};
  return [...head,...detail,organic];
}

// 채널 매트릭스 셀 1개 렌더. revOnly(오가닉 등 지출 없는 채널)=매출만·중립색(ROAS/지출 미표시).
function _chrCell(v,revOnly,avg){
  if(!v.r&&!v.s)return avg?'<td class="mc"></td>':'<td></td>';
  if(revOnly)return '<td class="mc"'+(avg?' style="font-weight:700"':'')+'><div class="rv" style="font-weight:700">'+moneyKRW(v.r)+'</div></td>';
  const roas=v.s>0?v.r/v.s*100:0,p=v.r-v.s;
  return '<td class="mc '+RC(roas)+'"'+(avg?' style="font-weight:700"':'')+'>'+MC_CH(roas,p,v.s,v.r)+'</td>';
}

// 행 <tr> 스타일: 종합행=굵은 구분선, 그룹(권역·소속) 첫 행=점선 구분선.
function _chrRowStyle(ch){
  if(ch.sum)return ' style="border-top:2px solid #9bb5d4"';
  return ch.grpTop?' style="border-top:2px dashed #b9c9db"':'';
}

// ===== '구분' 컬럼 (채널 왼쪽 최좌측) — 행이 속한 카테고리를 rowspan 으로 병합 표시 =====
// 종합 / 국내·우리 / 국내·밴스드 / 글로벌·우리 / 글로벌·밴스드 / 미귀속(오가닉).
// sticky 는 추이차트와 같은 방식: 구분=fxa(left 0), 채널=fx0 → 렌더 후 _fixSticky 가 left 계산.
const CHR_GRP_META={
  sum      :{label:'종합',            bg:'#b9cde4'},
  dom_us   :{label:'국내<br>우리',     bg:'#d9e8d4'},
  dom_van  :{label:'국내<br>밴스드',   bg:'#e6dcf2'},
  gl_us    :{label:'글로벌<br>우리',   bg:'#d3e7f2'},
  gl_van   :{label:'글로벌<br>밴스드', bg:'#f2dee6'},
  organic  :{label:'미귀속',          bg:'#e5e7eb'},
  etc      :{label:'기타',            bg:'#e9edf2'},
};
function _chrGrpKey(ch){return ch.sum?'sum':(ch.name==='오가닉'?'organic':(CHR_GRP_META[ch.grp]?ch.grp:'etc'))}
// rows 와 같은 길이의 배열 반환 — 그룹 첫 행만 <td rowspan>, 병합돼 생략되는 행은 ''.
function _chrGrpCells(rows){
  const out=rows.map(()=>'');
  for(let i=0;i<rows.length;){
    const k=_chrGrpKey(rows[i]);let j=i;
    while(j+1<rows.length&&_chrGrpKey(rows[j+1])===k)j++;
    const m=CHR_GRP_META[k]||CHR_GRP_META.etc;
    out[i]='<td class="fx fxa" rowspan="'+(j-i+1)+'" style="background:'+m.bg+';font-size:10px;font-weight:700;color:#33475a;'
      +'text-align:center;line-height:1.25;padding:3px 4px;vertical-align:middle;cursor:default">'+m.label+'</td>';
    i=j+1;
  }
  return out;
}
const CHR_TH_GRP='<th style="min-width:58px;text-align:center">구분</th>';

// 채널 행 나열 순서: 종합(sum) 상단 고정 → 상세채널 → 오가닉 맨 아래.
// 상세채널은 ①권역(국내→글로벌) ②소속(우리→밴스드) 로 묶고, 그 안에서만 표시기간 매출 내림차순.
//   그룹 첫 행엔 grpTop=true 를 달아 표에서 점선 구분선을 그린다(그룹 태그는 _chrevChannels 가 부여).
const CHR_GRP_ORDER=['dom_us','dom_van','gl_us','gl_van'];
function _chrevSortByRev(channels,dates){
  const head=channels.filter(c=>c.sum);
  const tail=channels.filter(c=>!c.sum&&c.name==='오가닉');
  const rank=c=>{const i=CHR_GRP_ORDER.indexOf(c.grp);return i<0?CHR_GRP_ORDER.length:i};  // 미태깅 그룹은 맨 뒤
  const mid=channels.filter(c=>!c.sum&&c.name!=='오가닉')
    .map(c=>{let r=0;dates.forEach(d=>{r+=(c.get(d).r||0)});return{c,r,g:rank(c)}})
    .sort((a,b)=>a.g-b.g||b.r-a.r).map(x=>x.c);
  let prev=null;mid.forEach(c=>{const g=rank(c);c.grpTop=(prev!==null&&g!==prev);prev=g});
  return [...head,...mid,...tail];
}

function _chrevDaily(channels,days){
  const today=new Date();
  const dates=[];
  for(let i=0;i<days;i++){
    const d=new Date(today);d.setDate(today.getDate()-i);
    dates.push(d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'));
  }
  channels=_chrevSortByRev(channels,dates.slice(0,7));  // 정렬 기준 = '7일 합계' 컬럼과 동일한 최근 7일 매출
  const ths=dates.map(d=>{const w=WD(d);return '<th class="'+(w==='일'?'sun':'')+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')</th>'}).join('');
  let h='<thead><tr>'+CHR_TH_GRP+'<th style="min-width:100px;text-align:left">채널</th><th style="min-width:90px">7일 합계</th>'+ths+'</tr></thead><tbody>';
  const r7=dates.slice(0,7);
  const grpCells=_chrGrpCells(channels);
  channels.forEach((ch,ci)=>{
    let a7s=0,a7r=0;
    r7.forEach(d=>{const v=ch.get(d);a7s+=v.s;a7r+=v.r});
    const a7roas=a7s>0?a7r/a7s*100:0,a7p=a7r-a7s;
    const nameBg=ch.sum?'#c5d6ea':'#dce6f0';
    h+='<tr'+_chrRowStyle(ch)+'>'+grpCells[ci]+'<td class="fx fx0" style="background:'+nameBg+';font-weight:700;padding:4px 6px">'+ch.name+'</td>';
    h+=_chrCell({s:a7s,r:a7r},ch.revOnly,true);
    dates.forEach(d=>{
      h+=_chrCell(ch.get(d),ch.revOnly,false);
    });
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('chrTbl').innerHTML=h;
  requestAnimationFrame(()=>_fixSticky(document.getElementById('chrTbl')));   // 구분(fxa) 폭만큼 채널(fx0) left 보정
  const periods=dates.map(d=>({label:DK(d)+'('+WD(d)+')',dates:[d]}));
  _chrevChart(channels,periods);
}

// 주별/월별 — 일별과 동일한 매트릭스 (행=채널, 열=기간, 셀=ROAS/순이익/매출/지출)
function _chrevPeriod(channels,view,count){
  const today=new Date();
  const fmt=d=>d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
  const periods=[];
  if(view==='weekly'){
    const monStart=new Date(WM(fmt(today)));
    for(let i=0;i<count;i++){
      const start=new Date(monStart);start.setDate(monStart.getDate()-i*7);
      const end=new Date(start);end.setDate(start.getDate()+6);
      const dates=[];for(let j=0;j<7;j++){const d=new Date(start);d.setDate(start.getDate()+j);dates.push(fmt(d))}
      const lab=(start.getMonth()+1)+'/'+start.getDate()+'~'+(end.getMonth()+1)+'/'+end.getDate();
      periods.push({label:lab,dates:dates});
    }
  }else{
    let yr=today.getFullYear(),mo=today.getMonth();
    for(let i=0;i<count;i++){
      const lastDay=new Date(yr,mo+1,0).getDate();
      const dates=[];for(let d=1;d<=lastDay;d++){dates.push(yr+'-'+String(mo+1).padStart(2,'0')+'-'+String(d).padStart(2,'0'))}
      const lab=String(yr).slice(2)+'년 '+(mo+1)+'월';
      periods.push({label:lab,dates:dates});
      mo--;if(mo<0){mo=11;yr--}
    }
  }
  // 일별 뷰(_chrevDaily)와 동일한 행=채널 / 열=기간 매트릭스. 각 셀은 MC_CH(ROAS/순이익/매출/지출).
  channels=_chrevSortByRev(channels,periods.flatMap(p=>p.dates));
  const ths=periods.map(per=>'<th style="min-width:var(--cw)">'+per.label+'</th>').join('');
  let h='<thead><tr>'+CHR_TH_GRP+'<th style="min-width:100px;text-align:left">채널</th>'+ths+'</tr></thead><tbody>';
  const grpCells=_chrGrpCells(channels);
  channels.forEach((ch,ci)=>{
    const nameBg=ch.sum?'#c5d6ea':'#dce6f0';
    h+='<tr'+_chrRowStyle(ch)+'>'+grpCells[ci]+'<td class="fx fx0" style="background:'+nameBg+';font-weight:700;padding:4px 6px">'+ch.name+'</td>';
    periods.forEach(per=>{
      let s=0,r=0;
      per.dates.forEach(d=>{const v=ch.get(d);s+=v.s;r+=v.r});
      h+=_chrCell({s,r},ch.revOnly,false);
    });
    h+='</tr>';
  });
  h+='</tbody>';document.getElementById('chrTbl').innerHTML=h;
  requestAnimationFrame(()=>_fixSticky(document.getElementById('chrTbl')));
  _chrevChart(channels,periods);
}

// ===== 채널별 매출 구성 누적 막대그래프 (매출탭 하단) =====
// 막대 1개 = 한 기간(일/주/월). 전체 높이 = 채널 매출 합, 누적 조각 = 채널별 매출 → 비율 시각화.
// 종합(sum) 행은 제외하고 실제 채널만 누적. 권역(scope) 선택은 _chrevChannels가 이미 반영.
let _chrevChartInst=null;
function _chrevChart(channels,periods){
  if(typeof Chart==='undefined')return;
  const cv=document.getElementById('chrChart');if(!cv)return;
  const mode=document.getElementById('chrChartMode')?.value||'abs';
  const style=document.getElementById('chrChartStyle')?.value||'stack';  // stack=누적, group=채널별 개별 막대 나란히
  const chs=channels.filter(c=>!c.sum);                 // 종합행 제외 → 실제 채널만
  const pers=periods.slice();                           // 최근→오래된 (최근이 왼쪽, 표와 동일 방향)
  const labels=pers.map(p=>p.label);
  const COLORS={'국내 메타':'#1877F2','밴스드 국내':'#9333ea','네이버':'#03C75A','네이버 브랜드검색':'#03C75A','네이버 일반검색어':'#7cd6a0','밴스드 구글':'#EA4335','디멘드젠(타이트)':'#FBBC04','틱톡':'#25F4EE','CRM':'#FEE500','메타(기타)':'#93c5fd',
    '구글 국내 디멘드젠(타이트)':'#FBBC04','구글 국내 디멘드젠(밴스드)':'#f59e0b','구글 국내 검색광고':'#4285F4',
    '구글 PMAX':'#34A853','구글 대만 검색광고':'#7cb0f5','구글 대만 디멘드젠':'#fcd34d','구글 대만 기타':'#cbd5e1','대만 밴스드':'#c084fc','글로벌(밴스드 제외)':'#0ea5e9','글로벌':'#0ea5e9','오가닉':'#94a3b8'};
  const PAL=['#60a5fa','#f59e0b','#34d399','#f472b6','#a78bfa','#fb7185','#22d3ee','#facc15'];
  const raw=chs.map((ch,i)=>({name:ch.name,color:COLORS[ch.name]||PAL[i%PAL.length],
    data:pers.map(p=>{let r=0;p.dates.forEach(d=>{r+=(ch.get(d).r||0)});return r})}));
  const totals=pers.map((p,idx)=>raw.reduce((s,ds)=>s+ds.data[idx],0));  // 기간별 채널 매출 합
  const datasets=raw.map(ds=>({
    label:ds.name,backgroundColor:ds.color,borderWidth:0,
    ...(style==='stack'?{stack:'rev'}:{}),
    data:mode==='pct'?ds.data.map((v,idx)=>totals[idx]>0?v/totals[idx]*100:0):ds.data,
    _abs:ds.data,_tot:totals,
  }));
  if(_chrevChartInst){try{_chrevChartInst.destroy()}catch(e){}}
  const won=v=>'₩'+Math.round(v).toLocaleString('ko-KR');
  const axMoney=v=>{const a=Math.abs(v);return a>=1e8?'₩'+(v/1e8).toFixed(1)+'억':a>=1e4?'₩'+Math.round(v/1e4)+'만':'₩'+Math.round(v)};
  _chrevChartInst=new Chart(cv,{type:'bar',data:{labels,datasets},
    options:{responsive:true,maintainAspectRatio:false,animation:false,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
        tooltip:{callbacks:{
          label:ctx=>{const d=ctx.dataset,abs=d._abs[ctx.dataIndex],tot=d._tot[ctx.dataIndex],pct=tot>0?abs/tot*100:0;
            return d.label+': '+won(abs)+' ('+pct.toFixed(1)+'%)'},
          footer:items=>items.length?'채널 합: '+won(items[0].dataset._tot[items[0].dataIndex]):''}}},
      scales:{
        x:{stacked:style==='stack',ticks:{font:{size:10},maxRotation:60,minRotation:0}},
        y:{stacked:style==='stack',beginAtZero:true,max:mode==='pct'?100:undefined,
          ticks:{font:{size:10},callback:v=>mode==='pct'?v+'%':axMoney(v)}}}}});
}

// ===== 채널별 매출 구성 도넛(비율) — 매출탭 하단, 자체 기간 드롭다운(chrDonutDays) =====
// 권역(chrScope) 선택은 _chrevChannels()가 이미 반영. 종합(sum)행 제외, 매출>0 채널만, 비율 표시.
let _chrDonutInst=null;
function renderChannelDonut(){
  if(typeof Chart==='undefined')return;
  const cv=document.getElementById('chrDonut');if(!cv)return;
  // 메타(기타)·카카오(CRM)·네이버MP 매출은 kr_channel_revenue_4h 기반 → 로드 완료 후 그려야
  // 첫 진입에도 반영됨 (미로드 시 매출 0 → rev>0 필터에 걸려 도넛에서 누락되던 버그 수정)
  if(!_KR_REV4H_LOADED){ensureKrRev4h(()=>renderChannelDonut());return}
  const days=parseInt(document.getElementById('chrDonutDays')?.value)||30;
  const channels=_chrevChannels().filter(c=>!c.sum);   // 실제 채널만(종합 제외)
  const today=new Date();const dates=[];
  for(let i=0;i<days;i++){const d=new Date(today);d.setDate(today.getDate()-i);dates.push(d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'));}
  const COLORS={'국내 메타':'#1877F2','밴스드 국내':'#9333ea','네이버':'#03C75A','네이버 브랜드검색':'#03C75A','네이버 일반검색어':'#7cd6a0','밴스드 구글':'#EA4335','디멘드젠(타이트)':'#FBBC04','틱톡':'#25F4EE','CRM':'#FEE500','메타(기타)':'#93c5fd',
    '구글 국내 디멘드젠(타이트)':'#FBBC04','구글 국내 디멘드젠(밴스드)':'#f59e0b','구글 국내 검색광고':'#4285F4',
    '구글 PMAX':'#34A853','구글 대만 검색광고':'#7cb0f5','구글 대만 디멘드젠':'#fcd34d','구글 대만 기타':'#cbd5e1','대만 밴스드':'#c084fc','글로벌(밴스드 제외)':'#0ea5e9','글로벌':'#0ea5e9','오가닉':'#94a3b8'};
  const PAL=['#60a5fa','#f59e0b','#34d399','#f472b6','#a78bfa','#fb7185','#22d3ee','#facc15'];
  let agg=channels.map((ch,i)=>{let r=0;dates.forEach(d=>{r+=(ch.get(d).r||0)});return {name:ch.name,rev:r,color:COLORS[ch.name]||PAL[i%PAL.length]};});
  agg=agg.filter(x=>x.rev>0).sort((a,b)=>b.rev-a.rev);   // 매출>0만(오가닉 음수 잔여 제외)
  const total=agg.reduce((s,x)=>s+x.rev,0);
  const won=v=>'₩'+Math.round(v).toLocaleString('ko-KR');
  const leg=document.getElementById('chrDonutLegend');
  if(_chrDonutInst){try{_chrDonutInst.destroy()}catch(e){}}
  if(!agg.length||total<=0){
    if(leg)leg.innerHTML='<span style="color:#888">선택 기간 매출 데이터 없음</span>';
    _chrDonutInst=new Chart(cv,{type:'doughnut',data:{labels:[],datasets:[{data:[]}]},options:{responsive:true,maintainAspectRatio:false}});
    return;
  }
  _chrDonutInst=new Chart(cv,{type:'doughnut',
    data:{labels:agg.map(x=>x.name),datasets:[{data:agg.map(x=>x.rev),backgroundColor:agg.map(x=>x.color),borderWidth:1,borderColor:'#fff'}]},
    options:{responsive:true,maintainAspectRatio:false,cutout:'58%',animation:false,
      plugins:{legend:{display:false},
        tooltip:{callbacks:{label:ctx=>{const v=ctx.parsed||0,p=total>0?v/total*100:0;return ctx.label+': '+won(v)+' ('+p.toFixed(1)+'%)'}}}}}});
  if(leg){
    const rows=agg.map(x=>{const p=x.rev/total*100;return '<div style="display:flex;align-items:center;gap:6px;margin:3px 0"><span style="width:11px;height:11px;border-radius:2px;background:'+x.color+';display:inline-block;flex:0 0 auto"></span><span style="flex:1">'+x.name+'</span><b style="margin-left:8px">'+p.toFixed(1)+'%</b><span style="color:#888;margin-left:8px;min-width:100px;text-align:right">'+won(x.rev)+'</span></div>'}).join('');
    leg.innerHTML='<div style="font-weight:700;margin-bottom:6px;border-bottom:1px solid #e5e9f0;padding-bottom:5px;display:flex;justify-content:space-between">합계<span>'+won(total)+'</span></div>'+rows;
  }
}
// ===== 채널별 매출·지출·순이익 가로 막대 — 매출탭 도넛 위, 자체 기간 컨트롤 =====
// 세로축=채널, 가로축=금액. 채널당 매출·지출·순이익 3개 막대(선택 기간 합계).
// 기간 = 드롭다운(chrBarDays: 최근 7·14·30·60·90·180일) 또는 캘린더(chrBarFrom~chrBarTo) 직접 지정.
// 권역(chrScope)·밴스드(chrVanced) 선택은 _chrevChannels()가 이미 반영 · 종합(sum)행은 제외.
let _chrBarInst=null;
function _chrDstr(d){return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0')}
// 캘린더 입력이 둘 다 채워져 있으면 그 구간(양끝 포함), 아니면 최근 N일.
//   반환 {dates:[YYYY-MM-DD…], label:'…', custom:bool}
function _chrBarDates(){
  const f=(document.getElementById('chrBarFrom')?.value||'').trim();
  const t=(document.getElementById('chrBarTo')?.value||'').trim();
  const dates=[];
  if(f&&t){
    let a=f,b=t;if(a>b){const x=a;a=b;b=x}                      // 거꾸로 넣어도 알아서 정렬
    const cur=new Date(a+'T00:00:00'),end=new Date(b+'T00:00:00');
    let guard=0;
    while(cur<=end&&guard++<2000){dates.push(_chrDstr(cur));cur.setDate(cur.getDate()+1)}
    return {dates,label:a+' ~ '+b+' ('+dates.length+'일)',custom:true};
  }
  const days=parseInt(document.getElementById('chrBarDays')?.value)||30;
  const today=new Date();
  for(let i=0;i<days;i++){const d=new Date(today);d.setDate(today.getDate()-i);dates.push(_chrDstr(d))}
  return {dates,label:dates[dates.length-1]+' ~ '+dates[0]+' (최근 '+days+'일)',custom:false};
}
// 캘린더 초기화 → 드롭다운 기간으로 복귀
function chrBarClearRange(){
  const f=document.getElementById('chrBarFrom'),t=document.getElementById('chrBarTo');
  if(f)f.value='';if(t)t.value='';
  const sel=document.getElementById('chrBarDays');
  if(sel&&sel.value==='custom')sel.value='30';
  renderChannelBars();
}
// 막대 안에 값을 직접 그리는 플러그인 — 호버 없이도 채널명·ROAS·금액을 읽게 한다.
//   · 매출 막대: 왼쪽에 '채널명 · ROAS 195%', 오른쪽 끝에 금액
//   · 지출·순이익 막대: 오른쪽 끝에 '지출 ₩…' / '순이익 ₩…'
//   막대가 좁아 글자가 안 들어가면 막대 바깥(오른쪽)에 회색으로 그린다.
const _chrBarLabels={
  id:'chrBarLabels',
  afterDatasetsDraw(chart,args,opts){
    const ctx=chart.ctx,meta=opts||{};   // opts = options.plugins.chrBarLabels ({won, info})
    ctx.save();
    chart.data.datasets.forEach((ds,di)=>{
      const m=chart.getDatasetMeta(di);
      if(m.hidden)return;
      m.data.forEach((bar,i)=>{
        const v=ds.data[i];if(v==null||v===0)return;   // 0(예: 오가닉의 지출)은 라벨 생략 — 축 옆 잡음
        const x0=bar.base,x1=bar.x,w=Math.abs(x1-x0),h=bar.height||0;
        if(h<9)return;                                   // 너무 얇으면 생략
        const fs=Math.min(11,Math.max(8,Math.round(h*0.62)));
        ctx.font='700 '+fs+'px -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif';
        ctx.textBaseline='middle';
        const amt=(meta.won?meta.won(v):String(v));
        const txt=(di===0?'':ds.label+' ')+amt;
        const right=Math.max(x0,x1),left=Math.min(x0,x1);
        // 금액: 막대 안 오른쪽 끝(공간 부족하면 막대 밖)
        const tw=ctx.measureText(txt).width;
        if(tw+14<=w){ctx.fillStyle='#fff';ctx.textAlign='right';ctx.fillText(txt,right-6,bar.y)}
        else{ctx.fillStyle='#555';ctx.textAlign='left';ctx.fillText(txt,right+5,bar.y)}
        // 매출 막대 왼쪽: 채널명 · ROAS (가독성용 — 축 라벨을 못 봐도 읽힌다)
        if(di===0){
          const info=(meta.info||{})[chart.data.labels[i]];
          if(info){
            const head=info.name+(info.roas!=null?'  ROAS '+info.roas.toFixed(0)+'%':'  ROAS —');
            ctx.textAlign='left';
            const hw=ctx.measureText(head).width;
            if(hw+(tw+14<=w?tw+20:8)+12<=w){ctx.fillStyle='#fff';ctx.fillText(head,left+7,bar.y)}
          }
        }
      });
    });
    ctx.restore();
  }
};
function renderChannelBars(){
  if(typeof Chart==='undefined')return;
  const cv=document.getElementById('chrBar');if(!cv)return;
  // 도넛과 같은 이유 — 메타(기타)·CRM(카카오)·네이버MP 매출은 kr_channel_revenue_4h 기반이라 로드 후 그린다
  if(!_KR_REV4H_LOADED){ensureKrRev4h(()=>renderChannelBars());return}
  const R=_chrBarDates();
  {const sel=document.getElementById('chrBarDays');if(sel&&R.custom&&sel.value!=='custom')sel.value='custom';}
  {const el=document.getElementById('chrBarRangeInfo');if(el)el.textContent=(R.custom?'📅 ':'')+R.label;}
  const channels=_chrevChannels().filter(c=>!c.sum);
  let agg=channels.map(ch=>{let r=0,s=0;R.dates.forEach(d=>{const v=ch.get(d)||{};r+=(+v.r||0);s+=(+v.s||0)});
    return {name:ch.name,rev:r,spend:s,profit:r-s,roas:s>0?r/s*100:null};});
  agg=agg.filter(x=>x.rev!==0||x.spend!==0).sort((a,b)=>b.rev-a.rev);
  const wrap=document.getElementById('chrBarWrap');
  if(wrap)wrap.style.height=Math.max(260,agg.length*68+72)+'px';   // 채널 수에 맞춰 높이 자동(막대 안 글자가 들어갈 두께)
  const won=v=>'₩'+Math.round(v).toLocaleString('ko-KR');
  const axMoney=v=>{const a=Math.abs(v);return a>=1e8?(v/1e8).toFixed(1)+'억':a>=1e4?Math.round(v/1e4).toLocaleString('ko-KR')+'만':String(Math.round(v))};
  if(_chrBarInst){try{_chrBarInst.destroy()}catch(e){}}
  if(!agg.length){
    _chrBarInst=new Chart(cv,{type:'bar',data:{labels:[],datasets:[]},options:{responsive:true,maintainAspectRatio:false}});
    return;
  }
  const byName={};agg.forEach(x=>{byName[x.name]=x});
  _chrBarInst=new Chart(cv,{type:'bar',
    data:{labels:agg.map(x=>x.name),datasets:[
      {label:'매출',data:agg.map(x=>x.rev),backgroundColor:'#1a73e8'},
      {label:'지출',data:agg.map(x=>x.spend),backgroundColor:'#ea4335'},
      {label:'순이익',data:agg.map(x=>x.profit),backgroundColor:agg.map(x=>x.profit>=0?'#34a853':'#f97316')},
    ]},
    options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,animation:false,
      layout:{padding:{right:64}},   // 막대 밖으로 밀려난 금액 글자 자리
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{position:'bottom',labels:{boxWidth:12,font:{size:11}}},
        chrBarLabels:{won:won,info:byName},   // 막대 안 라벨 플러그인이 쓰는 값(첫 렌더부터 적용)
        tooltip:{callbacks:{
          label:ctx=>ctx.dataset.label+': '+won(ctx.parsed.x),
          footer:items=>{if(!items.length)return'';const x=byName[items[0].label];if(!x)return'';
            return 'ROAS '+(x.roas!=null?x.roas.toFixed(0)+'%':'—(지출 없음)')}}}},
      scales:{
        x:{beginAtZero:true,ticks:{font:{size:10},callback:v=>axMoney(v)},grid:{color:'#eef2f7'}},
        y:{ticks:{font:{size:11},autoSkip:false},grid:{display:false}}}},
    plugins:[_chrBarLabels]});
}

// 채널별 매출 탭용 셀 (ROAS / 순이익 / 매출 / 지출)
function MC_CH(roas,profit,spend,revenue){
  if(!spend&&!revenue)return'';
  const pc=profit>=0?'p':'p neg';
  return'<div class="r">'+roas.toFixed(0)+'</div>'
    +'<div class="'+pc+'">'+moneyKRW(profit)+'</div>'
    +'<div class="rv">'+moneyKRW(revenue)+'</div>'
    +'<div class="s">-'+moneyKRW(spend)+'</div>';
}
function moneyKRW(n){if(n==null||n===0)return'';return'₩'+Math.round(n).toLocaleString('ko-KR')}

// ===== 네이버SA_추이차트 (일별 광고그룹 레벨) =====
function renderNsaDaily(){
  const days=parseInt(document.getElementById('nsaDays').value);
  const minSp=parseFloat(document.getElementById('nsaMinSpend').value)||0;
  const sortBy=document.getElementById('nsaSort').value;
  // 검색어 추이 먼저 렌더 (광고그룹 차트 위쪽 패널)
  renderNsaKeywords(days);
  // 날짜 리스트
  const allDates=[...new Set(NSA_DAILY.map(r=>r.date))].sort().reverse().slice(0,days);
  if(!allDates.length){document.getElementById('nsaTbl').innerHTML='<tr><td>네이버SA 데이터 없음 — naver_sa_daily 파이프라인이 실행돼야 합니다</td></tr>';return}
  const d7=allDates.slice(0,7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // adgroup별 그룹화
  const byA={};
  NSA_DAILY.forEach(r=>{
    if(!allDates.includes(r.date))return;
    const id=r.adgroup_id||'';
    if(!byA[id])byA[id]={cn:r.campaign_name||'',an:r.adgroup_name||'',id,product:r.product||'',d:{}};
    byA[id].d[r.date]=r;
  });
  // 7일 합산 + 정렬키
  const list=Object.values(byA).map(a=>{
    let s=0,rv=0,conv=0,clk=0,imp=0;
    d7.forEach(d=>{const x=a.d[d];if(x){s+=(x.cost_vat||0);rv+=(x.revenue||0);conv+=(x.conversions||0);clk+=(x.clicks||0);imp+=(x.impressions||0)}});
    a._s=s;a._r=rv;a._p=rv-s;a._roas=s>0?rv/s*100:0;a._cvr=clk>0?conv/clk*100:0;a._ctr=imp>0?clk/imp*100:0;
    a._yS=a.d[yDay]?(a.d[yDay].cost_vat||0):0;
    return a;
  }).filter(a=>a._s>=minSp);
  list.sort((a,b)=>{
    if(sortBy==='roas')return b._roas-a._roas;
    if(sortBy==='spend')return b._s-a._s;
    if(sortBy==='profit')return b._p-a._p;
    return b._yS-a._yS; // recent
  });
  const ths=allDates.map(d=>{const w=WD(d);const yd=d===yDay?' col-yday':'';return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')</th>'}).join('');
  // 종합
  const totD={};allDates.forEach(d=>{let s=0,r=0,conv=0,clk=0,imp=0;list.forEach(a=>{const x=a.d[d];if(x){s+=(x.cost_vat||0);r+=(x.revenue||0);conv+=(x.conversions||0);clk+=(x.clicks||0);imp+=(x.impressions||0)}});totD[d]={s,r,conv,clk,imp}});
  const ts=d7.reduce((a,d)=>a+(totD[d]?.s||0),0),tr=d7.reduce((a,d)=>a+(totD[d]?.r||0),0),tp=tr-ts,troas=ts>0?tr/ts*100:0;
  let tcvr=0,tctr=0;const tconv=d7.reduce((a,d)=>a+(totD[d]?.conv||0),0),tclk=d7.reduce((a,d)=>a+(totD[d]?.clk||0),0),timp=d7.reduce((a,d)=>a+(totD[d]?.imp||0),0);
  if(tclk>0)tcvr=tconv/tclk*100;
  if(timp>0)tctr=tclk/timp*100;
  let h='<thead><tr><th style="min-width:200px;text-align:left">캠페인</th><th style="min-width:200px;text-align:left">광고그룹</th><th style="min-width:130px">그룹 ID</th><th>7일</th>'+ths+'</tr></thead><tbody>';
  h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">종합 ('+list.length+'개)</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8"><div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div></td><td class="mc '+RC(troas)+'">'+MC(troas,tp,ts,tr,tcvr,null,tctr)+'</td>';
  allDates.forEach(d=>{const x=totD[d];const yd=d===yDay?' col-yday':'';const roas=x.s>0?x.r/x.s*100:0;const cvr=x.clk>0?x.conv/x.clk*100:0;const ctr=x.imp>0?x.clk/x.imp*100:0;h+='<td class="mc '+RC(roas)+yd+'">'+MC(roas,x.r-x.s,x.s,x.r,cvr,null,ctr)+'</td>'});
  h+='</tr>';
  // 상품별 그룹화
  const byProd={};list.forEach(a=>{const p=a.product||'기타';if(!byProd[p])byProd[p]={items:[],yS:0};byProd[p].items.push(a);byProd[p].yS+=a._yS});
  const colSpan=allDates.length+4;
  Object.keys(byProd).sort((a,b)=>byProd[b].yS-byProd[a].yS).forEach(prod=>{
    const g=byProd[prod];
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.items.length+'개)</td></tr>';
    g.items.forEach(a=>{
      const cells=allDates.map(d=>{const r=a.d[d];const yd=d===yDay?' col-yday':'';if(!r||!r.cost_vat)return'<td class="'+yd+'"></td>';const roas=r.cost_vat>0?r.revenue/r.cost_vat*100:0;const cvr=r.clicks>0?r.conversions/r.clicks*100:0;const ctr=r.impressions>0?r.clicks/r.impressions*100:0;return'<td class="mc '+RC(roas)+yd+'">'+MC(roas,r.revenue-r.cost_vat,r.cost_vat,r.revenue,cvr,null,ctr)+'</td>'}).join('');
      h+='<tr><td class="fx fx0">'+(a.cn||'').slice(0,25)+'</td><td class="fx fx1">'+(a.an||'').slice(0,25)+'</td><td style="font-size:9px">'+a.id+'</td><td class="mc '+RC(a._roas)+'">'+MC(a._roas,a._p,a._s,a._r,a._cvr,null,a._ctr)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';document.getElementById('nsaTbl').innerHTML=h;
}

// ===== 네이버SA 검색어 추이 (Naver SA + Mixpanel 매출 보강) =====
function renderNsaKeywords(days){
  const tbl=document.getElementById('nsaKwTbl');
  if(!tbl)return;
  if(!NSA_KW||!NSA_KW.length){
    tbl.innerHTML='<tr><td style="padding:12px;color:#888">검색어 데이터 없음 — naver_sa_keyword_daily 테이블이 비어있거나 파이프라인 실행 전입니다 (네이버_supabase.py v3 필요)</td></tr>';
    return;
  }
  const topN=parseInt(document.getElementById('nsaKwTop').value)||20;
  const minSp=parseFloat(document.getElementById('nsaMinSpend').value)||0;
  const sortBy=document.getElementById('nsaSort').value;
  // 날짜 리스트 (NSA_DAILY 와 동일 정렬)
  const allDates=[...new Set(NSA_KW.map(r=>r.date))].sort().reverse().slice(0,days);
  if(!allDates.length){tbl.innerHTML='<tr><td>검색어 데이터 없음</td></tr>';return}
  const d7=allDates.slice(0,7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // Mixpanel 매출 lookup: (date|keyword) → revenue
  const mpByDateKw={};
  if(NAVER_KW&&NAVER_KW.length){
    NAVER_KW.forEach(r=>{
      const k=(r.keyword||'').replace(/\s+/g,'').toLowerCase();
      if(!k)return;
      mpByDateKw[r.date+'|'+k]={r:r.revenue||0,c:r.purchase_count||0};
    });
  }
  // keyword_text × date 그룹화 (같은 텍스트가 여러 광고그룹/키워드ID에 있을 수 있음 → 합산)
  const byKw={};
  NSA_KW.forEach(r=>{
    if(!allDates.includes(r.date))return;
    const kw=(r.keyword_text||'(미매핑)').trim();
    if(!byKw[kw]){
      byKw[kw]={kw,product:r.product||'',d:{},
        adgroups:new Set(),_s:0,_r:0,_p:0,_clk:0,_imp:0,_conv:0,_yS:0,
        _mpR:0,_mpC:0};
    }
    if(r.adgroup_name)byKw[kw].adgroups.add(r.adgroup_name);
    if(!byKw[kw].d[r.date]){byKw[kw].d[r.date]={cost:0,rev:0,clk:0,imp:0,conv:0,mpR:0,mpC:0}}
    const x=byKw[kw].d[r.date];
    x.cost+=(r.cost_vat||0);x.rev+=(r.revenue||0);x.clk+=(r.clicks||0);
    x.imp+=(r.impressions||0);x.conv+=(r.conversions||0);
    // Mixpanel 매출 매칭 (한 번만 — 키워드 텍스트로 join, 띄어쓰기/대소문자 무시)
    const norm=(kw||'').replace(/\s+/g,'').toLowerCase();
    const mp=mpByDateKw[r.date+'|'+norm];
    if(mp&&!x.mpJoined){x.mpR=mp.r;x.mpC=mp.c;x.mpJoined=true}
  });
  // 7일 합계 + 정렬키 + Mixpanel rev 합산
  const list=Object.values(byKw).map(k=>{
    let s=0,rv=0,clk=0,imp=0,conv=0,mpR=0,mpC=0;
    d7.forEach(d=>{const x=k.d[d];if(x){s+=x.cost;rv+=x.rev;clk+=x.clk;imp+=x.imp;conv+=x.conv;mpR+=x.mpR;mpC+=x.mpC}});
    k._s=s;k._r=rv;k._p=rv-s;k._clk=clk;k._imp=imp;k._conv=conv;
    k._mpR=mpR;k._mpC=mpC;
    k._roas=s>0?rv/s*100:0;
    k._cvr=clk>0?conv/clk*100:0;
    k._ctr=imp>0?clk/imp*100:0;
    k._yS=k.d[yDay]?k.d[yDay].cost:0;
    return k;
  }).filter(k=>k._s>=minSp);
  list.sort((a,b)=>{
    if(sortBy==='roas')return b._roas-a._roas;
    if(sortBy==='spend')return b._s-a._s;
    if(sortBy==='profit')return b._p-a._p;
    if(sortBy==='recent')return b._yS-a._yS;
    return b._s-a._s;
  });
  const top=list.slice(0,topN);
  if(!top.length){tbl.innerHTML='<tr><td>조건에 맞는 검색어 없음 (최소지출/필터 확인)</td></tr>';return}
  // 종합
  const totD={};
  allDates.forEach(d=>{let s=0,r=0,clk=0,conv=0,mpR=0,imp=0;top.forEach(k=>{const x=k.d[d];if(x){s+=x.cost;r+=x.rev;clk+=x.clk;conv+=x.conv;mpR+=x.mpR;imp+=x.imp}});totD[d]={s,r,clk,conv,mpR,imp}});
  const ts=d7.reduce((a,d)=>a+(totD[d]?.s||0),0),tr=d7.reduce((a,d)=>a+(totD[d]?.r||0),0);
  const tp=tr-ts,troas=ts>0?tr/ts*100:0;
  const tconv=d7.reduce((a,d)=>a+(totD[d]?.conv||0),0),tclk=d7.reduce((a,d)=>a+(totD[d]?.clk||0),0),timp=d7.reduce((a,d)=>a+(totD[d]?.imp||0),0);
  const tcvr=tclk>0?tconv/tclk*100:0,tctr=timp>0?tclk/timp*100:0;
  const ths=allDates.map(d=>{const w=WD(d);const yd=d===yDay?' col-yday':'';return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')</th>'}).join('');
  let h='<thead><tr><th style="min-width:240px;text-align:left">검색어</th><th style="min-width:160px;text-align:left">광고그룹</th><th style="min-width:130px">라벨</th><th>7일</th>'+ths+'</tr></thead><tbody>';
  // 종합 행
  h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">📊 종합 Top'+top.length+'</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8"><div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div></td><td class="mc '+RC(troas)+'">'+MC(troas,tp,ts,tr,tcvr,null,tctr)+'</td>';
  allDates.forEach(d=>{
    const x=totD[d];const yd=d===yDay?' col-yday':'';
    const roas=x.s>0?x.r/x.s*100:0;const cvr=x.clk>0?x.conv/x.clk*100:0;const ctr=x.imp>0?x.clk/x.imp*100:0;
    h+='<td class="mc '+RC(roas)+yd+'">'+MC(roas,x.r-x.s,x.s,x.r,cvr,null,ctr)+'</td>';
  });
  h+='</tr>';
  // product 그룹화 (광고그룹 차트와 같은 시각 구조)
  const byProd={};top.forEach(k=>{const p=k.product||'기타';if(!byProd[p])byProd[p]={items:[],yS:0};byProd[p].items.push(k);byProd[p].yS+=k._yS});
  const colSpan=allDates.length+4;
  Object.keys(byProd).sort((a,b)=>byProd[b].yS-byProd[a].yS).forEach(prod=>{
    const g=byProd[prod];
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.items.length+'개 검색어)</td></tr>';
    g.items.forEach(k=>{
      const cells=allDates.map(d=>{
        const x=k.d[d];const yd=d===yDay?' col-yday':'';
        if(!x||!x.cost){
          // 지출 0 이라도 Mixpanel 매출 있으면 표시
          if(x&&x.mpR>0)return'<td class="'+yd+'" style="text-align:right;font-size:9px;color:#888">MP:<br>'+money(x.mpR)+'<br>('+x.mpC+')</td>';
          return'<td class="'+yd+'"></td>';
        }
        const roas=x.cost>0?x.rev/x.cost*100:0;
        const cvr=x.clk>0?x.conv/x.clk*100:0;
        const ctr=x.imp>0?x.clk/x.imp*100:0;
        return'<td class="mc '+RC(roas)+yd+'">'+MC(roas,x.rev-x.cost,x.cost,x.rev,cvr,null,ctr)+'</td>';
      }).join('');
      const kwSafe=(k.kw||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
      const kwDisp=kwSafe.length>30?kwSafe.slice(0,30)+'…':kwSafe;
      const agList=[...k.adgroups].slice(0,2).join(', ');
      const agDisp=agList.length>22?agList.slice(0,22)+'…':agList;
      const mpHint=k._mpR>0?'<div style="color:#888;font-size:9px;margin-top:2px">MP매출 7일: '+money(k._mpR)+' ('+k._mpC+'건)</div>':'';
      h+='<tr><td class="fx fx0" title="'+kwSafe+'">'+kwDisp+mpHint+'</td><td class="fx fx1" title="'+agList.replace(/"/g,'&quot;')+'">'+agDisp+'</td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4"><div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div></td><td class="mc '+RC(k._roas)+'">'+MC(k._roas,k._p,k._s,k._r,k._cvr,null,k._ctr)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';
  tbl.innerHTML=h;
}

// ===== 네이버SA_주간추이 (주간 집계) =====
function renderNsaWeekly(){
  const maxWeeks=parseInt(document.getElementById('nsaWeeks').value);
  const minSp=parseFloat(document.getElementById('nsawMinSpend').value)||0;
  const allWeekMap={};
  NSA_DAILY.forEach(r=>{const wk=WM(r.date);if(!allWeekMap[wk])allWeekMap[wk]=true});
  const weekKeys=Object.keys(allWeekMap).sort().reverse().slice(0,maxWeeks);
  if(!weekKeys.length){document.getElementById('nsawTbl').innerHTML='<tr><td>네이버SA 데이터 없음</td></tr>';return}
  function wkLabel(wk){const m=new Date(wk);const s=new Date(m.getTime()+6*864e5);return(m.getMonth()+1)+'/'+m.getDate()+'~'+(s.getMonth()+1)+'/'+s.getDate()}
  // adgroup × week 집계
  const byA={};
  NSA_DAILY.forEach(r=>{
    const wk=WM(r.date);if(!weekKeys.includes(wk))return;
    const id=r.adgroup_id||'';
    if(!byA[id])byA[id]={cn:r.campaign_name||'',an:r.adgroup_name||'',id,product:r.product||'',w:{}};
    if(!byA[id].w[wk])byA[id].w[wk]={s:0,r:0,conv:0,clk:0,imp:0};
    byA[id].w[wk].s+=(r.cost_vat||0);byA[id].w[wk].r+=(r.revenue||0);byA[id].w[wk].conv+=(r.conversions||0);byA[id].w[wk].clk+=(r.clicks||0);byA[id].w[wk].imp+=(r.impressions||0);
  });
  const recentWk=weekKeys[0];
  const list=Object.values(byA).map(a=>{
    let ts=0,tr=0,tconv=0,tclk=0,timp=0;
    weekKeys.forEach(wk=>{const w=a.w[wk];if(w){ts+=w.s;tr+=w.r;tconv+=w.conv;tclk+=w.clk;timp+=w.imp||0}});
    a._s=ts;a._r=tr;a._p=tr-ts;a._roas=ts>0?tr/ts*100:0;a._cvr=tclk>0?tconv/tclk*100:0;a._ctr=timp>0?tclk/timp*100:0;
    a._recentS=a.w[recentWk]?a.w[recentWk].s:0;
    return a;
  }).filter(a=>a._s>=minSp);
  list.sort((a,b)=>b._recentS-a._recentS);
  const ths=weekKeys.map(wk=>'<th style="min-width:var(--cw)">'+wkLabel(wk)+'</th>').join('');
  const totW={};weekKeys.forEach(wk=>{let s=0,r=0,conv=0,clk=0,imp=0;list.forEach(a=>{const w=a.w[wk];if(w){s+=w.s;r+=w.r;conv+=w.conv;clk+=w.clk;imp+=w.imp||0}});totW[wk]={s,r,conv,clk,imp}});
  const ts=weekKeys.reduce((a,wk)=>a+(totW[wk]?.s||0),0),tr=weekKeys.reduce((a,wk)=>a+(totW[wk]?.r||0),0),tp=tr-ts,troas=ts>0?tr/ts*100:0;
  const tconv=weekKeys.reduce((a,wk)=>a+(totW[wk]?.conv||0),0),tclk=weekKeys.reduce((a,wk)=>a+(totW[wk]?.clk||0),0),timp=weekKeys.reduce((a,wk)=>a+(totW[wk]?.imp||0),0);
  const tcvr=tclk>0?tconv/tclk*100:0,tctr=timp>0?tclk/timp*100:0;
  let h='<thead><tr><th style="min-width:200px;text-align:left">캠페인</th><th style="min-width:200px;text-align:left">광고그룹</th><th style="min-width:130px">그룹 ID</th><th>전체</th>'+ths+'</tr></thead><tbody>';
  h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">종합 ('+list.length+'개)</td><td class="fx fx1" style="background:#e8e8e8"></td><td class="mc" style="font-size:9px;text-align:left;line-height:1.4;background:#e8e8e8"><div class="r">ROAS</div><div class="p">순이익</div><div class="s">지출</div><div class="rv">매출</div><div class="cv">CVR(CTR)</div></td><td class="mc '+RC(troas)+'">'+MC(troas,tp,ts,tr,tcvr,null,tctr)+'</td>';
  weekKeys.forEach(wk=>{const x=totW[wk];const roas=x.s>0?x.r/x.s*100:0;const cvr=x.clk>0?x.conv/x.clk*100:0;const ctr=x.imp>0?x.clk/x.imp*100:0;h+='<td class="mc '+RC(roas)+'">'+MC(roas,x.r-x.s,x.s,x.r,cvr,null,ctr)+'</td>'});
  h+='</tr>';
  const byProd={};list.forEach(a=>{const p=a.product||'기타';if(!byProd[p])byProd[p]={items:[],recentS:0};byProd[p].items.push(a);byProd[p].recentS+=a._recentS});
  const colSpan=weekKeys.length+4;
  Object.keys(byProd).sort((a,b)=>byProd[b].recentS-byProd[a].recentS).forEach(prod=>{
    const g=byProd[prod];
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.items.length+'개)</td></tr>';
    g.items.forEach(a=>{
      const cells=weekKeys.map(wk=>{const w=a.w[wk];if(!w||!w.s)return'<td></td>';const roas=w.s>0?w.r/w.s*100:0;const cvr=w.clk>0?w.conv/w.clk*100:0;const ctr=w.imp>0?w.clk/w.imp*100:0;return'<td class="mc '+RC(roas)+'">'+MC(roas,w.r-w.s,w.s,w.r,cvr,null,ctr)+'</td>'}).join('');
      h+='<tr><td class="fx fx0">'+(a.cn||'').slice(0,25)+'</td><td class="fx fx1">'+(a.an||'').slice(0,25)+'</td><td style="font-size:9px">'+a.id+'</td><td class="mc '+RC(a._roas)+'">'+MC(a._roas,a._p,a._s,a._r,a._cvr,null,a._ctr)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';document.getElementById('nsawTbl').innerHTML=h;
}

// ===== 구글 디멘드젠 콘텐츠(ct)별 추이 (Mixpanel ch=google) =====
// 국내 채널 추이와 동일한 날짜×행 + ROAS/순이익/지출/매출/건수 셀 구조.
//   매출·건수: google_demandgen_content_mp_daily (Mixpanel, 구글_디멘드젠_mp_supabase.py)
//   지출:      google_demandgen_content_spend_daily (구글 Ads API, 구글_디멘드젠_api_supabase.py)
//   (date, content) 로 두 소스 조인 → ROAS = 매출/지출. 지출 없는 날은 매출/건수만.
function renderGgdgContent(){
  const tbl=document.getElementById('ggdgctTbl');
  if(!tbl)return;
  // lazy load + stale-while-revalidate:
  //  - 이번 세션에 fresh fetch 전(!loaded)이면 백그라운드로 최신 데이터를 가져오고,
  //    완료되면 이 탭이 활성일 때 재렌더 → 캐시에 없던 신규 날짜 지출/매출이 반영됨.
  //  - 캐시가 있으면 우선 그걸로 즉시 렌더(아래 진행), 없으면 로딩 표시.
  const loaded=window._BIG_LOADED&&window._BIG_LOADED.ggdgct;
  if(!loaded){
    ensureBigTable('ggdgct').then(()=>{
      const at=document.querySelector('.tab.active');
      if(at&&at.dataset.t==='ggdgct')renderGgdgContent();
    });
    if(!GGDG_CT.length&&!GGDG_SP.length){
      tbl.innerHTML='<tr><td style="padding:24px;color:#888">구글 디멘드젠 콘텐츠 데이터 로딩 중…</td></tr>';
      return;
    }
  }
  if(!GGDG_CT.length&&!GGDG_SP.length){
    tbl.innerHTML='<tr><td style="padding:24px;color:#888">콘텐츠 데이터 없음 — <code>google_demandgen_content_mp_daily</code>(매출)/<code>google_demandgen_content_spend_daily</code>(지출) 테이블이 비어있거나 파이프라인 실행 전입니다.</td></tr>';
    return;
  }
  const days=parseInt(document.getElementById('ggdgDays').value)||30;
  const topN=parseInt(document.getElementById('ggdgTop').value)||20;
  const minRev=parseFloat(document.getElementById('ggdgMinRev').value)||0;
  // 매출·지출 양쪽 날짜 합집합 (디멘드젠 dg_ 콘텐츠 기준)
  const _isDgDate=c=>/^dg_/i.test(c||'');
  const allDates=[...new Set([...GGDG_CT.filter(r=>_isDgDate(r.content)).map(r=>r.date),...GGDG_SP.filter(r=>_isDgDate(r.content)).map(r=>r.date)])].sort().reverse().slice(0,days);
  if(!allDates.length){tbl.innerHTML='<tr><td>데이터 없음</td></tr>';return}
  const dateSet=new Set(allDates);
  const d7=allDates.slice(0,7),d7set=new Set(d7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // 콘텐츠(ct)별 그룹화 — d[date]={r:매출, c:건수, s:지출}
  // 디멘드젠(dg_)만 표시. sa_(검색광고) 등 다른 채널 콘텐츠는 제외.
  const isDg=ct=>/^dg_/i.test(ct||'');
  const byC={};
  const ensure=(ct,d)=>{if(!byC[ct])byC[ct]={ct,d:{}};if(!byC[ct].d[d])byC[ct].d[d]={r:0,c:0,s:0};return byC[ct].d[d]};
  GGDG_CT.forEach(r=>{if(!dateSet.has(r.date)||!isDg(r.content))return;const x=ensure(r.content,r.date);x.r+=(r.revenue||0);x.c+=(r.purchase_count||0)});
  GGDG_SP.forEach(r=>{if(!dateSet.has(r.date)||!isDg(r.content))return;const x=ensure(r.content,r.date);x.s+=(r.spend||0)});
  // 7일/전체 합산 + 정렬(7일 매출↓)
  let list=Object.values(byC).map(o=>{
    let r7=0,c7=0,s7=0,rt=0;
    allDates.forEach(d=>{const x=o.d[d];if(x){rt+=x.r;if(d7set.has(d)){r7+=x.r;c7+=x.c;s7+=x.s}}});
    o._r7=r7;o._c7=c7;o._s7=s7;o._rt=rt;o._p7=r7-s7;o._roas7=s7>0?r7/s7*100:0;
    return o;
  }).filter(o=>o._rt>=minRev||o._s7>0);
  // content → country: 지출 테이블 country(캠페인명 TW 태그 기준) 우선, 없으면 content 이름 tw 토큰 폴백.
  const _twName=ct=>/(^|[_\-\s])tw([_\-\s]|$)/i.test(ct||'');
  const ctCountry={};
  GGDG_SP.forEach(r=>{const c=(r.country||'').toUpperCase();if(!c)return;if(c==='TW'||ctCountry[r.content]==='TW')ctCountry[r.content]='TW';else if(!ctCountry[r.content])ctCountry[r.content]=c});
  const countryOf=ct=>ctCountry[ct]||(_twName(ct)?'TW':'KR');
  const ths=allDates.map(d=>{const w=WD(d);const yd=d===yDay?' col-yday':'';return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')</th>'}).join('');
  // 셀: 지출 있으면 ROAS/순이익/지출/매출/건수, 없으면 매출/건수만
  const cell=(s,r,c)=>{
    if(!s&&!r&&!c)return'';
    let h='';
    if(s>0){const roas=r/s*100,p=r-s;h+='<div class="r">'+roas.toFixed(0)+'</div><div class="'+(p>=0?'p':'p neg')+'">'+moneyKRW(p)+'</div><div class="s">-'+moneyKRW(s)+'</div>'}
    h+='<div class="rv">'+moneyKRW(r)+'</div><div class="cv" style="color:#888">'+(c||0)+'건</div>';
    return h;
  };
  let h='<thead><tr><th style="min-width:260px;text-align:left">콘텐츠 (ct)</th><th style="min-width:120px">7일 합계</th>'+ths+'</tr></thead><tbody>';
  const colSpan=allDates.length+2;
  // 국가 그룹(대만/국내) 분리 — 각 그룹 종합 + Top N 콘텐츠
  [['TW','🇹🇼 대만 (캠페인명 TW 태그)'],['KR','🇰🇷 국내']].forEach(([cc,label])=>{
    const g=list.filter(o=>countryOf(o.ct)===cc).sort((a,b)=>b._r7-a._r7).slice(0,topN);
    if(!g.length)return;
    h+='<tr><td colspan="'+colSpan+'" style="text-align:left;font-weight:700;background:#e8eefc;padding:6px 8px">'+label+' · '+g.length+'개</td></tr>';
    const totD={};allDates.forEach(d=>{let r=0,c=0,s=0;g.forEach(o=>{const x=o.d[d];if(x){r+=x.r;c+=x.c;s+=x.s}});totD[d]={r,c,s}});
    const t7r=d7.reduce((a,d)=>a+(totD[d]?.r||0),0),t7c=d7.reduce((a,d)=>a+(totD[d]?.c||0),0),t7s=d7.reduce((a,d)=>a+(totD[d]?.s||0),0);
    const t7roas=t7s>0?t7r/t7s*100:0;
    h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8;font-weight:700;text-align:left">'+(cc==='TW'?'대만':'국내')+' 종합</td><td class="mc '+(t7s>0?RC(t7roas):'')+'" style="background:#e8e8e8;font-weight:700">'+cell(t7s,t7r,t7c)+'</td>';
    allDates.forEach(d=>{const x=totD[d];const yd=d===yDay?' col-yday':'';const roas=x.s>0?x.r/x.s*100:0;h+='<td class="mc '+(x.s>0?RC(roas):'')+yd+'">'+cell(x.s,x.r,x.c)+'</td>'});
    h+='</tr>';
    g.forEach(o=>{
      const cells=allDates.map(d=>{const x=o.d[d];const yd=d===yDay?' col-yday':'';if(!x||(!x.r&&!x.c&&!x.s))return'<td class="'+yd+'"></td>';const roas=x.s>0?x.r/x.s*100:0;return'<td class="mc '+(x.s>0?RC(roas):'')+yd+'">'+cell(x.s,x.r,x.c)+'</td>'}).join('');
      h+='<tr><td class="fx fx0" style="text-align:left;font-size:11px">'+o.ct+'</td><td class="mc '+(o._s7>0?RC(o._roas7):'')+'" style="font-weight:700">'+cell(o._s7,o._r7,o._c7)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';tbl.innerHTML=h;
}

// ===== 구글 디멘드젠 [Tight] 캠페인별 추이 (국내 탭) =====
// 캠페인 id × 일자. 지출=Google Ads API, 매출/구매=Mixpanel utm_campaign 매칭.
//   단일 테이블 google_demandgen_campaign_daily (구글_디멘드젠_캠페인_supabase.py).
//   추이차트와 동일 셀(ROAS/순이익/-지출/매출/구매건) + ROAS 색상밴드. 최신 날짜 왼쪽.
// budget_apply_log(region='gd') 지연 로드. 실패는 캐시하지 않아 다음 렌더에서 재시도한다.
//   ok=true 만 — 실패한 적용은 예산이 실제로 안 바뀌었으므로 테두리를 그리면 거짓말이 된다.
function ensureGgdgChg(cb){
  if(_GGDG_CHG_LOADED){cb&&cb();return}
  sbQ('budget_apply_log','select=adset_id,applied_at,tag,field,before_value,after_value&region=eq.gd&ok=is.true&order=applied_at.desc&limit=3000')
    .then(d=>{GGDG_CHG=Array.isArray(d)?d:[];_GGDG_CHG_LOADED=true;cb&&cb();})
    .catch(()=>{GGDG_CHG=[];cb&&cb();});
}
// (ad_group_id, KST날짜) → {tag, 툴팁문구}. 같은 날 여러 번 적용했으면 마지막(최신) 것을 쓴다.
function _ggdgChgMap(){
  const m={};
  (GGDG_CHG||[]).forEach(r=>{
    const id=String(r.adset_id||'');if(!id||!r.applied_at)return;
    const t=new Date(r.applied_at);if(isNaN(t))return;
    const k=new Date(t.getTime()+9*3600*1000);   // UTC → KST
    const d=k.getUTCFullYear()+'-'+String(k.getUTCMonth()+1).padStart(2,'0')+'-'+String(k.getUTCDate()).padStart(2,'0');
    const tag=String(r.tag||'');if(!HL_CONFIG[tag])return;
    const lbl=r.field==='status'
      ? (String(r.after_value||'')==='PAUSED'?'OFF':String(r.after_value||''))
      : HL_CONFIG[tag].label+' '+(+r.before_value||0).toLocaleString('ko-KR')+'→'+(+r.after_value||0).toLocaleString('ko-KR');
    (m[id]||(m[id]={}));
    // applied_at desc 로 받아오므로 먼저 들어온 것이 최신 → 이미 있으면 덮지 않는다
    if(!m[id][d])m[id][d]={tag,txt:d+' '+lbl};
  });
  return m;
}

function renderGgdgTight(){
  const tbl=document.getElementById('ggdgkrTbl');
  if(!tbl)return;
  // 증감액 테두리 데이터가 아직 없으면 로드 후 재렌더 (표는 먼저 그려서 체감 지연 없음)
  if(!_GGDG_CHG_LOADED)ensureGgdgChg(()=>{const at=document.querySelector('.tab.active');if(at&&at.dataset.t==='ggdgkr')renderGgdgTight()});
  const loaded=window._BIG_LOADED&&window._BIG_LOADED.ggdgkr;
  if(!loaded){
    ensureBigTable('ggdgkr').then(()=>{
      const at=document.querySelector('.tab.active');
      if(at&&at.dataset.t==='ggdgkr')renderGgdgTight();
    });
    if(!GGDG_TIGHT.length){
      tbl.innerHTML='<tr><td style="padding:24px;color:#888">구글 디멘드젠 [Tight] 데이터 로딩 중…</td></tr>';
      return;
    }
  }
  if(!GGDG_TIGHT.length){
    tbl.innerHTML='<tr><td style="padding:24px;color:#888">데이터 없음 — <code>google_demandgen_campaign_daily</code> 테이블이 비어있거나 파이프라인(google-dg-tight) 실행 전입니다.</td></tr>';
    return;
  }
  const days=parseInt(document.getElementById('ggdgkrDays').value)||30;
  const view=document.getElementById('ggdgkrView')?.value||'day';
  const kw=(document.getElementById('ggdgkrFilter')?.value||'').trim().toLowerCase();
  // 최신 날짜 왼쪽: 내림차순 정렬 후 days 만큼
  const allDates=[...new Set(GGDG_TIGHT.map(r=>r.date))].sort().reverse().slice(0,days);
  if(!allDates.length){tbl.innerHTML='<tr><td>데이터 없음</td></tr>';return}
  const dateSet=new Set(allDates);
  // 첫 컬럼 = 최근 7일 일평균 (2026-08-08: 기존 '기간합계'에서 변경).
  //   allDates 는 최신순이라 앞 7개가 최근 7일. 선택 기간이 더 짧으면 있는 만큼으로 나눈다.
  //   지출/매출/건수는 일평균, ROAS 는 비율이라 7일 합계 기준과 동일(평균끼리 나눠도 같음).
  const avgDates=allDates.slice(0,7), AVGN=avgDates.length||1;
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // 예산 — 구글 디멘드젠은 예산이 **캠페인 단위**라 파이프라인이 광고그룹별 최신 날짜 행에만
    //   현재 캠페인 예산을 적는다(구글_디멘드젠_캠페인_supabase.py). 여기서는 세트별로
    //   '예산이 들어있는 가장 최근 행'을 집는다 — 표시 기간(dateSet) 밖도 허용해야
    //   기간을 14일로 좁혔을 때 값이 사라지지 않는다.
  const GBUD={};
  GGDG_TIGHT.forEach(r=>{const b=+r.budget||0;if(!(b>0))return;const id=r.ad_group_id;const p=GBUD[id];if(!p||r.date>p.d)GBUD[id]={d:r.date,b:b}});
  // 그룹화 — 행=세트(ad_group_id), 상품(prod)은 표시용 묶음 키. d[date]={s,r,c}
  const byC={};
  GGDG_TIGHT.forEach(r=>{if(!dateSet.has(r.date))return;const id=r.ad_group_id;if(!byC[id])byC[id]={id,camp:r.campaign_name||'',name:r.ad_group_name||id,prod:_dgProduct(r.ad_group_name||r.campaign_name),d:{}};/* 이름은 ★선택 기간 내 최신 날짜 행★ 기준 (2026-08-12). 구글에서 캠페인/광고그룹 이름을 바꾸면
   파이프라인이 재적재 창 안쪽 날짜만 새 이름으로 갱신해 한 id 에 옛/새 이름이 공존한다.
   무조건 덮어쓰면 date.desc 로드 순서상 '가장 오래된 이름'이 남아 새 이름으로는 표·검색에서
   사라진다(예: 23948103750 이 0618_구미호_2nd → 0811_임신 개명 후 구미호로만 보이던 건). */
if(r.date>=(byC[id]._nd||'')){if(r.campaign_name)byC[id].camp=r.campaign_name;if(r.ad_group_name){byC[id].name=r.ad_group_name;byC[id].prod=_dgProduct(r.ad_group_name||r.campaign_name)}byC[id]._nd=r.date}const x=byC[id].d[r.date]||(byC[id].d[r.date]={s:0,r:0,c:0});x.s+=(r.spend||0);x.r+=(r.revenue||0);x.c+=(r.purchase_count||0)});
  // 보기(일/주/월) → 컬럼 = 기간키. colOf[key]=[dates]
  const colKey=d=>view==='week'?WM(d):view==='month'?d.slice(0,7):d;
  const colOf={};allDates.forEach(d=>{const k=colKey(d);(colOf[k]||(colOf[k]=[])).push(d)});
  const cols=Object.keys(colOf).sort().reverse();
  const colLabel=k=>{if(view==='month'){const p=k.split('-');return p[0].slice(2)+'/'+p[1]}
    if(view==='week'){const s=new Date(k),e=new Date(k);e.setDate(e.getDate()+6);return(s.getMonth()+1)+'/'+s.getDate()+'~'+(e.getMonth()+1)+'/'+e.getDate()}
    return DK(k)+'('+WD(k)+')'};
  const setCol=(o,k)=>{let s=0,r=0,c=0;colOf[k].forEach(d=>{const x=o.d[d];if(x){s+=x.s;r+=x.r;c+=x.c}});return{s,r,c}};
  // 소재 펼침(toggleGgdgCreatives)이 세트 행과 똑같은 컬럼을 그리도록 현재 컬럼 구성을 보관
  _GGDG_COLCFG={cols,colOf,view,yDay,allDates,avgDates,AVGN};
  // 기간합계(_s/_r/_c, 정렬·소계용) + 최근 7일 합계(_a*, 첫 컬럼 표시용) + 검색필터
  //   정렬은 그대로 어제 지출↓ → 기간 지출↓.
  let list=Object.values(byC).map(o=>{let s=0,r=0,c=0;allDates.forEach(d=>{const x=o.d[d];if(x){s+=x.s;r+=x.r;c+=x.c}});o._s=s;o._r=r;o._c=c;o._roas=s>0?r/s*100:0;o._sy=o.d[yDay]?.s||0;
    let as=0,ar=0,ac=0;avgDates.forEach(d=>{const x=o.d[d];if(x){as+=x.s;ar+=x.r;ac+=x.c}});o._as=as;o._ar=ar;o._ac=ac;
    o._bud=(GBUD[o.id]||{}).b||0;return o});
  list=list.filter(o=>o._s>0||o._r>0);
  if(kw)list=list.filter(o=>((o.camp||'')+' '+(o.name||'')+' '+(o.id||'')).toLowerCase().includes(kw));
  list.sort((a,b)=>(b._sy-a._sy)||(b._s-a._s));
  GGDG_ROWS=list;   // '구글에 예산 적용' 대상 = 화면에 실제로 보이는 광고그룹
  abSyncBtnG();
  // 셀: 지출 있으면 ROAS/순이익/-지출/매출/건수, 없으면 매출/건수만
  const cell=(s,r,c)=>{
    if(!s&&!r&&!c)return'';
    let h='';
    if(s>0){const roas=r/s*100,p=r-s;h+='<div class="r">'+roas.toFixed(0)+'</div><div class="'+(p>=0?'p':'p neg')+'">'+moneyKRW(p)+'</div><div class="s">-'+moneyKRW(s)+'</div>'}
    h+='<div class="rv">'+moneyKRW(r)+'</div><div class="cv" style="color:#888">'+(c||0)+'건</div>';
    return h;
  };
  // 7일 평균 셀 — 7일 합계를 일수로 나눠 표시. 건수는 소수 1자리(하루 평균 2.4건).
  const cellAvg=(s,r,c)=>cell(s/AVGN,r/AVGN,Math.round(c/AVGN*10)/10);
  const ths=cols.map(k=>{const isSun=view==='day'&&WD(k)==='일';const yd=(view==='day'&&k===yDay)?' col-yday':'';return'<th class="'+(isSun?'sun':'')+yd+'" style="min-width:var(--cw)">'+colLabel(k)+'</th>'}).join('');
  const avgTitle='최근 '+AVGN+'일('+(avgDates[avgDates.length-1]||'')+'~'+(avgDates[0]||'')+') 일평균 · ROAS는 7일 합계 기준 · 당일(오늘)은 진행 중이라 부분값';
  let h='<thead><tr><th class="fx fx0" style="min-width:200px;text-align:left">캠페인</th><th style="min-width:200px;text-align:left">세트</th><th style="min-width:110px;text-align:left">세트ID</th>'
    +'<th class="hbud" title="현재 일예산 — 디멘드젠은 예산이 캠페인 단위라 같은 캠페인의 세트마다 같은 값이 반복된다(세로로 더하지 말 것). 날짜별 이력이 아니라 지금 설정값의 스냅샷">예산</th>'
    +'<th style="min-width:120px" title="'+avgTitle+'">7일 평균</th>'+ths+'</tr></thead><tbody>';
  // 종합행
  const totCol={};cols.forEach(k=>{let s=0,r=0,c=0;list.forEach(o=>{const x=setCol(o,k);s+=x.s;r+=x.r;c+=x.c});totCol[k]={s,r,c}});
  const gS=list.reduce((a,o)=>a+o._as,0),gR=list.reduce((a,o)=>a+o._ar,0),gC=list.reduce((a,o)=>a+o._ac,0),gRoas=gS>0?gR/gS*100:0;
  h+='<tr class="sr"><td colspan="4" style="background:#e8e8e8;font-weight:700;text-align:left">종합 ([Tight] '+list.length+'세트'+(kw?' · 검색결과':'')+')</td><td class="mc '+(gS>0?RC(gRoas):'')+'" style="background:#e8e8e8;font-weight:700" title="'+avgTitle+'">'+cellAvg(gS,gR,gC)+'</td>';
  cols.forEach(k=>{const x=totCol[k];const yd=(view==='day'&&k===yDay)?' col-yday':'';const roas=x.s>0?x.r/x.s*100:0;h+='<td class="mc '+(x.s>0?RC(roas):'')+yd+'">'+cell(x.s,x.r,x.c)+'</td>'});
  h+='</tr>';
  // 증감액 테두리 — '⚡ 구글에 예산 적용'으로 실제 반영된 날짜 셀에 색 링(메타 추이차트와 동일한 표현).
  //   주/월 보기에선 그 기간에 적용이 하나라도 있으면 링을 그리고, 여러 건이면 최신 건의 색을 쓴다.
  const CHGMAP=_ggdgChgMap();
  const chgOfCol=(id,k)=>{const per=CHGMAP[id];if(!per)return null;
    const ds=colOf[k].filter(d=>per[d]).sort();      // 기간 내 적용일
    if(!ds.length)return null;
    const last=per[ds[ds.length-1]];
    return {bg:HL_CONFIG[last.tag].bg, txt:ds.map(d=>per[d].txt).join(' / ')};
  };
  // 세트(광고그룹) 행 1개 생성
  const rowHtml=o=>{
    const cells=cols.map(k=>{const x=setCol(o,k);const yd=(view==='day'&&k===yDay)?' col-yday':'';
      const cg=chgOfCol(o.id,k);
      const cb=cg?' style="box-shadow:inset 0 0 0 3px '+cg.bg+'" title="증감액 적용: '+cg.txt.replace(/"/g,'&quot;')+'"':'';
      if(!x.s&&!x.r&&!x.c)return'<td class="'+yd+'"'+cb+'></td>';const roas=x.s>0?x.r/x.s*100:0;return'<td class="mc '+(x.s>0?RC(roas):'')+yd+'"'+cb+'>'+cell(x.s,x.r,x.c)+'</td>'}).join('');
    // 하이라이트 — 추이차트와 동일한 색상 피커·저장소(키=세트 id)
    const hl=hlClass(o.id);
    const ck=' clickable" data-id="'+o.id+'" onclick="showCPGgdg(\''+o.id+'\',this)"';
    // 캠페인 칸의 ▶ 캐럿 = 그 캠페인(=세트) 하위 소재 목록 펼치기/접기.
    //   '(세트미상)' 합성 행(camp_<id>)은 실제 광고가 없으므로 캐럿 없음.
    const caret=/^camp_/.test(o.id)?'':'<span class="ex-caret" onclick="event.stopPropagation();toggleGgdgCreatives(\''+o.id+'\',this.closest(\'tr\'))" title="소재 목록 펼치기/접기">▶</span>';
    const head='<td class="fx fx0 '+hl+ck+' style="text-align:left;font-size:11px">'+caret+(o.camp||'')+'</td><td class="'+hl+ck+' style="text-align:left;font-size:11px">'+(o.name||'')+'</td><td class="'+hl+ck+' style="text-align:left;font-size:9px;color:#888">'+o.id+'</td>'
      +'<td class="budc">'+(o._bud>0?moneyKRW(o._bud)+'<span style="color:#aaa;font-size:9px"> (캠)</span>':'')+'</td>';
    const aRoas=o._as>0?o._ar/o._as*100:0;
    return'<tr data-ggdg-row="'+o.id+'">'+head+'<td class="mc '+(o._as>0?RC(aRoas):'')+'" style="font-weight:700" title="'+avgTitle+'">'+cellAvg(o._as,o._ar,o._ac)+'</td>'+cells+'</tr>';
  };
  // 추이차트와 동일하게 상품별로 나눠서 표시 (📦 상품 헤더 + 상품 소계 + 세트 행).
  //   상품 판정=_dgProduct(세트명||캠페인명), 주간종합 탭의 디멘드젠 상품분해와 동일 기준.
  //   정렬: 상품은 전날 지출 큰 순, 상품 안의 세트는 전날 지출↓ → 기간 지출↓.
  {
    const byProd={};
    list.forEach(o=>{const p=o.prod||'기타';if(!byProd[p])byProd[p]={sets:[],sy:0};byProd[p].sets.push(o);byProd[p].sy+=(o._sy||0)});
    const colSpanAll=5+cols.length;   // 캠페인/세트/세트ID/예산/7일 평균 + 기간 컬럼
    Object.keys(byProd).sort((a,b)=>byProd[b].sy-byProd[a].sy).forEach(p=>{
      const g=byProd[p];
      const pS=g.sets.reduce((a,o)=>a+o._s,0),pR=g.sets.reduce((a,o)=>a+o._r,0);
      const pRoas=pS>0?pR/pS*100:0;   // 상품 헤더의 '기간 ROAS' — 선택 기간 전체 기준 유지
      // 소계 행의 첫 컬럼은 세트 행과 같은 최근 7일 일평균
      const paS=g.sets.reduce((a,o)=>a+o._as,0),paR=g.sets.reduce((a,o)=>a+o._ar,0),paC=g.sets.reduce((a,o)=>a+o._ac,0);
      const paRoas=paS>0?paR/paS*100:0;
      h+='<tr><td colspan="'+colSpanAll+'" class="prod-header">📦 '+p+' ('+g.sets.length+'개) 전날 '+(moneyKRW(g.sy)||'₩0')+' · 기간 ROAS '+pRoas.toFixed(0)+'%</td></tr>';
      const pCells=cols.map(k=>{let s=0,r=0,c=0;g.sets.forEach(o=>{const x=setCol(o,k);s+=x.s;r+=x.r;c+=x.c});const yd=(view==='day'&&k===yDay)?' col-yday':'';if(!s&&!r&&!c)return'<td class="'+yd+'" style="background:#e8e8e8"></td>';const roas=s>0?r/s*100:0;return'<td class="mc '+(s>0?RC(roas):'')+yd+'" style="background:#e8e8e8">'+cell(s,r,c)+'</td>'}).join('');
      h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8;text-align:left;font-weight:700">'+p+' 소계</td><td style="background:#e8e8e8"></td><td style="background:#e8e8e8"></td><td style="background:#e8e8e8"></td><td class="mc '+(paS>0?RC(paRoas):'')+'" style="background:#e8e8e8;font-weight:700" title="'+avgTitle+'">'+cellAvg(paS,paR,paC)+'</td>'+pCells+'</tr>';
      g.sets.forEach(o=>{h+=rowHtml(o)});
    });
  }
  h+='</tbody>';tbl.innerHTML=h;
}

// ===== 구글 디멘드젠 [Tight] — 캠페인(세트) 행 ▶ → 하위 소재(광고) 펼침 =====
// 소스: google_demandgen_ad_daily (구글_디멘드젠_캠페인_supabase.py 가 세트별 테이블과
//   같은 실행에서 함께 적재 — Ads API ad_group_ad 지출 + MP utm_content(=광고 id) 매출).
//   클릭한 세트의 소재만 Supabase에서 직접 fetch(세션 캐시) → 세트 행과 완전히 같은
//   컬럼 구성(_GGDG_COLCFG: 일/주/월 보기·기간)으로 소계 + 소재 행을 그린다.
//   ⚠ 매출은 utm_content 가 붙은 결제만 소재로 귀속되므로 소재 합계 ≤ 세트 매출(소계 행에 표시).
let _GGDG_COLCFG=null;
async function toggleGgdgCreatives(adGroupId,anchorRow){
  const tbody=anchorRow.parentNode;
  const sel='tr.creative-expanded[data-parent="'+CSS.escape(adGroupId)+'"]';
  const existing=tbody.querySelectorAll(sel);
  const caret=anchorRow.querySelector('.ex-caret');
  if(existing.length){existing.forEach(r=>r.remove());if(caret)caret.textContent='▶';return}
  if(caret)caret.textContent='⌛';
  await new Promise(r=>requestAnimationFrame(r));
  // 세트별 1회 fetch (in-flight 중복 방지)
  if(!window._GGDG_AD_CACHE)window._GGDG_AD_CACHE={};
  let recs=window._GGDG_AD_CACHE[adGroupId];
  if(recs===undefined){
    const url=SB_URL+'/rest/v1/google_demandgen_ad_daily?select=*&order=date.desc&ad_group_id=eq.'
      +encodeURIComponent(adGroupId)+'&date=gte.'+_dateCutoff(215)+'&limit=100000';
    const p=fetch(url,{headers:SBH}).then(r=>r.json()).catch(()=>[]);
    window._GGDG_AD_CACHE[adGroupId]=p;
    const got=await p;
    recs=window._GGDG_AD_CACHE[adGroupId]=Array.isArray(got)?got:[];
  }else if(recs&&typeof recs.then==='function'){
    const got=await recs;recs=Array.isArray(got)?got:[];
  }
  const cfg=_GGDG_COLCFG||{};
  const cols=cfg.cols||[],colOf=cfg.colOf||{},view=cfg.view||'day',yDay=cfg.yDay||'',allDates=cfg.allDates||[];
  const avgDates=cfg.avgDates||allDates.slice(0,7), AVGN=cfg.AVGN||avgDates.length||1;   // 첫 컬럼=최근 7일 일평균(세트 행과 동일)
  const dateSet=new Set(allDates);
  const totalCols=anchorRow.cells.length;
  const cell=(s,r,c)=>{
    if(!s&&!r&&!c)return'';
    let h='';
    if(s>0){const roas=r/s*100,p=r-s;h+='<div class="r">'+roas.toFixed(0)+'</div><div class="'+(p>=0?'p':'p neg')+'">'+moneyKRW(p)+'</div><div class="s">-'+moneyKRW(s)+'</div>'}
    h+='<div class="rv">'+moneyKRW(r)+'</div><div class="cv" style="color:#888">'+(c||0)+'건</div>';
    return h;
  };
  const cellAvg=(s,r,c)=>cell(s/AVGN,r/AVGN,Math.round(c/AVGN*10)/10);
  // 소재(ad_id)별 × 일자 집계
  const byA={};
  recs.forEach(r=>{if(!dateSet.has(r.date))return;
    const id=String(r.ad_id||'');if(!id)return;
    const a=byA[id]||(byA[id]={id,name:r.ad_name||r.ct||id,ct:r.ct||'',d:{}});
    if(r.ad_name)a.name=r.ad_name;
    const x=a.d[r.date]||(a.d[r.date]={s:0,r:0,c:0});
    x.s+=(+r.spend||0);x.r+=(+r.revenue||0);x.c+=(+r.purchase_count||0)});
  let list=Object.values(byA).map(a=>{let s=0,r=0,c=0;allDates.forEach(d=>{const x=a.d[d];if(x){s+=x.s;r+=x.r;c+=x.c}});
    a._s=s;a._r=r;a._c=c;a._roas=s>0?r/s*100:0;
    let as=0,ar=0,ac=0;avgDates.forEach(d=>{const x=a.d[d];if(x){as+=x.s;ar+=x.r;ac+=x.c}});a._as=as;a._ar=ar;a._ac=ac;a._aroas=as>0?ar/as*100:0;
    return a}).filter(a=>a._s>0||a._r>0||a._c>0);
  list.sort((a,b)=>(b._s-a._s)||(b._r-a._r));
  if(!list.length){
    const tr=document.createElement('tr');
    tr.className='creative-expanded';tr.setAttribute('data-parent',adGroupId);
    tr.innerHTML='<td colspan="'+totalCols+'" style="padding:8px 24px;background:#fff7e6;color:#999;font-size:11px;font-style:italic">↳ 소재 데이터 없음 (google_demandgen_ad_daily 미수집 또는 해당 기간 미운영)</td>';
    anchorRow.after(tr);if(caret)caret.textContent='▼';return;
  }
  const colOfSum=(a,k)=>{let s=0,r=0,c=0;(colOf[k]||[]).forEach(d=>{const x=a.d[d];if(x){s+=x.s;r+=x.r;c+=x.c}});return{s,r,c}};
  const trs=[];
  // 1) 소재 소계 — 세트 행과 비교용(매출 커버리지 확인)
  {
    const tS=list.reduce((x,a)=>x+a._as,0),tR=list.reduce((x,a)=>x+a._ar,0),tC=list.reduce((x,a)=>x+a._ac,0);
    const tRoas=tS>0?tR/tS*100:0;
    const cells=cols.map(k=>{let s=0,r=0,c=0;list.forEach(a=>{const x=colOfSum(a,k);s+=x.s;r+=x.r;c+=x.c});
      const yd=(view==='day'&&k===yDay)?' col-yday':'';
      if(!s&&!r&&!c)return'<td class="'+yd+'"></td>';
      const roas=s>0?r/s*100:0;return'<td class="mc '+(s>0?RC(roas):'')+yd+'">'+cell(s,r,c)+'</td>'}).join('');
    const tr=document.createElement('tr');
    tr.className='creative-expanded cr-subtotal';tr.setAttribute('data-parent',adGroupId);
    tr.innerHTML='<td class="fx fx0 cr-name">↳ 소재 소계 ('+list.length+'개)</td><td class="cr-name2"></td><td class="cr-id"></td><td></td>'
      +'<td class="mc '+(tS>0?RC(tRoas):'')+'" style="font-weight:700" title="최근 '+AVGN+'일 일평균">'+cellAvg(tS,tR,tC)+'</td>'+cells;
    trs.push(tr);
  }
  // 2) 개별 소재 행
  list.forEach(a=>{
    const cells=cols.map(k=>{const x=colOfSum(a,k);const yd=(view==='day'&&k===yDay)?' col-yday':'';
      if(!x.s&&!x.r&&!x.c)return'<td class="'+yd+'"></td>';
      const roas=x.s>0?x.r/x.s*100:0;return'<td class="mc '+(x.s>0?RC(roas):'')+yd+'">'+cell(x.s,x.r,x.c)+'</td>'}).join('');
    const nm=(a.name||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const ttl=(a.ct?('ct='+a.ct+' · '):'')+(a.name||'');
    const tr=document.createElement('tr');
    tr.className='creative-expanded';tr.setAttribute('data-parent',adGroupId);
    tr.innerHTML='<td class="fx fx0 cr-name"></td><td class="cr-name2" title="'+ttl.replace(/"/g,'&quot;')+'"><span style="color:#888">┗</span> '+nm+'</td>'
      +'<td class="cr-id" style="color:#888">'+a.id+'</td><td></td>'
      +'<td class="mc '+(a._as>0?RC(a._aroas):'')+'" style="font-weight:700" title="최근 '+AVGN+'일 일평균">'+cellAvg(a._as,a._ar,a._ac)+'</td>'+cells;
    trs.push(tr);
  });
  const frag=document.createDocumentFragment();
  trs.forEach(t=>frag.appendChild(t));
  anchorRow.parentNode.insertBefore(frag,anchorRow.nextSibling);
  if(caret)caret.textContent='▼';
}

// ===== 🎵 틱톡 (국내탭) — 추이차트와 같은 포맷 =====
// 소스 = 구글시트 '틱톡 캠페인 추이차트'(fill_tiktok.py 가 채운다) gid=0.
//   · 지출 = 틱톡 광고관리자 Campaign Report 실지출(활성 캠페인)
//   · 매출·판매수 = Mixpanel 결제완료(utm_id=캠페인ID 귀속 · order_id dedup)
//   틱톡은 Supabase 파이프라인이 없어 시트가 원장이다 → 시트를 gviz CSV 로 직접 읽고,
//   읽기에 실패하면 아래 스냅샷(마지막 확인 2026-08-09)으로 폴백한다.
const TT_SHEET_ID='1PYGM70GseCggr6oSxFDgOdw5TUe1IiU0VzKayxM1A4A';
const TT_SHEET_URL='https://docs.google.com/spreadsheets/d/'+TT_SHEET_ID+'/gviz/tq?tqx=out:csv&gid=0&headers=0';
const TT_SNAP_C={'1870671597111538':['무녀_헤드뱅잉_2600714',''],'1870672615781810':['무당_빙수범산_260714',''],'1872128852227393':['무당_aiUGC_ASC','-19만원'],'1872037759235186':['무당_애니메이션모음_ASC','-19만원'],'1872955572106417':['무당_애니메이션모음_ASC(2)','-10만원']};
const TT_SNAP_D=[
['2026-07-14','1870671597111538',36177,0,0],['2026-07-14','1870672615781810',50110,0,0],['2026-07-15','1870671597111538',54322,117000,3],['2026-07-15','1870672615781810',50111,136000,2],
['2026-07-16','1870671597111538',57343,59000,1],['2026-07-16','1870672615781810',39854,39000,1],['2026-07-17','1870671597111538',105057,39000,1],['2026-07-17','1870672615781810',100442,305600,6],
['2026-07-18','1870671597111538',94943,108800,2],['2026-07-18','1870672615781810',99205,0,0],['2026-07-19','1870671597111538',78304,0,0],['2026-07-19','1870672615781810',79334,46800,1],
['2026-07-20','1870671597111538',105162,78000,2],['2026-07-20','1870672615781810',120906,39000,1],['2026-07-21','1870671597111538',112909,78000,2],['2026-07-21','1870672615781810',106911,0,0],
['2026-07-22','1870671597111538',107238,107900,3],['2026-07-22','1870672615781810',101181,0,0],['2026-07-31','1872128852227393',13383,137100,3],['2026-07-31','1872037759235186',51560,96600,2],
['2026-08-01','1872128852227393',48884,101500,2],['2026-08-01','1872037759235186',125000,370700,7],['2026-08-02','1872128852227393',10279,49800,1],['2026-08-02','1872037759235186',100613,315700,7],
['2026-08-03','1872128852227393',14199,0,0],['2026-08-03','1872037759235186',57498,209400,4],['2026-08-04','1872128852227393',55575,159500,4],['2026-08-04','1872037759235186',55082,276900,7],
['2026-08-05','1872128852227393',19141,0,0],['2026-08-05','1872037759235186',120030,556700,16],['2026-08-06','1872128852227393',150000,602400,13],['2026-08-06','1872037759235186',121127,455300,11],
['2026-08-07','1872128852227393',48952,102500,3],['2026-08-07','1872037759235186',44350,149400,3],['2026-08-08','1872128852227393',46997,93600,2],['2026-08-08','1872037759235186',48200,116800,2],
['2026-08-08','1872955572106417',46651,199200,4],['2026-08-09','1872128852227393',44950,231800,7],['2026-08-09','1872037759235186',111968,213400,4],['2026-08-09','1872955572106417',36779,2900,1]];
const TT_SNAPSHOT=TT_SNAP_D.map(r=>({date:r[0],campaign:(TT_SNAP_C[r[1]]||['',''])[0],campaign_id:r[1],budget:(TT_SNAP_C[r[1]]||['',''])[1],spend:r[2],revenue:r[3],orders:r[4]}));
let TIKTOK=TT_SNAPSHOT.slice(),TT_SRC='스냅샷(08/09까지)',TT_LOADED=false,TT_LOADING=null;

// 따옴표 안 줄바꿈(셀 안 5줄)을 살리는 최소 CSV 파서
function _ttCSV(txt){
  const rows=[];let row=[],cur='',q=false;
  for(let i=0;i<txt.length;i++){const c=txt[i];
    if(q){ if(c==='"'){ if(txt[i+1]==='"'){cur+='"';i++} else q=false } else cur+=c }
    else if(c==='"')q=true;
    else if(c===','){row.push(cur);cur=''}
    else if(c==='\n'){row.push(cur);cur='';rows.push(row);row=[]}
    else if(c!=='\r')cur+=c;
  }
  if(cur!==''||row.length){row.push(cur);rows.push(row)}
  return rows;
}
function _ttNum(s){s=String(s).replace(/[,원건%+\s]/g,'');if(!s||s==='-')return null;const v=parseFloat(s);return isNaN(v)?null:v}
// 시트 → [{date,campaign,campaign_id,budget,spend,revenue,orders}]
//   헤더행 = '캠페인 ＼ 날짜', 날짜 컬럼 = 'MM/DD (요일)' (최신 왼쪽), 셀 = ROAS/순이익/지출/매출/판매수 5줄.
//   '전체' 행은 화면에서 다시 합산하므로 버린다(캠페인 합 = 전체 행과 일치함을 확인).
function _ttParseSheet(txt){
  const rows=_ttCSV(txt);
  const hi=rows.findIndex(r=>r&&/^(광고그룹|캠페인)/.test(String(r[0]||'').trim()));
  if(hi<0)return null;
  const head=rows[hi];
  let year=new Date().getFullYear();
  for(let i=0;i<hi;i++){const m=String(rows[i][0]||'').match(/(20\d\d)-\d\d-\d\d/);if(m){year=+m[1];break}}
  const cols=[];let prev='';
  for(let j=2;j<head.length;j++){
    const m=String(head[j]||'').trim().match(/^(\d\d)\/(\d\d)/);
    if(!m)continue;
    let d=year+'-'+m[1]+'-'+m[2];
    if(prev&&d>prev){year--;d=year+'-'+m[1]+'-'+m[2]}  // 왼쪽이 최신 → 날짜가 커지면 연도 넘어감(12월↔1월)
    prev=d;cols.push([j,d]);
  }
  const out=[];
  for(let i=hi+1;i<rows.length;i++){
    const r=rows[i];const raw=String(r[0]||'').trim();
    if(!raw||raw.indexOf('ROAS 배경색')===0)continue;
    const nm=raw.split('\n').map(s=>s.trim()).filter(Boolean);
    const cname=nm[0];
    if(!cname||cname==='전체')continue;
    // A열 라벨 = 광고그룹명(2026-08-20~ 단독). 구 형식은 캠페인명 / '└ 광고그룹명' / 예산 메모.
    // 광고그룹을 복사하면 그룹명이 (2-1)·(2-2) 로 갈려 캠페인명만으론 행을 구분할 수 없다.
    const gl=nm.slice(1).filter(x=>x.indexOf('└')===0);
    const gname=gl.length?gl[0].replace(/^└\s*/,''):cname;
    // 예산은 2026-08-18부터 C열이 원장(광고관리자 설정 스냅샷, fill_tiktok.py --budget).
    // 그 이전엔 A열에 손으로 적어두던 메모였어서 C열이 비면 옛 방식으로 폴백한다.
    const budget=String(r[2]||'').trim()||nm.slice(1).filter(x=>x.indexOf('└')!==0)[0]||'';
    const cid=String(r[1]||'').trim();
    for(let k=0;k<cols.length;k++){
      const j=cols[k][0],d=cols[k][1];
      const c=String(r[j]||'').trim();
      if(!c)continue;
      const p=c.split('\n').map(s=>s.trim());
      if(p.length<4)continue;
      const spend=_ttNum(p[2]),rev=_ttNum(p[3]),ord=p.length>4?_ttNum(p[4]):null;
      if(spend==null&&rev==null)continue;
      out.push({date:d,campaign:cname,adgroup:gname,campaign_id:cid,budget:budget,spend:Math.abs(spend||0),revenue:rev||0,orders:ord||0});
    }
  }
  return out.length?out:null;
}
function loadTiktok(force){
  if(TT_LOADED&&!force)return Promise.resolve();
  if(TT_LOADING&&!force)return TT_LOADING;
  TT_LOADING=fetch(TT_SHEET_URL+'&_='+Date.now(),{cache:'no-store'})
    .then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.text()})
    .then(t=>{const d=_ttParseSheet(t);if(!d)throw new Error('시트 형식이 바뀜');
      TIKTOK=d;TT_SRC='시트 실시간';TT_LOADED=true})
    .catch(e=>{TIKTOK=TT_SNAPSHOT.slice();TT_SRC='스냅샷 폴백(시트 읽기 실패: '+(e.message||e)+')';TT_LOADED=true})
    .then(()=>{TT_LOADING=null});
  return TT_LOADING;
}
// 틱톡 셀 — 추이차트 MC 와 같은 배치. 클릭/노출이 없어 CVR·CPM 대신 판매수(건)를 넣는다.
function TTC(roas,profit,spend,revenue,orders,cpa){
  if(!spend&&!revenue)return'';
  const pc=profit>=0?'p':'p neg';
  let h='<div class="r">'+(spend>0?roas.toFixed(0):'')+'</div>'
    +'<div class="'+pc+'">'+money(profit)+'</div>'
    +'<div class="s">'+(spend?'-'+money(spend):'')+'</div>'
    +'<div class="rv">'+money(revenue)+'</div>'
    +'<div class="cv">'+(orders?orders+'건':'')+'</div>';
  if(cpa)h+='<div class="cpa">'+money(cpa)+'</div>';
  return h;
}
// 캠페인명 앞머리(무당_/무녀_)를 상품으로 — 추이차트의 📦 상품별 소계와 같은 모양
function _ttProduct(name){const s=String(name||'');const i=s.indexOf('_');return (i>0?s.slice(0,i):s)||'기타'}
function renderTiktok(){
  const tbl=document.getElementById('ttTbl');if(!tbl)return;
  const info=document.getElementById('ttInfo');
  const days=parseInt(document.getElementById('ttDays').value)||30;
  const kw=(document.getElementById('ttFilter').value||'').trim().toLowerCase();
  const dd=[...new Set(TIKTOK.map(r=>r.date))].sort().reverse().slice(0,days);
  if(info)info.innerHTML=' · 데이터 '+TT_SRC+(dd.length?' · 최신 '+dd[0]:'');
  if(!dd.length){tbl.innerHTML='<tr><td style="padding:12px;color:#888">틱톡 데이터 없음 — 시트를 읽지 못했습니다</td></tr>';return}
  const d7=dd.slice(0,7);
  const _yd=new Date();_yd.setDate(_yd.getDate()-1);
  const yDay=_yd.getFullYear()+'-'+String(_yd.getMonth()+1).padStart(2,'0')+'-'+String(_yd.getDate()).padStart(2,'0');
  // 캠페인별 그룹화
  const byC={};
  TIKTOK.forEach(r=>{
    if(dd.indexOf(r.date)<0)return;
    const id=r.campaign_id||r.campaign;
    if(!byC[id])byC[id]={cn:r.campaign,gn:r.adgroup||r.campaign,id:id,bud:r.budget||'',product:_ttProduct(r.campaign),d:{}};
    if(r.budget)byC[id].bud=r.budget;
    byC[id].d[r.date]=r;
  });
  let list=Object.values(byC);
  if(kw)list=list.filter(a=>((a.cn||'')+' '+(a.gn||'')+' '+(a.id||'')).toLowerCase().indexOf(kw)>=0);
  list.forEach(a=>{let s=0,rv=0,o=0;d7.forEach(d=>{const x=a.d[d];if(x){s+=x.spend;rv+=x.revenue;o+=x.orders}});
    a._s=s;a._r=rv;a._p=rv-s;a._o=o;a._roas=s>0?rv/s*100:0;a._cpa=o>0?s/o:0;
    a._yS=a.d[yDay]?a.d[yDay].spend:0;
    // 일예산: 시트 값이 '-100,000' 같은 문자열이라 숫자만 뽑아 정렬 기준으로 쓴다.
    a._bud=Math.abs(parseFloat(String(a.bud||'').replace(/[^0-9.\-]/g,''))||0);});
  list.sort((a,b)=>tSortMode()==='budget'?((b._bud-a._bud)||(b._yS-a._yS)||(b._s-a._s)):((b._yS-a._yS)||(b._s-a._s)));
  const ths=dd.map(d=>{const w=WD(d);const yd=d===yDay?' col-yday':'';return'<th class="'+(w==='일'?'sun':'')+yd+'" style="min-width:var(--cw)">'+DK(d)+'('+w+')</th>'}).join('');
  const colSpan=dd.length+4;  // 캠페인/ID/일예산/7일
  const agg=(items,d)=>{let s=0,r=0,o=0;items.forEach(a=>{const x=a.d[d];if(x){s+=x.spend;r+=x.revenue;o+=x.orders}});return{s,r,o,p:r-s,roas:s>0?r/s*100:0,cpa:o>0?s/o:0}};
  const cellsOf=items=>dd.map(d=>{const t=agg(items,d);const yd=d===yDay?' col-yday':'';
    return (t.s||t.r)?'<td class="mc '+RC(t.roas)+yd+'">'+TTC(t.roas,t.p,t.s,t.r,t.o,t.cpa)+'</td>':'<td class="'+yd+'"></td>'}).join('');
  const sum7=items=>{let s=0,r=0,o=0;d7.forEach(d=>{const t=agg(items,d);s+=t.s;r+=t.r;o+=t.o});return{s,r,o,p:r-s,roas:s>0?r/s*100:0,cpa:o>0?s/o:0}};
  let h='<thead><tr><th style="text-align:left;white-space:nowrap">광고그룹</th><th style="min-width:130px">광고그룹 ID</th><th style="min-width:70px">일예산</th><th>7일</th>'+ths+'</tr></thead><tbody>';
  // 종합
  const T=sum7(list);
  h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">종합 ('+list.length+'개)</td><td class="fx fx1" style="background:#e8e8e8"></td>'
    +'<td style="background:#e8e8e8"></td>'   // 일예산 칸은 색 없이 비워둔다
    +'<td class="mc '+RC(T.roas)+'">'+TTC(T.roas,T.p,T.s,T.r,T.o,T.cpa)+'</td>'+cellsOf(list)+'</tr>';
  // 📦 상품별
  const byProd={};list.forEach(a=>{const p=a.product;if(!byProd[p])byProd[p]={items:[],yS:0,s:0,bud:0};byProd[p].items.push(a);byProd[p].yS+=a._yS;byProd[p].s+=a._s;byProd[p].bud+=a._bud});
  orderProdKeys(byProd).forEach(prod=>{
    const g=byProd[prod];const P7=sum7(g.items);
    h+='<tr><td colspan="'+colSpan+'" class="prod-header">📦 '+prod+' ('+g.items.length+'개) 전날 '+money(g.yS)+' · 7일 ROAS '+P7.roas.toFixed(0)+'%</td></tr>';
    h+='<tr class="sr"><td class="fx fx0" style="background:#e8e8e8">'+prod+' 소계</td><td class="fx fx1" style="background:#e8e8e8"></td><td></td>'
      +'<td class="mc '+RC(P7.roas)+'">'+TTC(P7.roas,P7.p,P7.s,P7.r,P7.o,P7.cpa)+'</td>'+cellsOf(g.items)+'</tr>';
    g.items.forEach(a=>{
      const cells=dd.map(d=>{const x=a.d[d];const yd=d===yDay?' col-yday':'';
        if(!x||(!x.spend&&!x.revenue))return'<td class="'+yd+'"></td>';
        const roas=x.spend>0?x.revenue/x.spend*100:0;
        return'<td class="mc '+RC(roas)+yd+'">'+TTC(roas,x.revenue-x.spend,x.spend,x.revenue,x.orders,x.orders>0?x.spend/x.orders:0)+'</td>'}).join('');
      const lbl=a.gn&&a.gn!==a.cn?a.cn+'<div style="font-size:9px;color:#888">└ '+a.gn+'</div>':a.cn;
      h+='<tr><td class="fx fx0">'+lbl+'</td><td class="fx fx1" style="font-size:9px">'+a.id+'</td><td style="font-size:10px">'+String(a.bud||'').replace(/^-/,'')+'</td>'
        +'<td class="mc '+RC(a._roas)+'">'+TTC(a._roas,a._p,a._s,a._r,a._o,a._cpa)+'</td>'+cells+'</tr>';
    });
  });
  h+='</tbody>';tbl.innerHTML=h;
  requestAnimationFrame(()=>_fixSticky(tbl));
}

// ===== 열 너비 (추이차트 날짜 열) =====
// 모든 추이차트의 날짜/기간 열은 min-width:var(--cw) 를 쓴다 → 변수 하나만 바꾸면
// 재렌더 없이 전 탭이 같이 움직인다. 라벨·합계 열은 고정(가로 스크롤 기준점).
const CW_DEF=96,CW_MIN=52,CW_MAX=200;
let colWidth=CW_DEF;
function applyCW(){
  document.documentElement.style.setProperty('--cw',colWidth+'px');
  const el=document.getElementById('cwPx');if(el)el.textContent=colWidth+'px';
  try{localStorage.setItem('cw',colWidth)}catch(e){}
}
function cwChange(d){colWidth=Math.max(CW_MIN,Math.min(CW_MAX,colWidth+d));applyCW()}
function cwReset(){colWidth=CW_DEF;applyCW()}
(function(){try{const v=parseInt(localStorage.getItem('cw'));
  if(v>=CW_MIN&&v<=CW_MAX)colWidth=v}catch(e){}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',applyCW);
  else applyCW();})();

// ===== ZOOM =====
let zoomLevel=100;
function applyZoom(){document.querySelectorAll('.panel').forEach(p=>{p.style.transformOrigin='top left';p.style.transform='scale('+(zoomLevel/100)+')';p.style.width=(10000/zoomLevel)+'%'});document.getElementById('zoomPct').textContent=zoomLevel+'%'}
function zoomChange(d){zoomLevel=Math.max(50,Math.min(200,zoomLevel+d));applyZoom()}
function zoomReset(){zoomLevel=100;applyZoom()}

// ===== AUTH (Supabase) =====
// 비밀번호는 더 이상 코드에 없음(해시조차). Supabase Auth가 서버에서 검증하고,
// 성공 시 받은 access token으로만 데이터가 읽힘 → 공개 키 노출이 무해해짐.
async function tryLogin(){
  const pw=document.getElementById('loginPw').value;
  document.getElementById('loginErr').textContent='확인 중…';
  const {data,error}=await SBC.auth.signInWithPassword({email:DASH_EMAIL,password:pw});
  if(error||!data||!data.session){document.getElementById('loginErr').textContent='비밀번호가 맞지 않습니다.';return}
  SBH.Authorization='Bearer '+data.session.access_token;
  showApp();
}
function showApp(){document.getElementById('loginScreen').style.display='none';document.getElementById('appContent').classList.add('ready');initData()}
async function logout(){try{await SBC.auth.signOut()}catch(e){}location.reload()}
document.getElementById('loginPw').addEventListener('keydown',e=>{if(e.key==='Enter')tryLogin()});
// 기존 세션이 있으면 자동 로그인 (supabase-js가 세션을 localStorage에 보존·자동갱신)
// ★ getSession() 은 저장된 세션을 '만료 여부와 무관하게 그대로' 돌려준다.
//   그 토큰으로 initData 가 출발하면 코어 fetch 가 전부 401(JWT expired) 이 되고,
//   새로고침해도 localStorage 의 같은 만료 토큰을 다시 집어 무한 반복된다(2026-08-23 실제 사고).
//   → 만료 60초 전이면 여기서 먼저 갱신하고, 갱신이 실패하면 로그인 화면을 띄운다.
(async()=>{try{
  let {data:{session}}=await SBC.auth.getSession();
  if(!session){document.getElementById('loginPw').focus();return}
  if(((session.expires_at||0)*1000)-Date.now()<60000){
    const {data,error}=await SBC.auth.refreshSession();
    if(error||!data||!data.session){
      document.getElementById('loginErr').textContent='세션이 만료되었습니다. 다시 로그인해 주세요.';
      document.getElementById('loginPw').focus();return;
    }
    session=data.session;
  }
  SBH.Authorization='Bearer '+session.access_token;showApp();
}catch(e){document.getElementById('loginPw').focus()}})();

// ===== 🏢 경쟁사분석 (국내 전용) =====
//   2026-08-18 이전엔 '경쟁사분석' 구글시트를 브라우저가 gviz CSV 로 직접 읽었다.
//   지금은 수집 스크립트가 대시보드 DB(new-tightauto)에 바로 적재하고 여기서 읽는다.
//   · gviz 는 숨긴 행을 조용히 빼고, 숫자 컬럼의 문자열 헤더를 빈칸으로 준다(틱톡 탭에서 실제 사고)
//   · 시트를 브라우저가 읽으려면 '링크가 있는 누구나' 공개여야 했다 — 이제 RLS 뒤로 들어갔다
//   시트 적재는 그대로 유지된다(사람이 보는 뷰 + Drive 리비전 복구용).
let COMPET={adDay:null,adWeek:null,prodDay:null,prodWeek:null,paDay:null,paWeek:null},
    CP_ERR={},CP_LOADED=false,CP_LOADING=null;

// {period: {name: {total, free}}} 형태로 접는다 — 표 렌더러가 쓰는 모양.
function _cpPivot(rows,periodKey,nameKey,valKey,freeKey){
  const periods=[],seen={},out={};
  rows.forEach(function(r){
    const p=String(r[periodKey]||'').slice(0,10);
    const n=String(r[nameKey]||'');
    if(!p||!n)return;
    if(!seen[p]){seen[p]=1;periods.push(p)}
    (out[n]||(out[n]={}))[p]={total:+r[valKey]||0,
                              free:(freeKey&&r[freeKey]!=null)?+r[freeKey]:null};
  });
  periods.sort(function(a,b){return a<b?1:-1});   // 최신 좌측
  return {periods:periods,rows:Object.keys(out).map(function(n){return {name:n,vals:out[n]}})};
}
// 상품별: 회사 그룹 + 상품 행
function _cpPivotProducts(rows,periodKey,valKey){
  const periods=[],seen={},byCo={};
  rows.forEach(function(r){
    const p=String(r[periodKey]||'').slice(0,10);
    const co=String(r.company||''),pn=String(r.product||'');
    if(!p||!co||!pn)return;
    if(!seen[p]){seen[p]=1;periods.push(p)}
    const g=byCo[co]||(byCo[co]={items:{},vals:{}});
    (g.items[pn]||(g.items[pn]={}))[p]={total:+r[valKey]||0,free:null};
    g.vals[p]={total:(g.vals[p]?g.vals[p].total:0)+(+r[valKey]||0),free:null};
  });
  periods.sort(function(a,b){return a<b?1:-1});
  return {periods:periods,groups:Object.keys(byCo).map(function(co){
    const g=byCo[co];
    return {company:co,vals:g.vals,all:null,
            items:Object.keys(g.items).map(function(pn){return {name:pn,vals:g.items[pn],all:null}})};
  })};
}
function _cpLabel(k){
  const m=String(k).match(/^\d{4}-(\d\d)-(\d\d)/);
  return m?((+m[1])+'/'+(+m[2])):String(k);
}
// 회사 그레인 일별 광고수 = 페이지 그레인 합
function _cpFoldAdDaily(rows){
  const agg={};
  rows.forEach(function(r){
    const k=String(r.date).slice(0,10)+' '+r.company;
    agg[k]=(agg[k]||0)+(+r.ads||0);
  });
  return _cpPivot(Object.keys(agg).map(function(k){
    const p=k.split(' ');return {date:p[0],company:p[1],ads:agg[k]};
  }),'date','company','ads',null);
}
const CP_DAYS_MAX=120, CP_WEEKS_MAX=80;
function _cpCut(days){const d=new Date();d.setDate(d.getDate()-days);return d.toISOString().slice(0,10)}
// ⚠ PostgREST 는 한 번에 1000행까지만 준다(db-max-rows). 경쟁사 주별 상품 테이블은
//    9천 행이라 페이징 없이 받으면 조용히 잘린다 — 2026-08-18 실측.
function _cpPage(table,q,acc,off){
  acc=acc||[];off=off||0;
  return sbQ(table,q+'&limit=1000&offset='+off).then(function(rows){
    acc=acc.concat(rows);
    if(rows.length<1000||acc.length>=60000)return acc;
    return _cpPage(table,q,acc,off+1000);
  });
}
function _cpLoad(key,table,q,fold){
  return _cpPage(table,q).then(function(rows){COMPET[key]=fold(rows);delete CP_ERR[key]})
    .catch(function(e){COMPET[key]=null;CP_ERR[key]=e.message||String(e)});
}
function loadCompet(force){
  if(CP_LOADED&&!force)return Promise.resolve();
  if(CP_LOADING&&!force)return CP_LOADING;
  const dCut=_cpCut(CP_DAYS_MAX), wCut=_cpCut(CP_WEEKS_MAX*7);
  CP_LOADING=Promise.all([
    _cpLoad('adDay','competitor_ad_daily','select=date,company,ads&date=gte.'+dCut+'',_cpFoldAdDaily),
    _cpLoad('adWeek','competitor_ad_weekly','select=week_start,company,ads&week_start=gte.'+wCut+'',
            function(r){return _cpPivot(r,'week_start','company','ads',null)}),
    _cpLoad('prodDay','competitor_product_count','select=date,site,total,free&date=gte.'+dCut+'',
            function(r){return _cpPivot(r,'date','site','total','free')}),
    _cpLoad('prodWeek','competitor_product_count_weekly','select=week_start,site,total,free&week_start=gte.'+wCut+'',
            function(r){return _cpPivot(r,'week_start','site','total','free')}),
    _cpLoad('paDay','competitor_product_ad_daily','select=date,company,product,ads&date=gte.'+dCut+'',
            function(r){return _cpPivotProducts(r,'date','ads')}),
    _cpLoad('paWeek','competitor_product_ad_weekly','select=week_start,company,product,ads&week_start=gte.'+wCut+'',
            function(r){return _cpPivotProducts(r,'week_start','ads')})
  ]).then(function(){CP_LOADED=true;CP_LOADING=null});
  return CP_LOADING;
}

// 추이차트와 같은 표 배치 — 행=회사, 열=기간(최신 좌측), 첫 칸은 요약.
// 지출·매출과 달리 광고 수·상품 수는 '스톡' 지표라 기간 합계가 뜻이 없다.
// 그래서 추이차트의 '전체'(기간 합계) 자리에 '최신'(최근 관측값 + 표시 구간 증감)을 둔다.
function _cpDeltaHtml(d, cls) {
  if (d == null || !d) return '';
  // 증가=빨강 / 감소=파랑 (좋고 나쁨이 아니라 방향 표시 — 경쟁사 증가는 '나쁨'이 아니다)
  return '<div class="' + (cls || 'cv') + '" style="color:' + (d > 0 ? '#d00' : '#1a56db') + '">'
       + (d > 0 ? '▲' : '▼') + Math.abs(d).toLocaleString('ko-KR') + '</div>';
}
function _cpCell(cur, prev) {
  if (!cur) return '<td></td>';
  let h = '<div class="r">' + cur.total.toLocaleString('ko-KR') + '</div>';
  if (cur.free != null) h += '<div class="rv">무료 ' + cur.free + '</div>';
  h += _cpDeltaHtml(prev ? cur.total - prev.total : null);
  return '<td class="mc">' + h + '</td>';
}
// 행 하나의 표시 구간 요약 — 최신값 + (가장 오래된 표시열 대비 증감)
function _cpSummaryCell(vals, cols) {
  const cur = vals[cols[0]];
  if (!cur) return '<td class="mc"></td>';
  let base = null;
  for (let i = cols.length - 1; i > 0; i--) { if (vals[cols[i]]) { base = vals[cols[i]]; break } }
  let h = '<div class="r">' + cur.total.toLocaleString('ko-KR') + '</div>';
  if (cur.free != null) h += '<div class="rv">무료 ' + cur.free + '</div>';
  h += _cpDeltaHtml(base ? cur.total - base.total : null);
  return '<td class="mc" style="background:#f5f8ff">' + h + '</td>';
}
function _cpTable(tblId, data, n, errKey, nameLabel) {
  const tbl = document.getElementById(tblId); if (!tbl) return;
  if (!data) {
    tbl.innerHTML = '<tr><td style="padding:12px;color:#888">시트를 읽지 못했습니다 — '
      + (CP_ERR[errKey] || '🔄 시트 새로고침을 눌러보세요') + '</td></tr>';
    return;
  }
  const cols = data.periods.slice(0, n);            // 최신이 왼쪽 — 시트·추이차트와 같은 방향
  if (!cols.length) { tbl.innerHTML = '<tr><td style="padding:12px;color:#888">관측 데이터가 없습니다</td></tr>'; return }
  const rows = data.rows.slice().sort(function (a, b) {
    const la = a.vals[cols[0]], lb = b.vals[cols[0]];
    return (lb ? lb.total : 0) - (la ? la.total : 0);
  });
  // 컬럼별 합계(= 시트의 합계 행을 표시 대상 행만으로 다시 계산)
  const tot = {};
  cols.forEach(function (k) {
    let t = 0, f = 0, hasF = false, any = false;
    rows.forEach(function (r) {
      const v = r.vals[k]; if (!v) return;
      any = true; t += v.total; if (v.free != null) { f += v.free; hasF = true }
    });
    tot[k] = any ? { total: t, free: hasF ? f : null } : null;
  });

  let h = '<thead><tr><th class="hcn" style="text-align:left;white-space:nowrap;min-width:130px">' + nameLabel + '</th>'
        + '<th style="min-width:74px">최신</th>'
        + cols.map(function (k) { return '<th style="min-width:74px">' + _cpLabel(k) + '</th>' }).join('')
        + '</tr></thead><tbody>';
  h += '<tr class="sr"><td class="fx fx0">종합</td>' + _cpSummaryCell(tot, cols)
     + cols.map(function (k, i) { return _cpCell(tot[k], tot[cols[i + 1]]) }).join('') + '</tr>';
  rows.forEach(function (r) {
    const own = /타이트사주/.test(r.name);
    h += '<tr><td class="fx fx0"' + (own ? ' style="font-weight:700;color:#1a73e8"' : '') + '>' + r.name + '</td>'
       + _cpSummaryCell(r.vals, cols)
       + cols.map(function (k, i) { return _cpCell(r.vals[k], r.vals[cols[i + 1]]) }).join('')
       + '</tr>';
  });
  tbl.innerHTML = h + '</tbody>';
}
function renderCompet() {
  if (!document.getElementById('cpAdTbl')) return;
  const granEl = document.getElementById('cpAdGran'), nEl = document.getElementById('cpAdN'),
        pwEl = document.getElementById('cpProdWeeks');
  const gran = granEl ? granEl.value : 'day';
  const adN = parseInt(nEl ? nEl.value : 30) || 30;
  const pw = parseInt(pwEl ? pwEl.value : 13) || 13;
  const u = document.getElementById('cpAdUnit'); if (u) u.textContent = (gran === 'week' ? '주' : '일');

  _cpTable('cpAdTbl', gran === 'week' ? COMPET.adWeek : COMPET.adDay, adN,
           gran === 'week' ? 'adWeek' : 'adDay', '회사');
  const pGran=(document.getElementById('cpProdGran')||{}).value||'day';
  const pu2=document.getElementById('cpProdUnit'); if(pu2)pu2.textContent=(pGran==='week'?'주':'일');
  _cpTable('cpProdTbl', pGran==='week'?COMPET.prodWeek:COMPET.prodDay, pw,
           pGran==='week'?'prodWeek':'prodDay', '사이트');
  const paGran=(document.getElementById('cpPaGran')||{}).value||'day';
  const pu=document.getElementById('cpPaUnit'); if(pu)pu.textContent=(paGran==='week'?'주':'일');
  _cpProdAdsTable(parseInt((document.getElementById('cpPaWeeks')||{}).value)||(paGran==='week'?13:30),
                  paGran==='week'?COMPET.paWeek:COMPET.paDay,
                  paGran==='week'?'paWeek':'paDay');

  const info = document.getElementById('cpInfo');
  if (info) {
    if (!CP_LOADED) { info.textContent = ' · 시트 읽는 중…'; return }
    const ad = gran === 'week' ? COMPET.adWeek : COMPET.adDay;
    const pc = (document.getElementById('cpProdGran')||{}).value === 'week' ? COMPET.prodWeek : COMPET.prodDay;
    const bits = [];
    if (ad && ad.periods.length) bits.push('광고수 최신 ' + _cpLabel(ad.periods[0]) + ' (관측 ' + ad.periods.length + '개)');
    if (pc && pc.periods.length) bits.push('상품수 최신 ' + _cpLabel(pc.periods[0]));
    const errs = Object.keys(CP_ERR);
    if (errs.length) bits.push('⚠ 읽기 실패: ' + errs.join(', '));
    info.textContent = bits.length ? ' · ' + bits.join(' · ') : '';
  }
}
['cpProdWeeks', 'cpAdGran', 'cpAdN', 'cpPaWeeks', 'cpPaGran', 'cpProdGran'].forEach(function (id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('change', function () {
    // 단위를 바꾸면 '최근 N' 선택지도 그 단위에 맞게 갈아끼운다(일 30 / 주 13 기본)
    if (id === 'cpProdGran') {
      const sel = document.getElementById('cpProdWeeks');
      if (sel) {
        const opts = this.value === 'week' ? ['8', '13', '26', '52'] : ['14', '30', '60', '90'];
        sel.innerHTML = opts.map(function (o) { return '<option value="' + o + '">' + o + '</option>' }).join('');
        sel.value = this.value === 'week' ? '13' : '30';
      }
    }
    if (id === 'cpPaGran') {
      const sel = document.getElementById('cpPaWeeks');
      if (sel) {
        const opts = this.value === 'week' ? ['8', '13', '26', '52'] : ['14', '30', '60', '90'];
        sel.innerHTML = opts.map(function (o) { return '<option value="' + o + '">' + o + '</option>' }).join('');
        sel.value = this.value === 'week' ? '13' : '30';
      }
    }
    if (id === 'cpAdGran') {
      const sel = document.getElementById('cpAdN');
      if (sel) {
        const opts = this.value === 'week' ? ['8', '13', '26', '52'] : ['14', '30', '60', '90'];
        sel.innerHTML = opts.map(function (o) { return '<option value="' + o + '">' + o + '</option>' }).join('');
        sel.value = this.value === 'week' ? '13' : '30';
      }
    }
    renderCompet();
  });
});
function _cpProdAdsTable(n, data, errKey) {
  const tbl = document.getElementById('cpPaTbl'); if (!tbl) return;
  if (!data) {
    tbl.innerHTML = '<tr><td style="padding:12px;color:#888">시트를 읽지 못했습니다 — '
      + (CP_ERR[errKey] || '🔄 시트 새로고침을 눌러보세요') + '</td></tr>';
    return;
  }
  if (!data.periods.length) {
    tbl.innerHTML = '<tr><td style="padding:12px;color:#888">아직 관측된 날짜가 없습니다</td></tr>';
    return;
  }
  const cols = data.periods.slice(0, n);
  const groups = data.groups.slice().sort(function (a, b) {
    return ((b.vals && b.vals[cols[0]]) ? b.vals[cols[0]].total : 0)
         - ((a.vals && a.vals[cols[0]]) ? a.vals[cols[0]].total : 0);
  });
  let h = '<thead><tr><th class="hcn" style="text-align:left;white-space:nowrap;min-width:230px">회사 · 상품</th>'
        + '<th style="min-width:74px">최신</th>'
        + cols.map(function (k) { return '<th style="min-width:74px">' + _cpLabel(k) + '</th>' }).join('')
        + '</tr></thead><tbody>';
  groups.forEach(function (g) {
    const own = /타이트사주/.test(g.company);
    h += '<tr class="sr"><td class="fx fx0"' + (own ? ' style="font-weight:700;color:#1a73e8"' : '')
       + ' title="전 기간 누적 광고 ' + ((g.all && g.all.total) || 0).toLocaleString('ko-KR') + '개">'
       + g.company + ' (' + g.items.length + '개 상품)</td>'
       + _cpSummaryCell(g.vals || {}, cols)
       + cols.map(function (k, i) { return _cpCell((g.vals || {})[k], (g.vals || {})[cols[i + 1]]) }).join('')
       + '</tr>';
    g.items.slice().sort(function (a, b) {
      return ((b.vals[cols[0]]) ? b.vals[cols[0]].total : 0) - ((a.vals[cols[0]]) ? a.vals[cols[0]].total : 0);
    }).forEach(function (it) {
      h += '<tr><td class="fx fx0" style="padding-left:18px;color:#444" title="전 기간 누적 광고 '
         + ((it.all && it.all.total) || 0).toLocaleString('ko-KR') + '개">└ ' + it.name + '</td>'
         + _cpSummaryCell(it.vals, cols)
         + cols.map(function (k, i) { return _cpCell(it.vals[k], it.vals[cols[i + 1]]) }).join('')
         + '</tr>';
    });
  });
  tbl.innerHTML = h + '</tbody>';
}

// ===== 시간별 ROAS 화면 (추이차트 셀 클릭 → 화면 전환) =====
// 추이차트 세트 행의 '날짜 셀'을 누르면 화면 전체가 이 화면으로 바뀌고, 그 세트의 그날
// 1시간 단위 ROAS·지출·매출을 그린다. ←(뒤로가기)·ESC·상단 버튼으로 추이차트로 돌아온다.
//
// 데이터: 시간별 그레인은 DB 에 없다(kr_channel_revenue_4h 는 '채널×4시간'이라 세트로 못 쪼갠다).
//   → Edge Function hourly-roas 가 클릭 시점에 Meta insights(hourly 지출) + Mixpanel export
//     (utm_term=세트 귀속 매출)을 직접 읽어 돌려준다. 저장하지 않으므로 아무 과거 날짜나 열린다.
//   기준·괴리 요인은 supabase/functions/hourly-roas/README.md.
const HR_FN=SB_URL+'/functions/v1/hourly-roas';
const HR_MODES={kr:'국내',gl:'글로벌',vn:'밴스드'};      // 세트 단위 모드만 (cr=소재는 제외)
let HR_OPEN=false;      // 화면이 열려 있는가 (뒤로가기가 이 화면만 닫도록 popstate 에서 본다)
let HR_PUSHED=false;    // 열 때 히스토리를 쌓았는가
let HR_CTX=null;        // {mode,id,acc,date,name,camp,day}
let HR_CHART=null;
let HR_SEQ=0;           // 응답 경합 방지 — 마지막 요청만 그린다
const HR_CACHE={};      // 'mode|id|date' → 응답 (같은 셀 재클릭은 즉시)

function hrSupported(){return !!HR_MODES[MODE]}
function hrKey(c){return c.mode+'|'+c.id+'|'+c.date}
function hrMoney(n,ccy){
  if(n==null||!isFinite(n))return'';
  return ccy==='USD'?'$'+Math.round(n).toLocaleString('en-US'):'₩'+Math.round(n).toLocaleString('ko-KR');
}
function _hrToday(){const t=new Date();return t.getFullYear()+'-'+String(t.getMonth()+1).padStart(2,'0')+'-'+String(t.getDate()).padStart(2,'0')}

// 추이차트 셀 클릭 — 위임 리스너 하나로 모든 추이차트(국내·글로벌·밴스드·대만·보조지표)를 받는다.
//   대상 셀에는 renderTrend 가 class="hr-cell" + data-hd="YYYY-MM-DD" 를, 행에는 data-acc 를 심는다.
document.addEventListener('click',function(e){
  const td=e.target&&e.target.closest?e.target.closest('td.hr-cell'):null;
  if(!td)return;
  const tr=td.closest('tr[data-adset-row]');
  if(!tr||!td.dataset.hd)return;
  hrOpen(tr.dataset.adsetRow,tr.dataset.acc||'',td.dataset.hd);
});

document.addEventListener('keydown',function(e){if(HR_OPEN&&e.key==='Escape')hrBack()});

function hrOpen(id,acc,date){
  if(!id||!date)return;
  if(!hrSupported()){alert('시간별 화면은 세트 단위 추이차트(국내·글로벌·밴스드)에서만 지원합니다.');return}
  // 세트 이름·캠페인·그날 일별 셀 값(합계 대조용)을 현재 데이터셋에서 집는다
  let name='',camp='',day=null;
  for(let i=0;i<AD.length;i++){
    const r=AD[i];
    if(r.date===date&&String(rowId(r))===String(id)){
      name=r.adset_name||'';camp=r.campaign_name||'';
      day={spend:+r.spend||0,revenue:+r.revenue||0,roas:+r.roas||0};
      break;
    }
  }
  HR_CTX={mode:MODE,id:String(id),acc:acc||'',date:date,name:name,camp:camp,day:day};
  if(!HR_OPEN){
    HR_OPEN=true;
    document.getElementById('hrView').classList.add('show');
    // 뒤로가기로 이 화면만 닫히게 한다(해시는 그대로 두고 상태만 한 칸 쌓는다)
    try{history.pushState(Object.assign({},navState(),{hr:1}),'',location.hash);HR_PUSHED=true}catch(e){HR_PUSHED=false}
  }
  hrRender(null);
  hrFetch();
}

function hrClose(){
  HR_OPEN=false;HR_PUSHED=false;HR_CTX=null;
  try{if(HR_CHART)HR_CHART.destroy()}catch(e){}
  HR_CHART=null;
  const v=document.getElementById('hrView');if(v)v.classList.remove('show');
}
// 사용자가 닫는 경로 — 히스토리를 쌓았다면 뒤로가기로 되돌려 앞/뒤 이동이 어긋나지 않게 한다
function hrBack(){
  if(HR_PUSHED){history.back();return}   // popstate 가 hrClose 를 부른다
  hrClose();
}
function hrShift(n){
  if(!HR_CTX)return;
  const d=new Date(HR_CTX.date+'T00:00:00');d.setDate(d.getDate()+n);
  const ds=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
  if(ds>_hrToday())return;
  hrOpen(HR_CTX.id,HR_CTX.acc,ds);
}
function hrReload(){
  if(!HR_CTX)return;
  delete HR_CACHE[hrKey(HR_CTX)];
  hrRender(null);hrFetch();
}

async function hrFetch(){
  const c=HR_CTX,seq=++HR_SEQ;
  const cached=HR_CACHE[hrKey(c)];
  if(cached){hrRender(cached);return}
  let out;
  try{
    const r=await fetch(HR_FN,{method:'POST',headers:await abAuthHeaders(),
      body:JSON.stringify({mode:c.mode,adset_id:c.id,ad_account_id:c.acc,date:c.date})});
    const j=await r.json().catch(function(){return{}});
    if(!r.ok||j.ok===false){
      // 404 = 함수 미배포. 원인이 바로 보이도록 배포 명령을 그대로 띄운다.
      out={error:(r.status===404?'hourly-roas Edge Function 이 배포되지 않았습니다.':(j.error||('서버 오류 ('+r.status+')'))),status:r.status};
    }else{
      out=j;HR_CACHE[hrKey(c)]=j;
    }
  }catch(e){
    out={error:String(e&&e.message||e)};
  }
  if(seq!==HR_SEQ)return;      // 그 사이 다른 셀을 눌렀다
  hrRender(out);
}

// data=null → 로딩 / data.error → 오류 / 그 외 → 차트+표
function hrRender(data){
  const c=HR_CTX;if(!c)return;
  const ttl=document.getElementById('hrTtl'),sub=document.getElementById('hrSub'),body=document.getElementById('hrBody');
  ttl.textContent='⏱ 시간별 ROAS · '+(c.name||('세트 '+c.id));
  sub.textContent=[HR_MODES[c.mode]||c.mode,c.camp,c.date+'('+WD(c.date)+')','세트 '+c.id].filter(Boolean).join('  ·  ');
  const nx=document.getElementById('hrNext');if(nx)nx.disabled=(c.date>=_hrToday());

  try{if(HR_CHART)HR_CHART.destroy()}catch(e){}
  HR_CHART=null;

  if(!data){
    body.innerHTML='<div class="hr-msg">⏳ Meta 시간별 지출 + Mixpanel 결제를 조회하는 중입니다…<br>'
      +'<span style="font-size:10px;color:#999">저장된 표가 아니라 원천을 직접 읽습니다 — 처음 여는 날짜는 15~30초 걸립니다(그 날짜의 결제 전량을 받아 걸러야 해서). 같은 날짜의 다른 세트는 훨씬 빠릅니다.</span></div>';
    return;
  }
  if(data.error){
    body.innerHTML='<div class="hr-msg"><div class="hr-err">'+_mEsc(data.error)+'</div>'
      +(data.status===404
        ?'<div style="margin-top:10px;font-size:11px;line-height:1.9">배포 후 다시 시도해 주세요.<br>'
          +'<code>cd newTightauto &amp;&amp; supabase functions deploy hourly-roas --project-ref grtglwavqhvlqcocahao</code><br>'
          +'Mixpanel 시크릿도 필요합니다 — <code>supabase secrets set MIXPANEL_USERNAME=… MIXPANEL_SECRET=… MIXPANEL_PROJECT_ID=3390233</code><br>'
          +'<span style="color:#999">자세한 절차: supabase/functions/hourly-roas/README.md</span></div>'
        :'<div style="margin-top:10px"><button class="hr-back" style="border-color:#ccc;color:#333" onclick="hrReload()">↻ 다시 시도</button></div>')
      +'</div>';
    return;
  }

  const ccy=data.currency||'KRW';
  const hs=data.hours||[];
  const tot=data.totals||{spend:0,revenue:0,purchases:0};
  const roas=tot.spend>0?tot.revenue/tot.spend*100:0;
  // 일별 셀과의 괴리 — 크로스셀 백필·환율·(글로벌)메타치환 때문에 완전히 같지는 않다
  const dd=c.day;
  const diff=function(a,b){return b>0?((a-b)/b*100):null};
  const dS=dd?diff(tot.spend,dd.spend):null,dR=dd?diff(tot.revenue,dd.revenue):null;
  const dTxt=function(v){return v==null?'':(v>=0?'+':'')+v.toFixed(1)+'%'};

  // 시간별 ROAS + 누적 ROAS(하루가 흐르며 어디서 수익이 붙었는지)
  const labels=hs.map(function(x){return String(x.h).padStart(2,'0')+'시'});
  const spend=hs.map(function(x){return +(+x.spend||0).toFixed(2)});
  const rev=hs.map(function(x){return +(+x.revenue||0).toFixed(2)});
  const hRoas=hs.map(function(x){return x.spend>0?+(x.revenue/x.spend*100).toFixed(1):null});
  let _cs=0,_cr=0;
  const cRoas=hs.map(function(x){_cs+=+x.spend||0;_cr+=+x.revenue||0;return _cs>0?+(_cr/_cs*100).toFixed(1):null});

  body.innerHTML=''
    +'<div class="hr-kpi">'
    +  '<div class="hr-k"><span>하루 ROAS</span><b style="color:'+(roas>=100?'#0a7d32':'#c00')+'">'+roas.toFixed(0)+'%</b>'
    +    '<div class="hr-d">'+(dd?'일별 셀 '+dd.roas.toFixed(0)+'%':'')+'</div></div>'
    +  '<div class="hr-k"><span>지출</span><b>'+hrMoney(tot.spend,ccy)+'</b><div class="hr-d">'+(dS==null?'':'일별 대비 '+dTxt(dS))+'</div></div>'
    +  '<div class="hr-k"><span>매출</span><b>'+hrMoney(tot.revenue,ccy)+'</b><div class="hr-d">'+(dR==null?'':'일별 대비 '+dTxt(dR))+'</div></div>'
    +  '<div class="hr-k"><span>구매</span><b>'+(tot.purchases||0)+'건</b><div class="hr-d">'+(tot.purchases?hrMoney(tot.spend/tot.purchases,ccy)+' /건':'')+'</div></div>'
    +  '<div class="hr-k"><span>노출 · 클릭</span><b style="font-size:12px">'+F(tot.impressions)+' · '+F(tot.clicks)+'</b>'
    +    '<div class="hr-d">'+(tot.impressions>0?'CTR '+P(tot.clicks/tot.impressions*100):'')+'</div></div>'
    +'</div>'
    +'<div class="hr-card"><h4>시간별 ROAS(선) · 지출·매출(막대) — KST</h4><div class="hr-chart"><canvas id="hrChart"></canvas></div></div>'
    +'<div class="hr-card"><h4>시간별 값</h4><div style="overflow-x:auto">'+_hrTable(hs,ccy)+'</div></div>'
    +'<div class="hr-card"><h4>기준</h4><div class="hr-notes">'
    +  '지출=Meta insights 시간대 브레이크다운(광고주 타임존=KST) · 매출=Mixpanel 결제완료 중 utm_term=이 세트 귀속(order_id dedup)<br>'
    +  (data.notes||[]).map(function(n){return /실패|없습니다/.test(n)?'<span class="hr-warn">⚠ '+_mEsc(n)+'</span>':'· '+_mEsc(n)}).join('<br>')
    +  ((COUNTRY&&COUNTRY!=='ALL')?'<br><span class="hr-warn">⚠ 국가 필터('+COUNTRY+') 적용 중 — 이 화면은 세트 전체(국가 합산) 기준이라 일별 셀보다 큽니다.</span>':'')
    +'</div></div>';

  if(typeof Chart==='undefined')return;
  const cv=document.getElementById('hrChart');if(!cv)return;
  HR_CHART=new Chart(cv.getContext('2d'),{
    data:{labels:labels,datasets:[
      {type:'bar',label:'지출',data:spend,backgroundColor:'rgba(120,130,145,.45)',order:3,yAxisID:'y'},
      {type:'bar',label:'매출',data:rev,backgroundColor:'rgba(10,125,50,.5)',order:3,yAxisID:'y'},
      {type:'line',label:'시간별 ROAS',data:hRoas,borderColor:'#d00',backgroundColor:'#d00',borderWidth:2,pointRadius:2.5,spanGaps:true,tension:.25,order:1,yAxisID:'y1'},
      {type:'line',label:'누적 ROAS',data:cRoas,borderColor:'#e8912d',borderDash:[5,4],borderWidth:2,pointRadius:0,spanGaps:true,order:2,yAxisID:'y1'}
    ]},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
      plugins:{legend:{labels:{boxWidth:12,font:{size:10}}},
        tooltip:{callbacks:{label:function(x){
          const v=x.parsed.y;
          if(v==null)return x.dataset.label+': —';
          return x.dataset.label+': '+(x.dataset.yAxisID==='y1'?v.toFixed(0)+'%':hrMoney(v,ccy));
        }}}},
      scales:{
        y:{position:'left',beginAtZero:true,ticks:{font:{size:9},callback:function(v){return hrMoney(v,ccy)}},title:{display:true,text:'금액('+ccy+')',font:{size:9}}},
        y1:{position:'right',beginAtZero:true,grid:{drawOnChartArea:false},ticks:{font:{size:9},callback:function(v){return v+'%'}},title:{display:true,text:'ROAS',font:{size:9}}},
        x:{ticks:{font:{size:9},maxRotation:0,autoSkip:false}}
      }}
  });
}

function _hrTable(hs,ccy){
  let h='<table class="hr-tbl"><thead><tr><th>시각(KST)</th><th>지출</th><th>매출</th><th>ROAS</th><th>누적 ROAS</th><th>구매</th><th>노출</th><th>클릭</th></tr></thead><tbody>';
  let cs=0,cr=0;
  hs.forEach(function(x){
    const s=+x.spend||0,r=+x.revenue||0;cs+=s;cr+=r;
    const ro=s>0?r/s*100:null,cro=cs>0?cr/cs*100:null;
    h+='<tr class="'+(s||r?'':'hr-zero')+'"><td class="hr-h">'+String(x.h).padStart(2,'0')+':00</td>'
      +'<td>'+(s?hrMoney(s,ccy):'-')+'</td><td>'+(r?hrMoney(r,ccy):'-')+'</td>'
      +'<td'+(ro!=null?' style="font-weight:600;color:'+(ro>=100?'#0a7d32':'#c00')+'"':'')+'>'+(ro!=null?ro.toFixed(0)+'%':'-')+'</td>'
      +'<td style="color:#888">'+(cro!=null?cro.toFixed(0)+'%':'-')+'</td>'
      +'<td>'+(x.purchases||'-')+'</td><td>'+(x.impressions?F(x.impressions):'-')+'</td><td>'+(x.clicks?F(x.clicks):'-')+'</td></tr>';
  });
  const ro=cs>0?cr/cs*100:0;
  const sum=function(k){return hs.reduce(function(a,x){return a+(+x[k]||0)},0)};
  h+='</tbody><tfoot><tr><td class="hr-h">합계</td><td>'+hrMoney(cs,ccy)+'</td><td>'+hrMoney(cr,ccy)+'</td><td>'+ro.toFixed(0)+'%</td><td></td>'
    +'<td>'+sum('purchases')+'</td><td>'+F(sum('impressions'))+'</td><td>'+F(sum('clicks'))+'</td></tr></tfoot></table>';
  return h;
}
