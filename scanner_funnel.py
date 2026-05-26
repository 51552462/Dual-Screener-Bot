"""
스캐너 퍼널 SSOT — 단계별 탈락·최종 생존·파이프라인 등재 추적 (KR/US 공통).
Stage Profile 주입으로 스캐너별 퍼널 단계를 OCP 방식으로 확장.
"""
from __future__ import annotations

import html
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import pytz

# --- Stage profiles (drop key → human label) ---

SUPERNOVA_LIVE_FUNNEL: Tuple[Tuple[str, str], ...] = (
    ("SKIP_POSITION", "보유·당일스캔 제외"),
    ("DATA_FAIL", "데이터 수신·20봉"),
    ("LIQUIDITY", "유동성(가격·거래량)"),
    ("TOXIC_ML_TREE", "ML 독성트리"),
    ("DNA_FAIL", "DNA 미통과"),
    ("ANTI_TOXIC", "오답노트 독성"),
    ("DOOMSDAY_HALT", "둠스데이 전면차단"),
)

KR_BOWL_LIVE_FUNNEL: Tuple[Tuple[str, str], ...] = (
    ("STATIC_QUOTE", "거래정지·단일가"),
    ("DATA_FAIL", "데이터·500봉 미만"),
    ("BOWL_FAIL", "밥그릇 시그널 미통과"),
    ("SKIP_DUPLICATE", "당일 중복 발송"),
    ("CHART_FAIL", "차트 생성 실패"),
    ("COMPUTE_ERROR", "연산 예외"),
)

US_BOWL_LIVE_FUNNEL: Tuple[Tuple[str, str], ...] = (
    ("STATIC_QUOTE", "거래정지·단일가"),
    ("DATA_FAIL", "데이터·500봉 미만"),
    ("BOWL_FAIL", "밥그릇 시그널 미통과"),
    ("SKIP_DUPLICATE", "당일 중복 발송"),
    ("CHART_FAIL", "차트 생성 실패"),
    ("COMPUTE_ERROR", "연산 예외"),
)

FUNNEL_STAGE_PROFILES: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "SUPERNOVA": SUPERNOVA_LIVE_FUNNEL,
    "KR_BOWL": KR_BOWL_LIVE_FUNNEL,
    "US_BOWL": US_BOWL_LIVE_FUNNEL,
}

SCANNER_DISPLAY_TITLES: Dict[str, str] = {
    "SUPERNOVA": "초신성 스캔",
    "KR_BOWL": "한국장 밥그릇(4번)",
    "US_BOWL": "미국장 밥그릇(3번)",
}

# KR 밥그릇: ENROLLED = forward_trades 아님 → 텔레그램 관종 발송 완료
_PIPELINE_STATUS_LABEL_KR_BOWL: Dict[str, str] = {
    "ENROLLED_SHADOW": "텔레그램+가상장부 관측 등재",
    "ENROLLED": "텔레그램만(장부 미등재)",
    "CHART_FAIL": "차트 생성 실패",
    "PENDING": "미처리",
    "SKIP_DUPLICATE": "당일 중복 스킵",
}


def _pipeline_status_display(profile_id: str, status: str) -> str:
    key = str(status or "").strip().upper()
    if profile_id == "KR_BOWL":
        return _PIPELINE_STATUS_LABEL_KR_BOWL.get(key, status)
    return status


def get_funnel_stages(profile_id: str) -> Tuple[Tuple[str, str], ...]:
    key = str(profile_id).upper()
    if key not in FUNNEL_STAGE_PROFILES:
        raise KeyError(
            f"Unknown funnel profile {profile_id!r}; "
            f"known: {', '.join(sorted(FUNNEL_STAGE_PROFILES))}"
        )
    return FUNNEL_STAGE_PROFILES[key]


def resolve_funnel_stages(
    *,
    profile: Optional[str] = None,
    funnel_stages: Optional[Sequence[Tuple[str, str]]] = None,
) -> Tuple[Tuple[str, str], ...]:
    if funnel_stages is not None:
        return tuple(funnel_stages)
    if profile is not None:
        return get_funnel_stages(profile)
    return SUPERNOVA_LIVE_FUNNEL


@dataclass(frozen=True)
class ScanSurvivor:
    code: str
    name: str
    pass_path: str
    final_sig: str
    final_score: float
    pipeline_status: str = "PENDING"


@dataclass(frozen=True)
class FunnelStep:
    key: str
    label: str
    count_surviving: int


@dataclass(frozen=True)
class ScanFunnelReport:
    scanner_id: str
    market: str
    as_of_kst: str
    universe: int
    steps: Tuple[FunnelStep, ...]
    drop_summary: Tuple[Tuple[str, int], ...]
    survivors_final: Tuple[ScanSurvivor, ...]
    top_n_display: Tuple[ScanSurvivor, ...]
    enrolled: Tuple[ScanSurvivor, ...]
    fetch_failed: int
    us_preloaded: Optional[int]
    pipeline_line: str
    profile_id: str = "SUPERNOVA"
    elapsed_min: Optional[float] = None
    db_error_samples: Tuple[str, ...] = ()


class ScanFunnelTracker:
    """Thread-safe funnel + final survivors + pipeline outcomes."""

    def __init__(
        self,
        *,
        scanner_id: str,
        market: str,
        universe_size: int,
        profile: Optional[str] = None,
        funnel_stages: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        self.scanner_id = str(scanner_id)
        self.market = str(market).upper()
        self.universe = int(universe_size)
        self.profile_id = (
            str(profile).upper()
            if profile is not None
            else str(scanner_id).upper()
        )
        self._funnel_stages = resolve_funnel_stages(
            profile=profile or self.profile_id,
            funnel_stages=funnel_stages,
        )
        self._lock = threading.Lock()
        self._drops: Counter = Counter()
        self._fetch_failed = 0
        self._us_preloaded: Optional[int] = None
        self._final: Dict[str, ScanSurvivor] = {}
        self._pipeline: Dict[str, str] = {}
        self._db_fail_samples: List[str] = []
        self._db_fail_reasons: Counter = Counter()

    def drop(self, reason: str, n: int = 1) -> None:
        if not reason or n <= 0:
            return
        with self._lock:
            self._drops[str(reason)] += int(n)

    def add_fetch_failed(self, n: int = 1) -> None:
        with self._lock:
            self._fetch_failed += int(n)

    def set_us_preloaded(self, count: int, universe: int) -> None:
        with self._lock:
            self._us_preloaded = int(count)
            self.universe = int(universe)

    def add_final_candidate(
        self,
        *,
        code: str,
        name: str,
        pass_path: str,
        final_sig: str,
        final_score: float,
    ) -> None:
        with self._lock:
            self._final[str(code)] = ScanSurvivor(
                code=str(code),
                name=str(name),
                pass_path=str(pass_path),
                final_sig=str(final_sig),
                final_score=float(final_score),
                pipeline_status="PENDING",
            )

    def set_pipeline_result(self, code: str, status: str) -> None:
        with self._lock:
            c = str(code)
            if c in self._final:
                prev = self._final[c]
                self._final[c] = ScanSurvivor(
                    code=prev.code,
                    name=prev.name,
                    pass_path=prev.pass_path,
                    final_sig=prev.final_sig,
                    final_score=prev.final_score,
                    pipeline_status=str(status),
                )
            self._pipeline[c] = str(status)

    def record_db_failure(self, reason: str) -> None:
        """forward_trades 등재 실패 사유 샘플 (Telegram·로그용, 최대 5종)."""
        key = str(reason or "UNKNOWN").strip()[:240]
        if not key:
            key = "UNKNOWN"
        with self._lock:
            self._db_fail_reasons[key] += 1
            if key not in self._db_fail_samples and len(self._db_fail_samples) < 5:
                self._db_fail_samples.append(key)

    def get_final_candidates(self) -> List[ScanSurvivor]:
        with self._lock:
            return list(self._final.values())

    def _build_pipeline_line(self, finals: List[ScanSurvivor]) -> str:
        n_enrolled = sum(1 for s in finals if s.pipeline_status == "ENROLLED")
        n_toxic_skip = sum(1 for s in finals if s.pipeline_status == "SKIPPED_TOXIC")
        n_doom = sum(1 for s in finals if s.pipeline_status == "SKIPPED_DOOMSDAY")
        n_fail = sum(1 for s in finals if s.pipeline_status == "FAILED_DB")
        n_pending = sum(1 for s in finals if s.pipeline_status == "PENDING")
        n_chart = sum(1 for s in finals if s.pipeline_status == "CHART_FAIL")

        pid = self.profile_id
        if pid == "KR_BOWL":
            n_shadow = sum(1 for s in finals if s.pipeline_status == "ENROLLED_SHADOW")
            n_tg_only = sum(1 for s in finals if s.pipeline_status == "ENROLLED")
            n_tg_total = n_shadow + n_tg_only
            line = (
                f"텔레그램 <b>{n_tg_total}</b>건 · "
                f"가상장부 관측 <b>{n_shadow}</b>건 "
                f"<i>(실매수 없음·Meta/Deathmatch 평가용)</i>"
            )
            if n_tg_only:
                line += f" · 장부 스킵 <b>{n_tg_only}</b>"
            if n_chart:
                line += f" · 차트 실패 <b>{n_chart}</b>"
            if n_pending:
                line += f" · 미처리 <b>{n_pending}</b>"
            return line
        if pid == "US_BOWL":
            n_tg = sum(
                1
                for s in finals
                if s.pipeline_status in ("ENROLLED", "FAILED_DB", "TELEGRAM_QUEUED")
            )
            line = f"텔레그램 <b>{n_tg}</b> · forward 등재 <b>{n_enrolled}</b>"
            if n_fail:
                line += f" · forward 실패 <b>{n_fail}</b>"
            if n_chart:
                line += f" · 차트 실패 <b>{n_chart}</b>"
            if n_pending:
                line += f" · 미처리 <b>{n_pending}</b>"
            return line

        line = f"forward_trades(가상매매) 등재 <b>{n_enrolled}</b>건"
        if n_toxic_skip:
            line += f" · 독성패턴 스킵 <b>{n_toxic_skip}</b>"
        if n_doom:
            line += f" · 둠스데이 스킵 <b>{n_doom}</b>"
        if n_fail:
            line += f" · DB등재 실패 <b>{n_fail}</b>"
        if n_pending:
            line += f" · 미처리 <b>{n_pending}</b>"
        if n_enrolled > 0:
            line += " · 오토파일럿 OPEN 감시 경로로 편입"
        return line

    def _build_pipeline_line_with_db_errors(
        self, finals: List[ScanSurvivor], db_samples: Tuple[str, ...]
    ) -> str:
        line = self._build_pipeline_line(finals)
        if db_samples:
            top = html.escape(db_samples[0][:160], quote=False)
            line += f"\n⚠️ <b>DB거절 샘플:</b> <code>{top}</code>"
            if len(db_samples) > 1:
                line += f" <i>(+{len(db_samples) - 1}종 유형)</i>"
        return line

    def finalize(self, *, elapsed_min: Optional[float] = None) -> ScanFunnelReport:
        with self._lock:
            drops = Counter(self._drops)
            fetch_failed = self._fetch_failed
            us_pre = self._us_preloaded
            finals = list(self._final.values())
            universe = self.universe
            profile_id = self.profile_id
            db_samples = tuple(self._db_fail_samples)

        n_final = len(finals)
        steps: List[FunnelStep] = [
            FunnelStep(key="UNIVERSE", label="전체 유니버스", count_surviving=universe)
        ]
        surviving = universe
        for key, label in self._funnel_stages:
            dropped = int(drops.get(key, 0))
            surviving = max(0, surviving - dropped)
            steps.append(
                FunnelStep(key=key, label=label, count_surviving=surviving)
            )
        steps.append(
            FunnelStep(
                key="FINAL_PASS",
                label="최종 시그널 합격",
                count_surviving=n_final,
            )
        )

        drop_summary = tuple(
            (k, int(v))
            for k, v in sorted(drops.items(), key=lambda x: -x[1])
            if v > 0
        )

        sorted_finals = sorted(finals, key=lambda s: s.final_score, reverse=True)
        top3 = tuple(sorted_finals[:3])
        enrolled = tuple(
            s
            for s in sorted_finals
            if s.pipeline_status in ("ENROLLED", "ENROLLED_SHADOW")
        )

        tz_kr = pytz.timezone("Asia/Seoul")
        as_of = datetime.now(tz_kr).strftime("%Y-%m-%d %H:%M")

        return ScanFunnelReport(
            scanner_id=self.scanner_id,
            market=self.market,
            as_of_kst=as_of,
            universe=universe,
            steps=tuple(steps),
            drop_summary=drop_summary,
            survivors_final=tuple(sorted_finals),
            top_n_display=top3,
            enrolled=enrolled,
            fetch_failed=fetch_failed,
            us_preloaded=us_pre,
            pipeline_line=self._build_pipeline_line_with_db_errors(
                sorted_finals, db_samples
            ),
            profile_id=profile_id,
            elapsed_min=elapsed_min,
            db_error_samples=db_samples,
        )


def format_scan_funnel_report(report: ScanFunnelReport) -> str:
    """퍼널 생존 체인 + Top3 + 파이프라인 (Telegram HTML)."""
    m = html.escape(report.market, quote=False)
    icon = "🇰🇷" if report.market == "KR" else "🇺🇸"
    title = SCANNER_DISPLAY_TITLES.get(
        report.profile_id,
        SCANNER_DISPLAY_TITLES.get(report.scanner_id, report.scanner_id),
    )
    title_esc = html.escape(title, quote=False)

    out = f"{icon} <b>[{m} {title_esc} · 퍼널]</b>\n"
    out += f"📅 KST <code>{html.escape(report.as_of_kst, quote=False)}</code>\n"
    if report.elapsed_min is not None:
        out += f"⏱ 소요 <b>{report.elapsed_min:.1f}</b>분\n"

    if report.us_preloaded is not None and report.market == "US":
        out += (
            f"📡 US OHLCV 선로드: <b>{report.us_preloaded}</b> / {report.universe} "
            f"(DATA_FAIL에 미포함)\n"
        )

    parts: List[str] = []
    for i, step in enumerate(report.steps):
        lbl = html.escape(step.label, quote=False)
        if i == 0:
            parts.append(f"<b>{step.count_surviving}</b>")
        else:
            parts.append(f"[{lbl}] <b>{step.count_surviving}</b>")
    out += "🔻 <b>퍼널:</b> " + " ➔ ".join(parts) + "\n"

    if report.drop_summary:
        drop_bits = [
            f"{html.escape(k, quote=False)} {v}"
            for k, v in report.drop_summary[:8]
        ]
        out += f"📉 <b>탈락:</b> " + " · ".join(drop_bits)
        if report.fetch_failed:
            out += f" · fetch실패 <b>{report.fetch_failed}</b>"
        out += "\n"
    elif report.fetch_failed:
        out += f"📉 데이터 수신 실패(누적): <b>{report.fetch_failed}</b>건\n"

    if report.top_n_display:
        out += "🏆 <b>Top 3 합격:</b>\n"
        for s in report.top_n_display:
            nm = html.escape(s.name, quote=False)
            cd = html.escape(s.code, quote=False)
            path = html.escape(s.pass_path, quote=False)
            sig = html.escape(s.final_sig[:48], quote=False)
            score_lbl = "신뢰" if report.profile_id in ("KR_BOWL", "US_BOWL") else "sim"
            pstat = _pipeline_status_display(report.profile_id, s.pipeline_status)
            out += (
                f" · <b>{nm}</b>({cd}) {path} · {score_lbl} <b>{s.final_score:.1f}</b> · "
                f"<i>{sig}</i> · <code>{html.escape(pstat, quote=False)}</code>\n"
            )
    else:
        out += "🏆 <b>Top 3:</b> <i>최종 합격 없음</i>\n"

    out += f"🛰️ <b>파이프라인:</b> {report.pipeline_line}\n"
    if report.profile_id == "KR_BOWL":
        out += (
            "📖 <b>용어:</b> "
            "<code>B (J 강조)</code>=6단 EMA 정배열+밥그릇 돌파 · "
            "<code>B (일반)</code>=돌파만 · "
            "<code>텔레그램+가상장부 관측</code>=유료방 발송+forward_trades 관측행(자본 0·청산 추적)\n"
        )
    if report.db_error_samples and "DB거절 샘플" not in report.pipeline_line:
        for i, sample in enumerate(report.db_error_samples[:3]):
            out += (
                f"⚠️ <b>DB거절[{i + 1}]:</b> "
                f"<code>{html.escape(sample[:200], quote=False)}</code>\n"
            )
    return out


def format_supernova_scan_report(report: ScanFunnelReport) -> str:
    """초신성 라이브 스캔 — format_scan_funnel_report 별칭."""
    return format_scan_funnel_report(report)
