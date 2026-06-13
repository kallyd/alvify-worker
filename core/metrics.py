"""
Lightweight per-job metrics for the Alvify worker.

Tracks throughput, cache hit rates, errors and timing without any
external dependency (no Prometheus, no StatsD — just structured logs).

Usage::

    from core.metrics import JobMetrics

    m = JobMetrics(job_id="abc-123")
    m.start()

    # Inside progress_cb:
    m.lead_scraped()
    m.lead_sent()
    m.cache_hit()       # place cache hit
    m.record_error()

    # Periodic or final report:
    m.log_summary()     # logs a structured line at INFO level
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class JobMetrics:
    """Per-job counters. Create one instance per job in _process_job."""

    job_id: str

    # Timing
    _start_ts: float = field(default_factory=time.monotonic, repr=False)
    _last_report_ts: float = field(default_factory=time.monotonic, repr=False)
    _last_report_leads: int = field(default=0, repr=False)

    # Counters
    leads_scraped: int = 0     # extracted by the scraper
    leads_sent: int = 0        # successfully submitted to backend
    leads_new: int = 0         # flagged as new by backend
    leads_deduped: int = 0     # dropped by local dedup
    cache_hits: int = 0        # place cache hits (scraping skipped)
    errors: int = 0            # submission/scraping errors
    retries: int = 0           # HTTP retries

    # Batch stats
    batches_sent: int = 0
    total_batch_ms: int = 0

    # Enrichment stats
    leads_enriched: int = 0          # leads that had a website and were enriched
    emails_found: int = 0
    socials_found: int = 0
    technologies_found: int = 0
    enrich_cache_hits: int = 0
    enrich_cache_misses: int = 0
    total_enrich_ms: int = 0

    # Phase timing
    phase1_ms: int = 0               # time spent scrolling the feed
    phase2_ms: int = 0               # time spent extracting place details
    nav_times_ms: list = field(default_factory=list, repr=False)  # per-place navigation times

    def start(self) -> None:
        """Reset timing. Call once at job start."""
        self._start_ts = time.monotonic()
        self._last_report_ts = time.monotonic()

    # ── Counters ──────────────────────────────────────────────────────────────

    def lead_scraped(self) -> None:
        self.leads_scraped += 1

    def lead_sent(self, count: int = 1) -> None:
        self.leads_sent += count

    def lead_new(self, count: int = 1) -> None:
        self.leads_new += count

    def lead_deduped(self) -> None:
        self.leads_deduped += 1

    def cache_hit(self) -> None:
        self.cache_hits += 1

    def record_error(self) -> None:
        self.errors += 1

    def record_retry(self) -> None:
        self.retries += 1

    def batch_sent(self, size: int, elapsed_ms: int) -> None:
        self.batches_sent += 1
        self.total_batch_ms += elapsed_ms

    def lead_enriched(
        self,
        emails: int = 0,
        socials: int = 0,
        technologies: int = 0,
        elapsed_ms: int = 0,
        cache_hit: bool = False,
    ) -> None:
        self.leads_enriched += 1
        self.emails_found += emails
        self.socials_found += socials
        self.technologies_found += technologies
        self.total_enrich_ms += elapsed_ms
        if cache_hit:
            self.enrich_cache_hits += 1
        else:
            self.enrich_cache_misses += 1

    def record_phase1(self, elapsed_ms: int) -> None:
        self.phase1_ms = elapsed_ms

    def record_phase2(self, elapsed_ms: int) -> None:
        self.phase2_ms = elapsed_ms

    def record_nav(self, elapsed_ms: int) -> None:
        self.nav_times_ms.append(elapsed_ms)

    # ── Computed properties ───────────────────────────────────────────────────

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self._start_ts

    @property
    def leads_per_min(self) -> float:
        elapsed = self.elapsed_s
        return (self.leads_sent / elapsed * 60) if elapsed > 0 else 0.0

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.leads_scraped
        return (self.cache_hits / total) if total > 0 else 0.0

    @property
    def avg_batch_ms(self) -> float:
        return (self.total_batch_ms / self.batches_sent) if self.batches_sent > 0 else 0.0

    @property
    def avg_nav_ms(self) -> float:
        return (sum(self.nav_times_ms) / len(self.nav_times_ms)) if self.nav_times_ms else 0.0

    # ── Reporting ─────────────────────────────────────────────────────────────

    def log_summary(self, final: bool = False) -> None:
        """
        Emit a single structured INFO log line with all metrics.
        Call periodically and at the end of the job.
        """
        now = time.monotonic()
        # Instantaneous leads/min since last report
        dt = now - self._last_report_ts
        dn = self.leads_sent - self._last_report_leads
        instant_lpm = (dn / dt * 60) if dt > 0 else 0.0
        self._last_report_ts = now
        self._last_report_leads = self.leads_sent

        label = "FINAL" if final else "progress"
        logger.info(
            "metrics [%s] job=%s "
            "scraped=%d sent=%d new=%d deduped=%d "
            "cache_hits=%d cache_hit_rate=%.0f%% "
            "errors=%d retries=%d "
            "batches=%d avg_batch_ms=%.0f "
            "phase1_ms=%d phase2_ms=%d avg_nav_ms=%.0f navs=%d "
            "enriched=%d emails=%d socials=%d technologies=%d "
            "enrich_cache_hits=%d enrich_cache_misses=%d "
            "elapsed=%.1fs leads_per_min=%.1f instant_lpm=%.1f",
            label, self.job_id,
            self.leads_scraped, self.leads_sent, self.leads_new, self.leads_deduped,
            self.cache_hits, self.cache_hit_rate * 100,
            self.errors, self.retries,
            self.batches_sent, self.avg_batch_ms,
            self.phase1_ms, self.phase2_ms, self.avg_nav_ms, len(self.nav_times_ms),
            self.leads_enriched, self.emails_found, self.socials_found, self.technologies_found,
            self.enrich_cache_hits, self.enrich_cache_misses,
            self.elapsed_s, self.leads_per_min, instant_lpm,
        )

    def as_dict(self) -> dict:
        """Serialisable snapshot — useful for heartbeat payloads."""
        return {
            "job_id": self.job_id,
            "leads_scraped": self.leads_scraped,
            "leads_sent": self.leads_sent,
            "leads_new": self.leads_new,
            "leads_deduped": self.leads_deduped,
            "cache_hits": self.cache_hits,
            "errors": self.errors,
            "elapsed_s": round(self.elapsed_s, 1),
            "leads_per_min": round(self.leads_per_min, 1),
            # phase timing
            "phase1_ms": self.phase1_ms,
            "phase2_ms": self.phase2_ms,
            "avg_nav_ms": round(self.avg_nav_ms, 0),
            "cache_hit_rate": round(self.cache_hit_rate * 100, 1),
            # enrichment
            "leads_enriched": self.leads_enriched,
            "emails_found": self.emails_found,
            "socials_found": self.socials_found,
            "technologies_found": self.technologies_found,
            "enrich_cache_hits": self.enrich_cache_hits,
            "enrich_cache_misses": self.enrich_cache_misses,
        }
