import hashlib
import os
import re
import sys
import threading
import time
import gc
import logging
import json
from collections import defaultdict
from dataclasses import dataclass, field
from queue import PriorityQueue, Empty
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from urllib.robotparser import RobotFileParser
from io import BytesIO
from datetime import datetime
from contextlib import asynccontextmanager

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# =============================================================================
# CONFIGURATION - EDIT THESE
# =============================================================================
SEED_URLS = [
    # ============================
    # NATIONAL / U.S. NEWS
    # ============================
    "https://www.apnews.com/",
    "https://www.reuters.com/",
    "https://www.usatoday.com/",
    "https://www.cnn.com/us",
    "https://www.nbcnews.com/us-news",
    "https://abcnews.go.com/US",
    "https://www.foxnews.com/us",
    "https://www.npr.org/sections/national/",
    "https://www.politico.com/",
    "https://thehill.com/",
    "https://www.bloomberg.com/",
    "https://www.wsj.com/news/us",
    "https://www.washingtonpost.com/",
    "https://www.nytimes.com/section/us",

    # ============================
    # U.S. GOVERNMENT & PUBLIC DATA
    # ============================
    "https://www.usa.gov/",
    "https://www.data.gov/",
    "https://www.census.gov/",
    "https://www.whitehouse.gov/",
    "https://www.congress.gov/",
    "https://www.supremecourt.gov/",
    "https://www.irs.gov/",
    "https://www.fbi.gov/",
    "https://www.dhs.gov/",
    "https://www.justice.gov/",
    "https://www.loc.gov/",
    "https://www.state.gov/",
    "https://www.dot.gov/",
    "https://www.epa.gov/",
    "https://www.noaa.gov/",

    # ============================
    # LAW, COURTS, LEGISLATION
    # ============================
    "https://www.supremecourt.gov/",
    "https://www.ca1.uscourts.gov/",
    "https://www.ca9.uscourts.gov/",
    "https://www.law.cornell.edu/",
    "https://www.govinfo.gov/",
    "https://www.federalregister.gov/",

    # ============================
    # ECONOMY, BUSINESS, FINANCE
    # ============================
    "https://www.sec.gov/edgar.shtml",
    "https://fred.stlouisfed.org/",
    "https://www.bls.gov/",
    "https://www.bea.gov/",
    "https://www.federalreserve.gov/",
    "https://www.sba.gov/",
    "https://www.nasdaq.com/",
    "https://www.nyse.com/",

    # ============================
    # HEALTH, SCIENCE, RESEARCH
    # ============================
    "https://www.cdc.gov/",
    "https://www.nih.gov/",
    "https://www.science.gov/",
    "https://www.nasa.gov/",
    "https://www.nsf.gov/",
    "https://www.hhs.gov/",
    "https://www.mayoclinic.org/",
    "https://www.clevelandclinic.org/",

    # ============================
    # EDUCATION / UNIVERSITIES
    # (high-authority U.S. domains)
    # ============================
    "https://www.harvard.edu/",
    "https://www.stanford.edu/",
    "https://www.mit.edu/",
    "https://www.berkeley.edu/",
    "https://www.utexas.edu/",
    "https://www.umich.edu/",
    "https://www.ucla.edu/",
    "https://www.yale.edu/",
    "https://www.princeton.edu/",
    "https://www.columbia.edu/",
    "https://www.cmu.edu/",

    # ============================
    # CULTURE / SOCIETY
    # ============================
    "https://www.smithsonianmag.com/",
    "https://www.nationalgeographic.com/",
    "https://www.imdb.com/",
    "https://www.billboard.com/",
    "https://www.rollingstone.com/",
    "https://www.espn.com/",
    "https://www.history.com/",

    # ============================
    # MAJOR U.S. CITY HUBS
    # (local news + city gov for top metros)
    # ============================

    # --- New York City ---
    "https://www.nyc.gov/",
    "https://www.nbcnewyork.com/",
    "https://www.nytimes.com/section/nyregion",
    "https://www.timeout.com/newyork",

    # --- Los Angeles ---
    "https://www.lacity.org/",
    "https://www.latimes.com/",
    "https://abc7.com/",
    "https://ktla.com/",

    # --- Chicago ---
    "https://www.chicago.gov/",
    "https://www.chicagotribune.com/",
    "https://chicago.suntimes.com/",
    "https://blockclubchicago.org/",

    # --- Houston ---
    "https://www.houstontx.gov/",
    "https://www.houstonchronicle.com/",
    "https://abc13.com/houston/",

    # --- Phoenix ---
    "https://www.phoenix.gov/",
    "https://www.azcentral.com/",

    # --- Miami ---
    "https://www.miamigov.com/",
    "https://www.miamiherald.com/",

    # --- Boston ---
    "https://www.boston.gov/",
    "https://www.bostonglobe.com/",
    "https://www.wbur.org/",

    # --- Atlanta ---
    "https://www.atlantaga.gov/",
    "https://www.ajc.com/",

    # --- Washington D.C. ---
    "https://dc.gov/",
    "https://www.washingtonpost.com/dc-md-va/",

    # ============================
    # PHILADELPHIA BLOCK
    # ============================

    # --- Philly News ---
    "https://www.inquirer.com/",
    "https://www.phillyvoice.com/",
    "https://billypenn.com/",
    "https://www.phillymag.com/",
    "https://whyy.org/news/",
    "https://www.phillytrib.com/",
    "https://6abc.com/philadelphia/",
    "https://www.nbcphiladelphia.com/",

    # --- Philly Government ---
    "https://www.phila.gov/",
    "https://www.opendataphilly.org/",
    "https://www.phila.gov/citycouncil/",
    "https://www.philadelphiapolice.com/",
    "https://atlas.phila.gov/",
    "https://courts.phila.gov/",

    # --- Philly Property & Real Estate ---
    "https://property.phila.gov/",
    "https://www.realtor.com/realestateandhomes-search/Philadelphia_PA",
    "https://www.redfin.com/city/15907/PA/Philadelphia",

    # --- Philly Transit & Culture ---
    "https://www.septa.org/",
    "https://www.visitphilly.com/",
    "https://www.philadelphiacalendar.com/",

    # --- Philly Universities ---
    "https://www.upenn.edu/",
    "https://www.temple.edu/",
    "https://www.drexel.edu/",
    "https://www.lasalle.edu/",
    "https://www.sju.edu/",
]


OUTPUT_DIR = "./__worker"  # KVS worker directory
LOG_FILE = "./crawl.log"  # Log file path
MAX_PAGES = 500000  # Target pages for tonight
NUM_WORKERS = 12  # Reduced for stability
CRAWL_DELAY = 0.5  # More respectful crawl delay
STORE_PAGE_CONTENT = True  # Set False to save memory (won't store HTML)
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# =============================================================================
# BLACKLIST PATTERNS (matches Java RobustCrawler blacklist logic)
# =============================================================================
BLACKLIST_PATTERNS = [
    # Add your blacklist patterns here (supports wildcards * and ?)
    # Examples:
    # "*spam.com*",
    # "*://malicious-site.com/*",
    # "*/admin/*",
    # "*.example.com/private/*",
]

# =============================================================================
# KEY ENCODING (matches Java KVS KeyEncoder)
# =============================================================================
def encode_key(key: str) -> str:
    """Encode key for filesystem - matches Java KeyEncoder"""
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"
    out = []
    for char in key:
        if char in allowed:
            out.append(char)
        else:
            out.append(f"_{ord(char):x}")
    return "".join(out)


def hasher_hash(s: str) -> str:
    """Match Java Hasher.hash() - SHA-256"""
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def matches_blacklist(url: str, patterns: List[str]) -> bool:
    """
    Check if URL matches any blacklist pattern (supports wildcards).
    Matches Java RobustCrawler.matchesWildcard() logic.
    """
    if not patterns:
        return False

    for pattern in patterns:
        # Convert wildcard pattern to regex
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        if re.match(regex_pattern, url):
            return True
    return False


# =============================================================================
# LOGGING SETUP
# =============================================================================
class CrawlLogger:
    """
    Thread-safe logger that writes to both console and file.
    Use: tail -f crawl.log to monitor live
    """

    def __init__(self, log_file: str, level: str = "INFO"):
        self.log_file = log_file
        self._lock = threading.Lock()

        # Ensure directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Create logger
        self.logger = logging.getLogger('RobustCrawler')
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self.logger.handlers = []  # Clear existing handlers

        # File handler (detailed)
        fh = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(fh_format)
        self.logger.addHandler(fh)

        # Console handler (summary)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch_format = logging.Formatter('%(message)s')
        ch.setFormatter(ch_format)
        self.logger.addHandler(ch)

        # Silence other loggers
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.ERROR)

        # Stats file for periodic snapshots
        self.stats_file = log_file.replace('.log', '_stats.jsonl')

    def debug(self, msg: str):
        with self._lock:
            self.logger.debug(msg)

    def info(self, msg: str):
        with self._lock:
            self.logger.info(msg)

    def warning(self, msg: str):
        with self._lock:
            self.logger.warning(msg)

    def error(self, msg: str):
        with self._lock:
            self.logger.error(msg)

    def crawl_event(self, url: str, status: int, size: int = 0, error: str = None):
        """Log individual crawl events (DEBUG level - file only)"""
        domain = urlparse(url).netloc
        if error:
            self.debug(f"CRAWL | {status:3d} | ERROR: {error:<20} | {domain} | {url[:100]}")
        else:
            self.debug(f"CRAWL | {status:3d} | {size:>10,} bytes | {domain} | {url[:100]}")

    def progress(self, pages_200: int, total: int, target: int, queue_size: int,
                 domains: int, rate: float, eta_min: float):
        """Log progress updates (INFO level - console + file)"""
        pct = (pages_200 / target) * 100
        self.info(
            f"  📄 {pages_200:,}/{target:,} ({pct:.1f}%) | "
            f"{rate:.1f}/s | ETA: {eta_min:.0f}m | "
            f"Queue: {queue_size:,} | Domains: {domains:,}"
        )

    def write_stats_snapshot(self, stats: dict):
        """Write periodic stats to JSONL file"""
        stats['timestamp'] = datetime.now().isoformat()
        with self._lock:
            with open(self.stats_file, 'a') as f:
                f.write(json.dumps(stats) + '\n')

    def section(self, title: str):
        """Log a section header"""
        self.info(f"\n{'='*60}")
        self.info(f"  {title}")
        self.info(f"{'='*60}")


# =============================================================================
# KVS ROW FORMAT - Matches Java Row.toByteArray()
# =============================================================================
class KVSRow:
    """
    Matches Java cis5550.kvs.Row format exactly.
    Format: key SP col SP len SP value SP col SP len SP value SP ... LF
    """
    def __init__(self, key: str):
        self.key = key
        self.columns: Dict[str, bytes] = {}

    def put(self, column: str, value):
        if isinstance(value, str):
            self.columns[column] = value.encode('utf-8')
        else:
            self.columns[column] = value

    def get(self, column: str) -> Optional[str]:
        val = self.columns.get(column)
        return val.decode('utf-8') if val else None

    def to_bytes(self) -> bytes:
        """Exact match to Java Row.toByteArray()"""
        buf = BytesIO()
        buf.write(self.key.encode('utf-8'))
        buf.write(b' ')

        for col, val in self.columns.items():
            buf.write(col.encode('utf-8'))
            buf.write(b' ')
            buf.write(str(len(val)).encode('utf-8'))
            buf.write(b' ')
            buf.write(val)
            buf.write(b' ')

        return buf.getvalue()

    @staticmethod
    def read_from(data: bytes) -> Optional['KVSRow']:
        """Parse Java Row format"""
        try:
            parts = data.split(b' ')
            if len(parts) < 1:
                return None
            key = parts[0].decode('utf-8')
            row = KVSRow(key)
            i = 1
            while i + 2 < len(parts):
                col = parts[i].decode('utf-8')
                length = int(parts[i + 1].decode('utf-8'))
                val = parts[i + 2]
                row.columns[col] = val
                i += 3
            return row
        except:
            return None


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================
@dataclass
class CrawlerConfig:
    max_pages: int = 500000  # Updated for tonight's run
    max_pages_per_domain: int = 5000  # Reduced for better variety across domains
    max_depth: int = 12  # Balanced depth (matches Java RobustCrawler approach)
    max_url_length: int = 2048
    max_content_size: int = 10 * 1024 * 1024
    default_crawl_delay: float = 0.5  # More respectful delay
    request_timeout: int = 15
    num_workers: int = 16
    user_agent: str = "cis5550-crawler"
    respect_robots: bool = True
    store_content: bool = True
    progress_interval: int = 100
    stats_snapshot_interval: int = 1000  # Write stats every N pages
    blocked_extensions: Set[str] = field(default_factory=lambda: {
        'jpg', 'jpeg', 'gif', 'png', 'bmp', 'svg', 'ico', 'webp', 'tiff', 'raw',
        'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'odt', 'ods', 'odp',
        'zip', 'rar', 'tar', 'gz', '7z', 'bz2', 'xz', 'iso',
        'mp3', 'mp4', 'avi', 'mov', 'wmv', 'flv', 'mkv', 'webm', 'wav', 'ogg', 'm4a',
        'exe', 'dmg', 'pkg', 'deb', 'rpm', 'msi', 'apk', 'bin',
        'css', 'js', 'json', 'xml', 'rss', 'atom', 'yaml', 'yml',
        'woff', 'woff2', 'ttf', 'eot', 'otf',
        'sql', 'db', 'sqlite', 'mdb',
    })
    blacklist_patterns: List[str] = field(default_factory=list)  # Matches Java blacklist logic


# =============================================================================
# CRAWL RESULT
# =============================================================================
@dataclass
class CrawlResult:
    url: str
    status_code: int
    content_type: Optional[str] = None
    body: Optional[str] = None
    content_hash: Optional[str] = None
    content_length: int = 0
    redirect_url: Optional[str] = None
    error: Optional[str] = None
    error_message: Optional[str] = None
    anchors: Dict[str, str] = field(default_factory=dict)
    extracted_urls: List[str] = field(default_factory=list)


@dataclass(order=True)
class CrawlTask:
    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False)
    domain: str = field(compare=False)


# =============================================================================
# ROBOTS.TXT HANDLER
# =============================================================================
class RobotsHandler:
    def __init__(self, user_agent: str, default_delay: float, logger: CrawlLogger):
        self.user_agent = user_agent
        self.default_delay = default_delay
        self.logger = logger
        self._cache: Dict[str, Tuple[RobotFileParser, float, str]] = {}
        self._lock = threading.Lock()
        self._session = requests.Session()
        retry = Retry(total=2, backoff_factor=0.2)
        self._session.mount('http://', HTTPAdapter(max_retries=retry))
        self._session.mount('https://', HTTPAdapter(max_retries=retry))

    def _fetch_and_parse(self, robots_url: str) -> Tuple[RobotFileParser, float, str]:
        rp = RobotFileParser()
        crawl_delay = self.default_delay
        raw_content = ""
        try:
            response = self._session.get(robots_url, timeout=5,
                                         headers={'User-Agent': self.user_agent})
            if response.status_code == 200:
                raw_content = response.text
                rp.parse(raw_content.splitlines())
                delay = rp.crawl_delay(self.user_agent) or rp.crawl_delay('*')
                if delay:
                    crawl_delay = max(float(delay), 0.1)
                self.logger.debug(f"ROBOTS | {robots_url} | delay={crawl_delay}s")
        except Exception as e:
            self.logger.debug(f"ROBOTS | {robots_url} | ERROR: {e}")
        rp.parse(raw_content.splitlines() if raw_content else [])
        return rp, crawl_delay, raw_content

    def get_robots_data(self, url: str) -> Tuple[RobotFileParser, float, str]:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        with self._lock:
            if robots_url not in self._cache:
                self._cache[robots_url] = self._fetch_and_parse(robots_url)
            return self._cache[robots_url]

    def is_allowed(self, url: str) -> bool:
        rp, _, _ = self.get_robots_data(url)
        try:
            return rp.can_fetch(self.user_agent, url)
        except:
            return True

    def get_crawl_delay(self, url: str) -> float:
        _, delay, _ = self.get_robots_data(url)
        return delay

    def get_raw_robots(self, url: str) -> str:
        _, _, raw = self.get_robots_data(url)
        return raw


# =============================================================================
# RATE LIMITER
# =============================================================================
class RateLimiter:
    def __init__(self):
        self._last_request: Dict[str, float] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def wait_if_needed(self, host: str, delay: float):
        with self._global_lock:
            if host not in self._locks:
                self._locks[host] = threading.Lock()
            lock = self._locks[host]

        with lock:
            now = time.time()
            last = self._last_request.get(host, 0)
            wait_time = delay - (now - last)
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_request[host] = time.time()


# =============================================================================
# URL UTILITIES
# =============================================================================
class URLUtils:
    @staticmethod
    def normalize_url(base_url: str, href: str) -> Optional[str]:
        if not href:
            return None
        href = href.strip()
        if href.startswith(('javascript:', 'mailto:', 'tel:', 'data:', '#', 'ftp:', 'file:')):
            return None
        try:
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in ('http', 'https'):
                return None

            scheme = parsed.scheme.lower()
            host = parsed.netloc.lower()

            host = re.sub(r':80$', '', host) if scheme == 'http' else host
            host = re.sub(r':443$', '', host) if scheme == 'https' else host

            if not host:
                return None

            path = parsed.path or '/'
            path = re.sub(r'/+', '/', path)

            query = parsed.query
            if query:
                try:
                    params = parse_qs(query, keep_blank_values=True)
                    query = urlencode(sorted(params.items()), doseq=True)
                except:
                    pass

            return urlunparse((scheme, host, path, '', query, ''))
        except:
            return None

    @staticmethod
    def extract_domain(url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except:
            return "unknown"

    @staticmethod
    def has_blocked_extension(url: str, blocked: Set[str]) -> bool:
        try:
            path = urlparse(url).path.lower()
            if '.' in path:
                ext = path.rsplit('.', 1)[-1]
                return ext in blocked
        except:
            pass
        return False

    @staticmethod
    def is_valid_url(url: str, max_length: int) -> bool:
        if not url or len(url) > max_length:
            return False
        try:
            parsed = urlparse(url)
            return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
        except:
            return False


# =============================================================================
# HTML PARSER
# =============================================================================
class HTMLParser:
    LINK_PATTERN = re.compile(
        r'<a\s+[^>]*href\s*=\s*["\']([^"\']*)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL
    )
    TAG_PATTERN = re.compile(r'<[^>]+>')
    WHITESPACE_PATTERN = re.compile(r'\s+')

    @classmethod
    def extract_links_and_anchors(cls, html: str, base_url: str) -> Tuple[List[str], Dict[str, str]]:
        links, anchors = [], {}
        if not html:
            return links, anchors

        seen = set()
        for match in cls.LINK_PATTERN.finditer(html):
            href = match.group(1)
            anchor_text = cls.WHITESPACE_PATTERN.sub(' ', cls.TAG_PATTERN.sub('', match.group(2))).strip()

            normalized = URLUtils.normalize_url(base_url, href)
            if normalized and normalized not in seen:
                seen.add(normalized)
                links.append(normalized)
                if anchor_text:
                    if normalized in anchors:
                        anchors[normalized] += ' ' + anchor_text
                    else:
                        anchors[normalized] = anchor_text[:500]

        return links, anchors


# =============================================================================
# KVS DIRECT STORAGE - Writes directly to worker format
# =============================================================================
class KVSDirectStorage:
    """
    Storage that writes directly to KVS worker directory format.
    Tables: pt-crawl, pt-hosts, pt-domain-stats, pt-content-hashes
    Structure: {output_dir}/{table}/__{key[:2]}/{encoded_key}
    """

    def __init__(self, output_dir: str, store_content: bool = True, logger: CrawlLogger = None):
        self.output_dir = output_dir
        self.store_content = store_content
        self.logger = logger
        os.makedirs(output_dir, exist_ok=True)

        self._locks = {
            'pt-crawl': threading.Lock(),
            'pt-hosts': threading.Lock(),
            'pt-domain-stats': threading.Lock(),
            'pt-content-hashes': threading.Lock()
        }
        self._global_lock = threading.Lock()

        self.crawled_urls: Set[str] = set()
        self.content_hashes: Dict[str, str] = {}
        self.domain_counts: Dict[str, int] = defaultdict(int)
        self.hosts_data: Set[str] = set()

    def _get_lock(self, table: str) -> threading.Lock:
        with self._global_lock:
            if table not in self._locks:
                self._locks[table] = threading.Lock()
            return self._locks[table]

    def _write_row(self, table: str, row: KVSRow):
        """Write row directly as individual file in KVS worker format"""
        encoded_key = encode_key(row.key)
        table_dir = os.path.join(self.output_dir, table)

        # Use subdirectory for keys >= 6 chars (matches Java KVS)
        if len(encoded_key) >= 6:
            file_dir = os.path.join(table_dir, f"__{encoded_key[:2]}")
        else:
            file_dir = table_dir

        lock = self._get_lock(table)
        with lock:
            os.makedirs(file_dir, exist_ok=True)
            filepath = os.path.join(file_dir, encoded_key)
            with open(filepath, 'wb') as f:
                f.write(row.to_bytes())

    def is_crawled(self, url_hash: str) -> bool:
        return url_hash in self.crawled_urls

    def store_crawl_result(self, result: CrawlResult):
        url_hash = hasher_hash(result.url)

        if url_hash in self.crawled_urls:
            return
        self.crawled_urls.add(url_hash)

        row = KVSRow(url_hash)
        row.put('url', result.url)
        row.put('responseCode', str(result.status_code))

        if result.status_code == 200:
            if result.content_type:
                row.put('contentType', result.content_type)
            row.put('length', str(result.content_length))
            if result.content_hash:
                row.put('contentHash', result.content_hash)
            if self.store_content and result.body:
                row.put('page', result.body)

            for link_url, anchor_text in result.anchors.items():
                link_hash = hasher_hash(link_url)
                row.put(f'anchor:{link_hash}', anchor_text)

        elif 300 <= result.status_code < 400:
            if result.redirect_url:
                row.put('location', result.redirect_url)
        else:
            if result.error:
                row.put('error', result.error)
            if result.error_message:
                row.put('message', result.error_message[:500])

        self._write_row('pt-crawl', row)

        if result.status_code == 200:
            domain = URLUtils.extract_domain(result.url)
            self._update_domain_stats(domain)

    def _update_domain_stats(self, domain: str):
        with self._locks['pt-domain-stats']:
            self.domain_counts[domain] += 1
            count = self.domain_counts[domain]

        row = KVSRow(domain)
        row.put('pageCount', str(count))
        row.put('lastUpdate', str(int(time.time() * 1000)))
        self._write_row('pt-domain-stats', row)

    def get_domain_count(self, domain: str) -> int:
        return self.domain_counts.get(domain, 0)

    def is_duplicate_content(self, content_hash: str, url: str) -> bool:
        with self._locks['pt-content-hashes']:
            if content_hash in self.content_hashes:
                return True
            self.content_hashes[content_hash] = url

        row = KVSRow(content_hash)
        row.put('canonicalURL', url)
        row.put('firstSeen', str(int(time.time() * 1000)))
        self._write_row('pt-content-hashes', row)
        return False

    def store_robots(self, host: str, robots_content: str, crawl_delay: float):
        if host in self.hosts_data:
            return
        self.hosts_data.add(host)

        row = KVSRow(host)
        row.put('robots', robots_content)
        row.put('crawlDelay', str(int(crawl_delay * 1000)))
        self._write_row('pt-hosts', row)

    def get_stats(self) -> Dict:
        return {
            'total_pages': len(self.crawled_urls),
            'unique_hosts': len(self.hosts_data),
            'domains_crawled': len(self.domain_counts),
            'unique_content': len(self.content_hashes)
        }


# =============================================================================
# MAIN CRAWLER
# =============================================================================
class RobustCrawler:
    def __init__(self, config: CrawlerConfig, output_dir: str, logger: CrawlLogger):
        self.config = config
        self.logger = logger
        self.storage = KVSDirectStorage(output_dir, config.store_content, logger)
        self.robots = RobotsHandler(config.user_agent, config.default_crawl_delay, logger)
        self.rate_limiter = RateLimiter()

        self.queue: PriorityQueue = PriorityQueue()
        self.queued_urls: Set[str] = set()
        self.queue_lock = threading.Lock()

        self.pages_crawled = 0
        self.pages_200 = 0
        self.errors_count = 0
        self.redirects_count = 0
        self.stats_lock = threading.Lock()
        self.shutdown = threading.Event()
        self.start_time = 0
        self.running = False
        self.workers = []

        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.headers['User-Agent'] = config.user_agent
        self.session.headers['Accept'] = 'text/html,application/xhtml+xml'
        self.session.headers['Accept-Language'] = 'en-US,en;q=0.9'

    def _should_skip_url(self, url: str, depth: int, domain: str) -> bool:
        url_hash = hasher_hash(url)
        if self.storage.is_crawled(url_hash):
            return True
        if not URLUtils.is_valid_url(url, self.config.max_url_length):
            return True
        if URLUtils.has_blocked_extension(url, self.config.blocked_extensions):
            return True
        # Blacklist check (matches Java RobustCrawler)
        if self.config.blacklist_patterns and matches_blacklist(url, self.config.blacklist_patterns):
            self.logger.debug(f"BLACKLIST | {url}")
            return True
        if depth > self.config.max_depth:
            return True
        if self.storage.get_domain_count(domain) >= self.config.max_pages_per_domain:
            return True
        if self.config.respect_robots and not self.robots.is_allowed(url):
            return True
        return False

    def _fetch_url(self, task: CrawlTask) -> CrawlResult:
        url, domain = task.url, task.domain

        crawl_delay = self.robots.get_crawl_delay(url)
        self.rate_limiter.wait_if_needed(domain, crawl_delay)

        robots_content = self.robots.get_raw_robots(url)
        self.storage.store_robots(domain, robots_content, crawl_delay)

        try:
            head = self.session.head(url, timeout=self.config.request_timeout, allow_redirects=False)
            status = head.status_code

            if 300 <= status < 400:
                location = head.headers.get('Location', '')
                self.logger.crawl_event(url, status, error=f"Redirect -> {location[:50]}")
                return CrawlResult(url=url, status_code=status, redirect_url=location)

            content_type = head.headers.get('Content-Type', '')
            if not any(t in content_type.lower() for t in ['text/html', 'application/xhtml']):
                self.logger.crawl_event(url, status, error=f"Skip: {content_type[:30]}")
                return CrawlResult(url=url, status_code=status, content_type=content_type)

            content_length = head.headers.get('Content-Length')
            if content_length:
                try:
                    if int(content_length) > self.config.max_content_size:
                        self.logger.crawl_event(url, status, error='ContentTooLarge')
                        return CrawlResult(url=url, status_code=status, error='ContentTooLarge')
                except:
                    pass

            if status != 200:
                self.logger.crawl_event(url, status, error=f'HTTP {status}')
                return CrawlResult(url=url, status_code=status, content_type=content_type)

            get = self.session.get(url, timeout=self.config.request_timeout, allow_redirects=False)
            status = get.status_code

            if status == 200:
                body = get.text
                content_length = len(get.content)

                if content_length > self.config.max_content_size:
                    self.logger.crawl_event(url, status, error='ContentTooLarge')
                    return CrawlResult(url=url, status_code=status, error='ContentTooLarge')

                content_hash = hashlib.sha256(body.encode('utf-8')).hexdigest()

                if self.storage.is_duplicate_content(content_hash, url):
                    self.logger.crawl_event(url, status, error='DuplicateContent')
                    return CrawlResult(url=url, status_code=200, error='DuplicateContent')

                extracted_urls, anchors = HTMLParser.extract_links_and_anchors(body, url)

                self.logger.crawl_event(url, status, size=content_length)

                return CrawlResult(
                    url=url, status_code=status,
                    content_type=get.headers.get('Content-Type', ''),
                    body=body, content_hash=content_hash, content_length=content_length,
                    anchors=anchors, extracted_urls=extracted_urls
                )

            self.logger.crawl_event(url, status, error=f'HTTP {status}')
            return CrawlResult(url=url, status_code=status)

        except requests.exceptions.Timeout:
            self.logger.crawl_event(url, 0, error='Timeout')
            return CrawlResult(url=url, status_code=0, error='Timeout')
        except requests.exceptions.ConnectionError as e:
            self.logger.crawl_event(url, 0, error='ConnectionError')
            return CrawlResult(url=url, status_code=0, error='ConnectionError', error_message=str(e)[:200])
        except Exception as e:
            self.logger.crawl_event(url, 0, error=type(e).__name__)
            return CrawlResult(url=url, status_code=0, error=type(e).__name__, error_message=str(e)[:200])

    def _process_task(self, task: CrawlTask) -> List[CrawlTask]:
        new_tasks = []

        if self.shutdown.is_set():
            return new_tasks

        with self.stats_lock:
            if self.pages_200 >= self.config.max_pages:
                return new_tasks

        if self._should_skip_url(task.url, task.depth, task.domain):
            return new_tasks

        result = self._fetch_url(task)
        self.storage.store_crawl_result(result)

        with self.stats_lock:
            self.pages_crawled += 1
            if result.error:
                self.errors_count += 1
            if 300 <= result.status_code < 400:
                self.redirects_count += 1

        if 300 <= result.status_code < 400 and result.redirect_url:
            redirect_url = URLUtils.normalize_url(task.url, result.redirect_url)
            if redirect_url and redirect_url != task.url:
                new_tasks.append(CrawlTask(
                    task.depth + 1, redirect_url, task.depth + 1,
                    URLUtils.extract_domain(redirect_url)
                ))

        if result.status_code == 200 and result.body and not result.error:
            with self.stats_lock:
                self.pages_200 += 1
                count = self.pages_200

            if count % self.config.progress_interval == 0:
                elapsed = time.time() - self.start_time
                rate = count / elapsed if elapsed > 0 else 0
                eta = (self.config.max_pages - count) / rate / 60 if rate > 0 else 0
                self.logger.progress(
                    count, self.pages_crawled, self.config.max_pages,
                    self.queue.qsize(), len(self.storage.domain_counts),
                    rate, eta
                )

                # Memory monitoring for stability
                if count % 1000 == 0:
                    try:
                        import psutil
                        mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
                        self.logger.info(f"  Memory: {mem_mb:.0f} MB")
                    except ImportError:
                        pass  # psutil not available

            # Periodic stats snapshot
            if count % self.config.stats_snapshot_interval == 0:
                elapsed = time.time() - self.start_time
                stats = {
                    'pages_200': count,
                    'pages_crawled': self.pages_crawled,
                    'errors': self.errors_count,
                    'redirects': self.redirects_count,
                    'queue_size': self.queue.qsize(),
                    'domains': len(self.storage.domain_counts),
                    'unique_content': len(self.storage.content_hashes),
                    'elapsed_seconds': elapsed,
                    'rate_per_second': count / elapsed if elapsed > 0 else 0
                }
                self.logger.write_stats_snapshot(stats)

            for link_url in result.extracted_urls:
                if len(link_url) <= self.config.max_url_length:
                    if not URLUtils.has_blocked_extension(link_url, self.config.blocked_extensions):
                        new_tasks.append(CrawlTask(
                            task.depth + 1, link_url, task.depth + 1,
                            URLUtils.extract_domain(link_url)
                        ))

        return new_tasks

    def _worker(self, worker_id: int):
        self.logger.debug(f"WORKER | {worker_id} | Started")
        while not self.shutdown.is_set():
            try:
                task = self.queue.get(timeout=3)
            except Empty:
                with self.stats_lock:
                    if self.pages_200 >= self.config.max_pages:
                        break
                continue

            try:
                new_tasks = self._process_task(task)

                with self.queue_lock:
                    for new_task in new_tasks:
                        if new_task.url not in self.queued_urls:
                            self.queued_urls.add(new_task.url)
                            self.queue.put(new_task)
            except Exception as e:
                self.logger.error(f"WORKER | {worker_id} | Exception: {e}")
            finally:
                self.queue.task_done()

        self.logger.debug(f"WORKER | {worker_id} | Stopped")
        gc.collect()

    def start(self, seed_urls: List[str]) -> bool:
        """Start crawling in background (non-blocking)"""
        if self.running:
            return False

        self.running = True
        self.shutdown.clear()

        banner = f"""
╔══════════════════════════════════════════════════════════════╗
║  🕷️  RobustCrawler v2 - CIS 5550 (FastAPI)                   ║
╠══════════════════════════════════════════════════════════════╣
║  Seeds: {len(seed_urls):<10}  Max Pages: {self.config.max_pages:<20,} ║
║  Workers: {self.config.num_workers:<8}  Crawl Delay: {self.config.default_crawl_delay:<17}s ║
║  Output: {self.storage.output_dir:<48} ║
║  Log: {self.logger.log_file:<51} ║
╚══════════════════════════════════════════════════════════════╝
"""
        self.logger.info(banner)
        self.logger.info(f"  📋 Monitor live: tail -f {self.logger.log_file}")
        self.logger.info(f"  📊 Stats snapshots: {self.logger.stats_file}\n")

        self.start_time = time.time()

        for url in seed_urls:
            normalized = URLUtils.normalize_url(url, url)
            if normalized:
                domain = URLUtils.extract_domain(normalized)
                self.queue.put(CrawlTask(0, normalized, 0, domain))
                self.queued_urls.add(normalized)
                self.logger.debug(f"SEED | {normalized}")

        self.logger.info(f"  🌱 Added {len(self.queued_urls)} seed URLs\n")

        self.workers = []
        for i in range(self.config.num_workers):
            t = threading.Thread(target=self._worker, args=(i,), daemon=True)
            t.start()
            self.workers.append(t)

        # Monitor thread
        def monitor():
            last_count = 0
            stall_checks = 0

            while not self.shutdown.is_set():
                with self.stats_lock:
                    current = self.pages_200
                    if current >= self.config.max_pages:
                        self.logger.info(f"\n  ✅ Reached target: {self.config.max_pages:,} pages")
                        break

                if current == last_count:
                    stall_checks += 1
                    if stall_checks > 10 and self.queue.empty():
                        self.logger.info(f"\n  ✅ Queue exhausted at {current:,} pages")
                        break
                else:
                    stall_checks = 0
                    last_count = current

                time.sleep(1)

            self.shutdown.set()
            self.running = False

            # Final stats
            elapsed = time.time() - self.start_time
            stats = self.storage.get_stats()
            stats['elapsed_seconds'] = elapsed
            stats['pages_per_second'] = stats['total_pages'] / elapsed if elapsed > 0 else 0
            stats['successful_pages'] = self.pages_200
            stats['errors'] = self.errors_count
            stats['redirects'] = self.redirects_count
            self.logger.write_stats_snapshot(stats)

            summary = f"""
╔══════════════════════════════════════════════════════════════╗
║  📊 CRAWL COMPLETE                                           ║
╠══════════════════════════════════════════════════════════════╣
║  Total Requests:  {stats['total_pages']:<42,} ║
║  Successful (200): {stats['successful_pages']:<41,} ║
║  Errors:          {stats['errors']:<42,} ║
║  Redirects:       {stats['redirects']:<42,} ║
║  Unique Hosts:    {stats['unique_hosts']:<42,} ║
║  Domains:         {stats['domains_crawled']:<42,} ║
║  Unique Content:  {stats['unique_content']:<42,} ║
║  Time:            {elapsed/60:<39.1f} min ║
║  Speed:           {stats['pages_per_second']:<39.1f} pages/sec ║
╚══════════════════════════════════════════════════════════════╝
"""
            self.logger.info(summary)

        threading.Thread(target=monitor, daemon=True).start()
        return True

    def stop(self):
        """Stop crawling"""
        self.shutdown.set()
        self.running = False
        for t in self.workers:
            t.join(timeout=2)

    def get_status(self) -> Dict:
        """Get current crawl status"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        with self.stats_lock:
            rate = self.pages_200 / elapsed if elapsed > 0 else 0
            eta = (self.config.max_pages - self.pages_200) / rate / 60 if rate > 0 else 0

            return {
                'running': self.running,
                'pages_200': self.pages_200,
                'pages_crawled': self.pages_crawled,
                'errors': self.errors_count,
                'redirects': self.redirects_count,
                'queue_size': self.queue.qsize(),
                'queued_urls': len(self.queued_urls),
                'rate_per_second': round(rate, 2),
                'eta_minutes': round(eta, 1),
                'elapsed_seconds': round(elapsed, 1),
                'elapsed_minutes': round(elapsed / 60, 1),
                'target': self.config.max_pages,
                'progress_percent': round(self.pages_200 / self.config.max_pages * 100, 2),
                **self.storage.get_stats()
            }

    def crawl(self, seed_urls: List[str]) -> Dict:
        """Blocking crawl - runs until complete"""
        self.start(seed_urls)

        # Wait for completion
        while self.running:
            time.sleep(1)

        return self.get_status()


# =============================================================================
# FASTAPI APP
# =============================================================================
crawler: Optional[RobustCrawler] = None
logger: Optional[CrawlLogger] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global crawler, logger
    logger = CrawlLogger(LOG_FILE, LOG_LEVEL)
    config = CrawlerConfig(
        max_pages=MAX_PAGES,
        num_workers=NUM_WORKERS,
        default_crawl_delay=CRAWL_DELAY,
        store_content=STORE_PAGE_CONTENT,
        respect_robots=True,
        progress_interval=500,
        stats_snapshot_interval=1000,
        blacklist_patterns=BLACKLIST_PATTERNS,
    )
    crawler = RobustCrawler(config, OUTPUT_DIR, logger)
    yield
    if crawler and crawler.running:
        crawler.stop()


app = FastAPI(
    title="RobustCrawler API",
    description="CIS 5550 Web Crawler - writes directly to KVS worker format",
    version="2.0",
    lifespan=lifespan
)


class StartRequest(BaseModel):
    seed_urls: Optional[List[str]] = None
    max_pages: Optional[int] = None
    num_workers: Optional[int] = None
    output_dir: Optional[str] = None
    crawl_delay: Optional[float] = None
    store_content: Optional[bool] = None


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "RobustCrawler API v2",
        "output_dir": OUTPUT_DIR,
        "tables": ["pt-crawl", "pt-hosts", "pt-domain-stats", "pt-content-hashes"]
    }


@app.post("/start")
async def start_crawl(req: StartRequest = None):
    """Start the crawler"""
    global crawler, logger

    if crawler and crawler.running:
        raise HTTPException(400, "Crawler already running")

    # Build config with overrides
    config = CrawlerConfig(
        max_pages=req.max_pages if req and req.max_pages else MAX_PAGES,
        num_workers=req.num_workers if req and req.num_workers else NUM_WORKERS,
        default_crawl_delay=req.crawl_delay if req and req.crawl_delay else CRAWL_DELAY,
        store_content=req.store_content if req and req.store_content is not None else STORE_PAGE_CONTENT,
        respect_robots=True,
        progress_interval=500,
        stats_snapshot_interval=1000,
        blacklist_patterns=BLACKLIST_PATTERNS,
    )

    output_dir = req.output_dir if req and req.output_dir else OUTPUT_DIR
    seeds = req.seed_urls if req and req.seed_urls else SEED_URLS

    logger = CrawlLogger(LOG_FILE, LOG_LEVEL)
    crawler = RobustCrawler(config, output_dir, logger)
    crawler.start(seeds)

    return {
        "status": "started",
        "seeds": len(seeds),
        "max_pages": config.max_pages,
        "num_workers": config.num_workers,
        "output_dir": output_dir,
        "tables": ["pt-crawl", "pt-hosts", "pt-domain-stats", "pt-content-hashes"]
    }


@app.post("/stop")
async def stop_crawl():
    """Stop the crawler"""
    global crawler
    if not crawler:
        raise HTTPException(400, "No crawler instance")

    crawler.stop()
    return {"status": "stopped", **crawler.get_status()}


@app.get("/status")
async def get_status():
    """Get crawler status"""
    global crawler
    if not crawler:
        return {"running": False, "message": "No crawler instance"}
    return crawler.get_status()


@app.get("/stats")
async def get_stats():
    """Get detailed statistics"""
    global crawler
    if not crawler:
        raise HTTPException(400, "No crawler instance")

    stats = crawler.storage.get_stats()
    stats['top_domains'] = dict(sorted(
        crawler.storage.domain_counts.items(),
        key=lambda x: x[1],
        reverse=True
    )[:20])
    return stats


# =============================================================================
# STANDALONE RUN (blocking mode)
# =============================================================================
def run_standalone():
    """Run crawler in standalone blocking mode (not as API)"""
    print("Running in standalone mode...")

    log = CrawlLogger(LOG_FILE, LOG_LEVEL)
    config = CrawlerConfig(
        max_pages=MAX_PAGES,
        num_workers=NUM_WORKERS,
        default_crawl_delay=CRAWL_DELAY,
        store_content=STORE_PAGE_CONTENT,
        respect_robots=True,
        progress_interval=500,
        stats_snapshot_interval=1000,
        blacklist_patterns=BLACKLIST_PATTERNS,
    )

    c = RobustCrawler(config, OUTPUT_DIR, log)
    stats = c.crawl(SEED_URLS)

    print(f"\n✅ Done! Files in {OUTPUT_DIR}/")
    print(f"   - pt-crawl/")
    print(f"   - pt-hosts/")
    print(f"   - pt-domain-stats/")
    print(f"   - pt-content-hashes/")
    print(f"   - crawl.log")
    print(f"   - crawl_stats.jsonl")

    return stats


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--standalone":
        # Run as standalone blocking crawler
        run_standalone()
    else:
        # Run as FastAPI server
        import uvicorn

        print("""
╔══════════════════════════════════════════════════════════════╗
║  🕷️  RobustCrawler FastAPI v2                                ║
║  Writes directly to KVS worker format                        ║
╠══════════════════════════════════════════════════════════════╣
║  Tables: pt-crawl, pt-hosts, pt-domain-stats,                ║
║          pt-content-hashes                                   ║
╠══════════════════════════════════════════════════════════════╣
║  Endpoints:                                                  ║
║    POST /start  - Start crawling                             ║
║    POST /stop   - Stop crawling                              ║
║    GET  /status - Get current status                         ║
║    GET  /stats  - Get detailed stats                         ║
╠══════════════════════════════════════════════════════════════╣
║  Run standalone: python crawler_api.py --standalone          ║
╚══════════════════════════════════════════════════════════════╝
""")

        uvicorn.run(app, host="0.0.0.0", port=8000)