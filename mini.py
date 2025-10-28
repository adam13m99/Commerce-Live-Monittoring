# metabase_shared.py
"""
Shared Metabase client with cross-process session reuse.

Features:
- Single login (first use), token saved to ~/.metabase_sessions/ (configurable).
- All scripts/processes on the same machine reuse the session.
- Auto-reauth on 401 and token refresh written back to cache.
- Database ID cache (works with 'growth' / 'data' / 'product' aliases or full names).
- Parallel pagination for large results.
- Thread-safe and process-safe via file locks (uses 'filelock' if installed).

Install requirements:
    pip install requests pandas filelock

Optional env:
    METABASE_SESSION_CACHE_DIR=/path/to/cache    # default: ~/.metabase_sessions
"""

import os
import json
import time
import hashlib
import logging
import threading
import contextlib
import concurrent.futures
import atexit
from pathlib import Path
from typing import Optional, Dict, Tuple, Any

import requests
import pandas as pd
from dataclasses import dataclass

# ---------------- Logging ----------------
logger = logging.getLogger("metabase_shared")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------- Optional file lock -------------
try:
    from filelock import FileLock
    _HAS_FILELOCK = True
except Exception:
    _HAS_FILELOCK = False


# ------------- Disk cache helpers -------------
def _cache_dir() -> Path:
    d = Path(os.getenv("METABASE_SESSION_CACHE_DIR", "~/.metabase_sessions")).expanduser()
    d.mkdir(parents=True, exist_ok=True)
    return d

def _cache_paths(url: str, username: str) -> Tuple[Path, Path]:
    key = f"{url}|{username}".encode("utf-8")
    h = hashlib.sha256(key).hexdigest()[:16]
    return _cache_dir() / f"session_{h}.json", _cache_dir() / f"session_{h}.lock"

@contextlib.contextmanager
def _cache_lock(lock_path: Path, timeout: float = 30.0):
    """
    Cross-process lock.
    Tries 'filelock' if available; otherwise uses naive O_EXCL spin-lock.
    """
    if _HAS_FILELOCK:
        lock = FileLock(str(lock_path))
        lock.acquire(timeout=timeout)
        try:
            yield
        finally:
            lock.release()
    else:
        start = time.time()
        fd = None
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                break
            except FileExistsError:
                if time.time() - start > timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {lock_path}")
                time.sleep(0.1)
        try:
            yield
        finally:
            try:
                if fd is not None:
                    os.close(fd)
                if lock_path.exists():
                    lock_path.unlink(missing_ok=True)
            except Exception:
                pass

def _read_json(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_json_atomic(p: Path, data: Dict[str, Any], lock_path: Path):
    with _cache_lock(lock_path):
        tmp = p.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f)
        tmp.replace(p)


# ------------- Config -------------
@dataclass
class MetabaseConfig:
    url: str
    username: str
    password: str
    database_name: Optional[str] = None
    database_id: Optional[int] = None

    @classmethod
    def create_with_team_db(cls, url: str, username: str, password: str, team: str):
        team_dbs = {
            "growth":  "Growth Team Clickhouse Connection",
            "data":    "Data Team Clickhouse Connection",
            "product": "Product Team Clickhouse Connection",
        }
        name = team_dbs.get(team.lower())
        if not name:
            raise ValueError(f"Invalid team. Choose from: {list(team_dbs.keys())}")
        return cls(url=url, username=username, password=password, database_name=name)


# ------------- Shared Client -------------
_TEAM_ALIASES = {
    "growth":  "Growth Team Clickhouse Connection",
    "data":    "Data Team Clickhouse Connection",
    "product": "Product Team Clickhouse Connection",
}

class MetabaseSharedClient:
    """
    A client that shares one Metabase session across threads *and processes*.

    - Token persisted to disk; others reuse it.
    - Auto-reauth if expired.
    - DB id cache persisted to disk (works across scripts).
    """

    def __init__(self, config: MetabaseConfig):
        self.config = config
        self.session = requests.Session()
        self.session_token: Optional[str] = None
        self.database_id: Optional[int] = config.database_id
        self._db_ids: Dict[str, int] = {}  # name -> id

        self._cache_path, self._lock_path = _cache_paths(config.url, config.username)
        self._cache_mtime: float = 0.0

        self._inproc_auth_lock = threading.Lock()  # avoid stampede on reauth

        self._load_cache()
        self._ensure_session()  # first use: create or reuse session

        # If config has a default database_name, make sure it's in cache
        if config.database_name and not self.database_id:
            self.database_id = self._resolve_database_id(config.database_name)
            self._write_cache()

    # ----- Cache -----
    def _load_cache(self):
        if self._cache_path.exists():
            self._cache_mtime = self._cache_path.stat().st_mtime
            data = _read_json(self._cache_path)
            token = data.get("session_token")
            if token:
                self.session_token = token
                self.session.headers.update({"X-Metabase-Session": self.session_token})

            db_ids = data.get("database_ids", {})
            if isinstance(db_ids, dict):
                # normalize ids to int
                for k, v in db_ids.items():
                    try:
                        self._db_ids[k] = int(v)
                    except Exception:
                        continue

    def _maybe_reload_token_from_disk(self):
        try:
            m = self._cache_path.stat().st_mtime
        except FileNotFoundError:
            return
        if m > self._cache_mtime:
            self._cache_mtime = m
            data = _read_json(self._cache_path)
            token = data.get("session_token")
            if token and token != self.session_token:
                self.session_token = token
                self.session.headers.update({"X-Metabase-Session": self.session_token})
            db_ids = data.get("database_ids", {})
            if isinstance(db_ids, dict):
                for k, v in db_ids.items():
                    try:
                        self._db_ids[k] = int(v)
                    except Exception:
                        continue

    def _write_cache(self):
        data = _read_json(self._cache_path)
        if self.session_token:
            data["session_token"] = self.session_token
        if self._db_ids:
            data["database_ids"] = self._db_ids
        data["updated_at"] = int(time.time())
        _write_json_atomic(self._cache_path, data, self._lock_path)
        try:
            self._cache_mtime = self._cache_path.stat().st_mtime
        except FileNotFoundError:
            pass

    # ----- Session lifecycle -----
    def _ping(self) -> bool:
        try:
            r = self.session.get(f"{self.config.url}/api/user/current", timeout=10)
            if r.status_code == 401:
                return False
            r.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def authenticate(self) -> bool:
        try:
            auth_url = f"{self.config.url}/api/session"
            r = self.session.post(auth_url, json={
                "username": self.config.username,
                "password": self.config.password
            }, timeout=30)
            r.raise_for_status()
            token = r.json().get("id")
            if not token:
                logger.error("Auth returned no token.")
                return False
            self.session_token = token
            self.session.headers.update({"X-Metabase-Session": self.session_token})
            self._write_cache()
            logger.info("Authenticated (new session).")
            return True
        except requests.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def _ensure_session(self):
        # Try existing token; if bad, authenticate and cache
        self._maybe_reload_token_from_disk()
        if self.session_token and self._ping():
            return
        with self._inproc_auth_lock:
            self._maybe_reload_token_from_disk()
            if self.session_token and self._ping():
                return
            if not self.authenticate():
                raise RuntimeError("Metabase authentication failed")

    def _request(self, method: str, url: str, *, retry_on_401: bool = True, **kwargs) -> requests.Response:
        self._ensure_session()
        self._maybe_reload_token_from_disk()

        resp = self.session.request(method, url, **kwargs)
        if retry_on_401 and resp.status_code == 401:
            logger.info("Session expired (401). Reauthenticating...")
            with self._inproc_auth_lock:
                self._maybe_reload_token_from_disk()
                if resp.status_code == 401 and not self._ping():
                    if not self.authenticate():
                        resp.raise_for_status()
            resp = self.session.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    # ----- Database resolution -----
    def _resolve_database_id(self, database: Optional[Any]) -> Optional[int]:
        """
        database can be:
          - None: use default (from config) if available
          - int:  treated as id
          - str:  team alias ('growth'/'data'/'product') OR full Metabase DB name
        """
        if database is None:
            return self.database_id

        if isinstance(database, int):
            return database

        # string: alias or full name
        name = _TEAM_ALIASES.get(str(database).lower(), str(database))

        if name in self._db_ids:
            return self._db_ids[name]

        # fetch all databases once, cache mapping
        r = self._request("GET", f"{self.config.url}/api/database")
        mapping = {db.get("name"): int(db.get("id")) for db in r.json().get("data", []) if db.get("id") is not None}
        if mapping:
            self._db_ids.update(mapping)
            self._write_cache()

        did = self._db_ids.get(name)
        if did is None:
            logger.error(f"Database '{name}' not found.")
        return did

    # ----- Public API -----
    def get_question_details(self, question_id: int) -> Optional[dict]:
        try:
            r = self._request("GET", f"{self.config.url}/api/card/{question_id}")
            return r.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get question {question_id}: {e}")
            return None

    def execute_query(
        self,
        sql_query: str,
        *,
        database: Optional[Any] = None,
        timeout: int = 300,
        max_results: int = 100000
    ) -> Optional[pd.DataFrame]:
        db_id = self._resolve_database_id(database)
        if not db_id:
            return None
        try:
            payload = {
                "type": "native",
                "native": {"query": sql_query},
                "database": db_id,
                "constraints": {"max-results": max_results, "max-results-bare-rows": max_results},
            }
            r = self._request("POST", f"{self.config.url}/api/dataset", json=payload, timeout=timeout)
            result = r.json()
            if result.get("status") and result.get("status") != "completed":
                logger.error(f"Query failed. Status: {result.get('status')}, Error: {result.get('error')}")
                return None
            data = result.get("data", {})
            rows = data.get("rows", [])
            cols = [c["name"] for c in data.get("cols", [])]
            return pd.DataFrame(rows, columns=cols)
        except requests.RequestException as e:
            logger.error(f"Query execution failed: {e}")
            return None

    def execute_query_with_parallel_pagination(
        self,
        sql_query: str,
        *,
        database: Optional[Any] = None,
        page_size: int = 50_000,
        max_workers: int = 8
    ) -> Optional[pd.DataFrame]:
        logger.info(f"--Parallel pagination: workers={max_workers}, page_size={page_size:,}")

        db_id = self._resolve_database_id(database)
        if not db_id:
            return None

        # Count rows
        count_sql = f"SELECT COUNT(*) AS total_rows FROM ({sql_query.rstrip(';')}) subq"
        count_df = self.execute_query(count_sql, database=db_id)
        if count_df is None or count_df.empty:
            logger.error("Failed to count rows.")
            return None
        total_rows = int(count_df.iloc[0]["total_rows"])
        if total_rows == 0:
            return pd.DataFrame()
        total_pages = (total_rows + page_size - 1) // page_size
        logger.info(f"total_rows={total_rows:,}, pages={total_pages}, page_size={page_size:,}")

        def fetch_page(i: int) -> Optional[pd.DataFrame]:
            try:
                with requests.Session() as s:
                    s.headers.update({"X-Metabase-Session": self.session_token})
                    offset = i * page_size
                    paginated = f"{sql_query.rstrip(';')} LIMIT {page_size} OFFSET {offset}"
                    payload = {
                        "type": "native",
                        "native": {"query": paginated},
                        "database": db_id,
                        "constraints": {"max-results": page_size, "max-results-bare-rows": page_size},
                    }
                    url = f"{self.config.url}/api/dataset"
                    r = s.post(url, json=payload, timeout=300)
                    if r.status_code == 401:
                        logger.info(f"Page {i+1}: 401 -> refreshing session...")
                        with self._inproc_auth_lock:
                            self._maybe_reload_token_from_disk()
                            if not self._ping():
                                if not self.authenticate():
                                    r.raise_for_status()
                            s.headers.update({"X-Metabase-Session": self.session_token})
                        r = s.post(url, json=payload, timeout=300)
                    r.raise_for_status()
                    result = r.json()
                    data = result.get("data", {})
                    rows = data.get("rows", [])
                    cols = [c["name"] for c in data.get("cols", [])]
                    df = pd.DataFrame(rows, columns=cols)
                    logger.info(f"✅ Page {i+1}/{total_pages} fetched ({len(df):,} rows)")
                    return df
            except Exception as e:
                logger.error(f"Error fetching page {i+1}: {e}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as exe:
            parts = list(exe.map(fetch_page, range(total_pages)))

        parts = [p for p in parts if p is not None]
        if not parts:
            logger.error("No data retrieved from any page.")
            return None

        out = pd.concat(parts, ignore_index=True)
        # Only warn if the difference is significant (>1% or >50 rows)
        row_diff = abs(len(out) - total_rows)
        if row_diff > 50 and (row_diff / max(total_rows, 1)) > 0.01:
            logger.warning(f"Row mismatch: expected {total_rows}, got {len(out)}")
        return out

    def logout(self):
        if not self.session_token:
            return
        try:
            self._request("DELETE", f"{self.config.url}/api/session", retry_on_401=False)
            logger.info("Logged out.")
        except requests.RequestException as e:
            logger.warning(f"Logout failed: {e}")
        finally:
            self.session_token = None
            self._write_cache()


# ------------- In-process client pool -------------
_client_pool: Dict[Tuple[str, str], MetabaseSharedClient] = {}
_client_pool_lock = threading.Lock()

def get_shared_client(metabase_url: str, username: str, password: str) -> MetabaseSharedClient:
    """
    Returns a process-wide shared client (keyed by url+username).
    Other processes reuse the same token via the disk cache.
    """
    key = (metabase_url, username)
    with _client_pool_lock:
        c = _client_pool.get(key)
        if c is None:
            cfg = MetabaseConfig(url=metabase_url, username=username, password=password)
            c = MetabaseSharedClient(cfg)
            _client_pool[key] = c
        return c


# ------------- Convenience wrappers -------------
def fetch_question_data_with_client(
    client: MetabaseSharedClient,
    question_id: int,
    *,
    database: Optional[Any] = None,
    workers: int = 8,
    page_size: int = 50_000
) -> Optional[pd.DataFrame]:
    details = client.get_question_details(question_id)
    if not details:
        return None
    native = details.get("dataset_query", {}).get("native")
    if not native or "query" not in native:
        logger.error(f"Question {question_id} is not a native SQL query.")
        return None
    sql = native["query"]
    return client.execute_query_with_parallel_pagination(
        sql, database=database, page_size=page_size, max_workers=workers
    )

def fetch_question_data(
    question_id: int,
    metabase_url: str,
    username: str,
    password: str,
    *,
    database: Optional[Any] = None,
    workers: int = 8,
    page_size: int = 50_000
) -> Optional[pd.DataFrame]:
    client = get_shared_client(metabase_url, username, password)
    return fetch_question_data_with_client(
        client, question_id, database=database, workers=workers, page_size=page_size
    )

def _cleanup_all():
    for c in list(_client_pool.values()):
        try:
            c.logout()
        except Exception:
            pass

if os.getenv("METABASE_LOGOUT_ON_EXIT", "").lower() in ("1", "true", "yes"):
    atexit.register(_cleanup_all)
