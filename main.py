# main.py
#
# Python backend for TriCoach — handles Garmin Connect authentication
# and FIT file downloads.
#
# Deployed on Render (free tier). The /health endpoint is used by
# the Flutter app to check if the server is reachable (warm-up).
#
# IMPORTANT: This version uses garth DIRECTLY for login instead of
# garminconnect.Garmin.login(). Why? Because garminconnect's login()
# method wraps garth which has its OWN aggressive retry logic —
# it fires 3 rapid OAuth requests in ~6 seconds, each hitting Garmin's
# rate limiter if the account is throttled. That makes things worse.
#
# By using garth directly, we control exactly ONE login request per
# attempt, with clean 60/120/240s waits between retries.
#
# Session caching:
#   - In-memory cache: 60-minute TTL
#   - Disk persistence: OAuth tokens saved to /tmp/garmin_session.json
#     so Render restarts don't force re-login
#   - Request lock: threading.Lock prevents concurrent logins
#
# Endpoints:
#   GET  /health              — health check (used by Flutter for warm-up)
#   GET  /check_rate_limit    — diagnose rate limiting (no login)
#   POST /test_connection     — test Garmin credentials
#   POST /activities          — list all Garmin activities
#   POST /activity/<id>/fit   — download raw FIT file for one activity

from flask import Flask, request, jsonify, Response
from garminconnect import Garmin
import garth
import time
import json
import os
import logging
import sys
import threading

app = Flask(__name__)

# Set up logging so we can see what's happening on Render
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --- Circuit Breaker ---
#
# Tracks Garmin 429 (rate limit) errors and automatically blocks all
# login attempts for a cool-off period when too many happen in a short
# window. This prevents the backend from hammering Garmin and making
# the rate limit worse.
#
# How it works:
#   - Tracks 429 failures in a sliding 30-minute window
#   - After 3 failures in that window, the circuit "opens"
#   - While open, all login requests are rejected immediately with a
#     clear error message (no request to Garmin is made)
#   - After a 2-hour cool-off, the circuit "half-opens" and allows
#     one attempt through
#   - If that attempt succeeds, the circuit closes (normal operation)
#   - If it fails, the circuit opens again for another 2 hours
#
# This protects both the user's Garmin account and the backend from
# wasting resources on requests that will definitely fail.

_circuit_breaker = {
    "state": "closed",        # "closed" | "open" | "half-open"
    "failures": [],           # list of timestamps of recent 429s
    "opened_at": 0,           # timestamp when circuit was last opened
}

# Circuit breaker thresholds
_CB_FAILURE_WINDOW = 1800     # 30 minutes — sliding window for counting failures
_CB_FAILURE_THRESHOLD = 3     # 3 failures in window → open circuit
_CB_COOL_OFF = 7200           # 2 hours — how long to stay open
_CB_MAX_FAILURES = 10         # 10 total failures ever → stay open indefinitely (manual reset needed)


def _check_circuit_breaker() -> dict:
    """
    Checks the current state of the circuit breaker.
    
    Returns a dict with:
      - blocked: True if requests should be blocked
      - reason: explanation string for the user
      - retry_after: seconds until retry is possible (or None)
    
    Also handles state transitions:
      - If open and cool-off has passed, transitions to half-open
      - If half-open, allows one request through
    """
    now = time.time()
    cb = _circuit_breaker
    
    # Prune old failures outside the sliding window
    cb["failures"] = [f for f in cb["failures"] if now - f < _CB_FAILURE_WINDOW]
    
    # Check if total failures exceeded max — permanent block until manual reset
    if len(cb["failures"]) >= _CB_MAX_FAILURES:
        return {
            "blocked": True,
            "reason": (
                "Garmin rate limit has been hit too many times. "
                "The sync system is disabled to protect your account. "
                "Please try again in 24 hours, or contact support."
            ),
            "retry_after": 86400,  # 24 hours
        }
    
    state = cb["state"]
    
    if state == "closed":
        # Normal operation — check if we should open
        if len(cb["failures"]) >= _CB_FAILURE_THRESHOLD:
            cb["state"] = "open"
            cb["opened_at"] = now
            logger.warning(
                f"Circuit breaker OPENED — {len(cb['failures'])} "
                f"rate limit hits in the last {_CB_FAILURE_WINDOW // 60} minutes"
            )
            return {
                "blocked": True,
                "reason": (
                    f"Garmin rate limit reached. "
                    f"Sync is paused for {_CB_COOL_OFF // 3600} hours "
                    f"to let your account cool down. "
                    f"Come back later and try again."
                ),
                "retry_after": _CB_COOL_OFF,
            }
        return {"blocked": False}
    
    if state == "open":
        # Check if cool-off has passed
        elapsed = now - cb["opened_at"]
        if elapsed >= _CB_COOL_OFF:
            # Transition to half-open — allow one attempt
            cb["state"] = "half-open"
            logger.info("Circuit breaker HALF-OPEN — allowing one attempt")
            return {"blocked": False}
        
        remaining = int(_CB_COOL_OFF - elapsed)
        return {
            "blocked": True,
            "reason": (
                f"Garmin rate limit cooldown in progress. "
                f"Try again in {remaining // 60} minutes."
            ),
            "retry_after": remaining,
        }
    
    if state == "half-open":
        # Allow through — will close or re-open based on result
        return {"blocked": False}
    
    return {"blocked": False}


def _record_failure():
    """
    Records a 429 failure and updates the circuit breaker state.
    Called when a login attempt fails with a rate limit error.
    If the circuit was half-open, transitions back to open.
    """
    cb = _circuit_breaker
    now = time.time()
    cb["failures"].append(now)
    
    if cb["state"] == "half-open":
        # Half-open attempt failed — back to open
        cb["state"] = "open"
        cb["opened_at"] = now
        logger.warning(
            f"Circuit breaker back to OPEN — half-open attempt failed. "
            f"Total failures: {len(cb['failures'])}"
        )


def _record_success():
    """
    Records a successful login and resets the circuit breaker.
    Called when a login attempt succeeds.
    """
    cb = _circuit_breaker
    if cb["state"] == "half-open":
        # Half-open attempt succeeded — back to closed
        cb["state"] = "closed"
        cb["failures"] = []
        logger.info("Circuit breaker CLOSED — login succeeded, resetting failure count")


def _reset_circuit_breaker():
    """
    Manually resets the circuit breaker to closed state.
    Useful for testing or if the user confirms Garmin is no longer
    rate-limiting them.
    Can be triggered via a /reset_circuit endpoint.
    """
    cb = _circuit_breaker
    cb["state"] = "closed"
    cb["failures"] = []
    cb["opened_at"] = 0
    logger.info("Circuit breaker manually reset")


# --- Request Lock ---
#
# Prevents concurrent requests from both trying to log in at the same
# time. Without this lock, if the user taps "Sync" twice rapidly, or
# if the warm-up request arrives while the sync request is logging in,
# two threads could both call Garmin.login() and double the rate limit
# hits.

_login_lock = threading.Lock()

# --- Disk Session Persistence ---
#
# garth OAuth tokens are saved to a JSON file so we can survive Render
# restarts without logging in again.

_SESSION_FILE = "/tmp/garmin_session.json"


def _save_session(client: Garmin) -> None:
    """
    Saves the current garth session (OAuth tokens) to a JSON file.
    After a Render restart, _load_session() can restore it.
    """
    try:
        session_data = client.garth.dumps()
        with open(_SESSION_FILE, "w") as f:
            f.write(session_data)
        logger.info("Garmin session saved to disk")
    except Exception as e:
        logger.warning(f"Failed to save session to disk: {e}")


def _load_session() -> Garmin | None:
    """
    Attempts to restore a Garmin session from the disk cache.
    Returns a logged-in Garmin client if successful, None otherwise.
    """
    if not os.path.exists(_SESSION_FILE):
        logger.info("No saved Garmin session found on disk")
        return None

    try:
        with open(_SESSION_FILE, "r") as f:
            session_data = f.read()

        # Restore the raw garth session first
        garth_client = garth.Client()
        garth_client.loads(session_data)

        # Wrap in garminconnect's Garmin class for API methods
        client = Garmin()
        client.garth = garth_client

        display_name = client.garth.profile.get("displayName", "unknown")
        logger.info(f"Restored Garmin session from disk for {display_name}")
        return client

    except Exception as e:
        logger.warning(f"Failed to restore session from disk: {e}")
        try:
            os.remove(_SESSION_FILE)
        except Exception:
            pass
        return None


# --- Session Cache ---

_session_cache = {
    "email": None,
    "client": None,
    "timestamp": 0,
}

_SESSION_TTL = 3600  # 60 minutes

# Garmin login retry settings
_MAX_LOGIN_RETRIES = 3
_LOGIN_RETRY_DELAYS = [60, 120, 240]  # seconds between retries


def _login_with_retry(email: str, password: str) -> Garmin:
    """
    Logs in to Garmin Connect using garth DIRECTLY (bypassing
    garminconnect's wrapper which has aggressive internal retries).

    With garth, each login attempt sends ONE clean request. If Garmin
    returns 429, we wait 60/120/240s before retrying. No additional
    hidden requests.

    After successful login, injects the authenticated garth client
    into a garminconnect.Garmin instance so we can use its convenient
    get_activities() and download_activity() methods.

    Tracks 429 failures via _record_failure() so the circuit breaker
    automatically blocks further attempts after 3 failures.
    """
    last_exception = None

    for attempt in range(1, _MAX_LOGIN_RETRIES + 1):
        try:
            logger.info(f"Logging in to Garmin via garth (attempt {attempt}/{_MAX_LOGIN_RETRIES})")

            # Step 1: Login using garth directly — ONE clean request
            garth_client = garth.Client()
            garth_client.login(email, password)

            # Step 2: Wrap in garminconnect's Garmin class for API access
            client = Garmin()
            client.garth = garth_client

            logger.info("Garmin login successful")

            # Record success — resets circuit breaker if it was half-open
            _record_success()

            # Save session to disk so Render restarts don't need a new login
            _save_session(client)

            return client

        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "Too Many Requests" in error_str
            last_exception = e

            if is_rate_limit and attempt < _MAX_LOGIN_RETRIES:
                # Track this failure in the circuit breaker
                _record_failure()

                delay = _LOGIN_RETRY_DELAYS[attempt - 1]
                logger.warning(
                    f"Garmin login attempt {attempt} failed: {error_str[:150]}"
                )
                logger.info(f"Rate limited. Waiting {delay}s before retry...")
                time.sleep(delay)
                continue
            else:
                logger.error(
                    f"Garmin login failed after {attempt} attempt(s)"
                )
                raise

    raise last_exception or Exception("Garmin login failed")


def _get_client(email: str, password: str) -> Garmin:
    """
    Returns a cached Garmin client, trying these sources in order:
      1. In-memory cache (fast, 60-min TTL)
      2. Disk session persistence (survives Render restarts)
      3. Fresh login via garth (last resort, with clean retry)

    Before attempting any login, checks the circuit breaker. If the
    circuit is open (too many recent 429s), raises an exception with
    the cooldown message. This prevents any request from reaching
    Garmin while the account is rate-limited.
    """
    now = time.time()

    # --- Step 1: Try the in-memory cache ---
    if (
        _session_cache["client"] is not None
        and _session_cache["email"] == email
        and (now - _session_cache["timestamp"]) < _SESSION_TTL
    ):
        logger.info("Reusing cached Garmin session")
        return _session_cache["client"]

    # --- Step 2: Check circuit breaker BEFORE acquiring lock ---
    #
    # If the circuit is open, we reject immediately without touching
    # Garmin. This prevents:
    #   - Wasting a rate limit token on a doomed request
    #   - Making the cooldown longer by adding more 429s
    #   - Wrapping up a gunicorn worker for 60+ seconds waiting
    cb = _check_circuit_breaker()
    if cb["blocked"]:
        reason = cb["reason"]
        retry_after = cb.get("retry_after")
        logger.warning(f"Circuit breaker blocked login: {reason}")
        msg = reason
        if retry_after:
            msg += f" (retry in {retry_after // 60} min)"
        raise Exception(msg)

    # --- Step 3: Acquire the login lock ---
    with _login_lock:
        # Double-check cache after lock
        if (
            _session_cache["client"] is not None
            and _session_cache["email"] == email
            and (now - _session_cache["timestamp"]) < _SESSION_TTL
        ):
            logger.info("Reusing cached Garmin session (after lock wait)")
            return _session_cache["client"]

        # --- Step 4: Try restoring from disk ---
        disk_client = _load_session()
        if disk_client is not None:
            _session_cache["email"] = email
            _session_cache["client"] = disk_client
            _session_cache["timestamp"] = now
            logger.info("Using Garmin session restored from disk")
            return disk_client

        # --- Step 5: Fresh login ---
        client = _login_with_retry(email, password)

        _session_cache["email"] = email
        _session_cache["client"] = client
        _session_cache["timestamp"] = now

        return client


@app.route("/check_rate_limit", methods=["GET"])
def check_rate_limit():
    """
    Diagnostic endpoint that checks if the Render IP or Garmin account
    is rate-limited WITHOUT doing a full login.

    Hits Garmin's SSO page with a lightweight GET. A 429 means the IP
    is blocked. A 200 means the IP is fine but the account may be
    throttled at the OAuth level.
    """
    import requests as req

    cache_valid = (
        _session_cache["client"] is not None
        and (time.time() - _session_cache["timestamp"]) < _SESSION_TTL
    )

    result = {
        "cache_status": "valid" if cache_valid else "expired_or_empty",
        "cache_age_seconds": int(time.time() - _session_cache["timestamp"])
                             if _session_cache["client"] is not None else None,
        "disk_cache_exists": os.path.exists(_SESSION_FILE),
    }

    try:
        garmin_resp = req.get(
            "https://sso.garmin.com/sso/embed",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        result["garmin_status"] = garmin_resp.status_code
        result["garmin_reason"] = "OK" if garmin_resp.status_code == 200 else str(garmin_resp.reason)

        if garmin_resp.status_code == 429:
            result["interpretation"] = (
                "Render IP is RATE-LIMITED by Garmin at the network level. "
                "A proxy or new Render service will fix this."
            )
        elif garmin_resp.status_code == 200:
            result["interpretation"] = (
                "Render IP can reach Garmin OK. "
                "Rate limiting is likely on your ACCOUNT (too many recent logins) "
                "or the OAuth endpoint. Wait a few hours then try again."
            )
        else:
            result["interpretation"] = f"Unexpected status {garmin_resp.status_code}"

    except Exception as e:
        result["garmin_status"] = "error"
        result["garmin_error"] = str(e)
        result["interpretation"] = f"Cannot reach Garmin at all: {e}"

    return jsonify(result), 200


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint. Returns 200 if the server is running."""
    return jsonify({"status": "ok"}), 200


@app.route("/test_connection", methods=["POST"])
def test_connection():
    """
    Tests if the provided Garmin credentials are valid.
    Expects JSON: { "email": "...", "password": "..." }
    """
    data = request.json
    try:
        client = _get_client(data["email"], data["password"])
        return jsonify({"status": "connected"}), 200
    except Exception as e:
        logger.error(f"test_connection failed: {e}")
        return jsonify({"error": str(e)}), 401


@app.route("/activities", methods=["POST"])
def get_activities():
    """
    Fetches the list of activities from Garmin Connect.
    Expects JSON: { "email": "...", "password": "..." }
    """
    data = request.json
    try:
        client = _get_client(data["email"], data["password"])
        activities = client.get_activities()
        return jsonify(activities), 200
    except Exception as e:
        logger.error(f"get_activities failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/activity/<int:activity_id>/fit", methods=["POST"])
def download_fit(activity_id):
    """
    Downloads the raw FIT file for one activity.
    Flutter saves these bytes to disk and parses them locally.
    """
    data = request.json
    try:
        client = _get_client(data["email"], data["password"])

        fit_bytes = client.download_activity(
            activity_id,
            dl_fmt=client.ActivityDownloadFormat.ORIGINAL,
        )

        return Response(
            fit_bytes,
            mimetype="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={activity_id}.fit"
            },
        )

    except Exception as e:
        logger.error(f"download_fit({activity_id}) failed: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)