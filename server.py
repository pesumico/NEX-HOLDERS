"""
NEX Holders Leaderboard — Backend Server
==========================================
Flask server with background data fetcher for the Nexus Testnet
NEX token holder leaderboard.
"""

import os
import json
import time
import threading
import re
import csv
import io
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict
from functools import wraps

import requests
from flask import Flask, jsonify, request, render_template, Response

# ── Configuration ──────────────────────────────────────────────────
V2_BASE = "https://nexus.testnet.blockscout.com/api/v2"
RPC_BASE = "https://nexus.testnet.blockscout.com/api"
CACHE_FILE = "holders_cache.json"
MIN_BALANCE_NEX = 100  # Only track holders with >= this many NEX
MIN_BALANCE_WEI = int(MIN_BALANCE_NEX * 10**18)  # 100 NEX in wei
REFRESH_INTERVAL = 600  # seconds (10 min — fetch only takes ~6 min now)
FETCH_DELAY = 0.05  # delay between API pages (minimal — API can handle it)
MAX_PAGES = 50000  # safety limit
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 120  # max requests per window

app = Flask(__name__, template_folder="templates", static_folder="static")

# ── Global State ───────────────────────────────────────────────────
data_lock = threading.RLock()
holders_data = {
    "holders": [],           # sorted by balance desc
    "address_index": {},     # address -> index in holders list
    "total_supply": 0,
    "total_holders": 0,
    "non_zero_holders": 0,
    "last_updated": None,
    "is_refreshing": False,
    "refresh_progress": 0,
    "chain_stats": {},
    "distribution": {},
}

# Rate limiting store
rate_limits = defaultdict(list)


# ── Helpers ────────────────────────────────────────────────────────
def validate_address(address):
    """Validate Ethereum address format."""
    if not address:
        return False
    return bool(re.match(r'^0x[0-9a-fA-F]{40}$', address))


def rate_limit(f):
    """Simple rate limiting decorator."""
    @wraps(f)
    def decorated(*args, **kwargs):
        ip = request.remote_addr
        now = time.time()
        # Clean old entries
        rate_limits[ip] = [t for t in rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]
        if len(rate_limits[ip]) >= RATE_LIMIT_MAX:
            return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
        rate_limits[ip].append(now)
        return f(*args, **kwargs)
    return decorated


def clean_params(params):
    """Fix None values in pagination params."""
    if not params:
        return params
    return {k: (0 if v is None else v) for k, v in params.items()}


def wei_to_nex(wei_str):
    """Convert wei string to NEX float."""
    try:
        return float(Decimal(int(wei_str)) / Decimal(10**18))
    except:
        return 0.0


def compute_distribution(holders):
    """Compute holder distribution buckets (>= 100 NEX only)."""
    buckets = {
        "100-500 NEX": 0,
        "500-1K NEX": 0,
        "1K-5K NEX": 0,
        "5K-10K NEX": 0,
        "10K-50K NEX": 0,
        "50K-100K NEX": 0,
        "100K-1M NEX": 0,
        "1M+ NEX": 0,
    }

    for h in holders:
        bal = wei_to_nex(h.get("balance_wei", "0"))
        if bal < 500:
            buckets["100-500 NEX"] += 1
        elif bal < 1000:
            buckets["500-1K NEX"] += 1
        elif bal < 5000:
            buckets["1K-5K NEX"] += 1
        elif bal < 10000:
            buckets["5K-10K NEX"] += 1
        elif bal < 50000:
            buckets["10K-50K NEX"] += 1
        elif bal < 100000:
            buckets["50K-100K NEX"] += 1
        elif bal < 1000000:
            buckets["100K-1M NEX"] += 1
        else:
            buckets["1M+ NEX"] += 1

    return buckets


# ── Data Fetcher ───────────────────────────────────────────────────
def get_chain_stats():
    """Fetch chain statistics."""
    try:
        r = requests.get(f"{V2_BASE}/stats", timeout=15)
        return r.json()
    except:
        return {}


def _commit_holders(holders, stats):
    """Sort and commit holders to global state (thread-safe)."""
    sorted_holders = sorted(
        holders,
        key=lambda x: int(x.get("balance_wei", "0") or "0"),
        reverse=True
    )

    index = {}
    non_zero = 0
    total_supply = 0
    for i, h in enumerate(sorted_holders):
        index[h["address"].lower()] = i
        bal = int(h.get("balance_wei", "0") or "0")
        if bal > 0:
            non_zero += 1
            total_supply += bal

    distribution = compute_distribution(sorted_holders)

    with data_lock:
        holders_data["holders"] = sorted_holders
        holders_data["address_index"] = index
        holders_data["total_supply"] = total_supply
        holders_data["total_holders"] = len(sorted_holders)
        holders_data["non_zero_holders"] = non_zero
        holders_data["last_updated"] = datetime.now().isoformat()
        holders_data["chain_stats"] = stats
        holders_data["distribution"] = distribution


def save_cache(holders, stats):
    """Save holder data to JSON cache file."""
    try:
        cache = {
            "holders": holders,
            "stats": stats,
            "updated": datetime.now().isoformat(),
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        print(f"Cache save error: {e}")


def load_cache():
    """Load holder data from cache if available, filtering by MIN_BALANCE."""
    if not os.path.exists(CACHE_FILE):
        return False
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        holders = cache.get("holders", [])
        stats = cache.get("stats", {})
        # Filter out addresses below minimum balance
        before = len(holders)
        holders = [h for h in holders if int(h.get("balance_wei", "0") or "0") >= MIN_BALANCE_WEI]
        if holders:
            print(f"  Loaded {len(holders):,} holders from cache (filtered from {before:,}, min {MIN_BALANCE_NEX} NEX)")
            _commit_holders(holders, stats)
            return True
    except Exception as e:
        print(f"Cache load error: {e}")
    return False


def refresh_data():
    """
    Background data refresh.
    
    IMPORTANT: Does NOT touch the live global data during fetch.
    Builds a new dataset in the background. Only swaps it in at the
    end if the new dataset has >= the current holder count.
    This ensures users always see the best available data.
    """
    with data_lock:
        if holders_data["is_refreshing"]:
            return
        holders_data["is_refreshing"] = True
        holders_data["refresh_progress"] = 0
        current_count = holders_data["total_holders"]

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting background refresh (current: {current_count:,} holders)...")

    stats = get_chain_stats()
    # Update chain stats immediately (lightweight)
    with data_lock:
        if stats:
            holders_data["chain_stats"] = stats

    new_holders = []
    seen = set()
    params = None
    page = 0
    stale = 0

    while page < MAX_PAGES:
        page += 1
        try:
            if params:
                r = requests.get(f"{V2_BASE}/addresses",
                                 params=clean_params(params), timeout=30)
            else:
                r = requests.get(f"{V2_BASE}/addresses", timeout=30)

            if r.status_code != 200:
                time.sleep(2)
                continue

            data = r.json()
            items = data.get("items", [])
            np = data.get("next_page_params")
        except Exception:
            time.sleep(2)
            continue

        if not items:
            break

        new_count = 0
        below_threshold = 0
        for item in items:
            addr = item["hash"].lower()
            if addr not in seen:
                seen.add(addr)
                balance_wei = item.get("coin_balance", "0") or "0"
                bal_int = int(balance_wei)
                if bal_int < MIN_BALANCE_WEI:
                    below_threshold += 1
                    continue  # Skip addresses with < 100 NEX
                new_count += 1
                new_holders.append({
                    "address": item["hash"],
                    "balance_wei": balance_wei,
                    "tx_count": item.get("tx_count", 0) or 0,
                    "is_contract": item.get("is_contract", False),
                    "name": item.get("ens_domain_name") or item.get("name") or "",
                })

        # API returns sorted by balance desc — if ALL items on this page
        # are below threshold, all subsequent pages will be too. Stop early.
        if below_threshold == len(items):
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] All items below {MIN_BALANCE_NEX} NEX on page {page}, stopping.")
            break

        # Update progress counter only (don't touch holder data)
        if page % 20 == 0:
            with data_lock:
                holders_data["refresh_progress"] = len(new_holders)
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Fetched {len(new_holders):,} (page {page})")

        if new_count == 0:
            stale += 1
            if stale >= 3:
                break
        else:
            stale = 0

        if not np:
            break

        params = np
        time.sleep(FETCH_DELAY)

    # Only swap in new data if it's better than what we have
    with data_lock:
        if len(new_holders) >= current_count:
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Swapping in {len(new_holders):,} holders (was {current_count:,})")
            _commit_holders(new_holders, stats)
            save_cache(new_holders, stats)
        else:
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Keeping existing {current_count:,} holders (new fetch only got {len(new_holders):,})")
        holders_data["is_refreshing"] = False

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Refresh complete.")


def background_refresh_loop():
    """Background thread that periodically refreshes data."""
    while True:
        try:
            refresh_data()
        except Exception as e:
            print(f"Refresh error: {e}")
            with data_lock:
                holders_data["is_refreshing"] = False
        time.sleep(REFRESH_INTERVAL)


# ── API Routes ─────────────────────────────────────────────────────
@app.route("/")
def index():
    """Serve the frontend."""
    return render_template("index.html")


@app.route("/api/stats")
@rate_limit
def api_stats():
    """Global statistics."""
    with data_lock:
        top10 = []
        for h in holders_data["holders"][:10]:
            top10.append({
                "address": h["address"],
                "balance": wei_to_nex(h["balance_wei"]),
                "name": h.get("name", ""),
            })

        return jsonify({
            "total_holders": holders_data["total_holders"],
            "non_zero_holders": holders_data["non_zero_holders"],
            "total_supply": wei_to_nex(str(holders_data["total_supply"])),
            "last_updated": holders_data["last_updated"],
            "is_refreshing": holders_data["is_refreshing"],
            "refresh_progress": holders_data["refresh_progress"],
            "chain_stats": {
                "total_addresses": holders_data["chain_stats"].get("total_addresses", "N/A"),
                "total_blocks": holders_data["chain_stats"].get("total_blocks", "N/A"),
                "total_transactions": holders_data["chain_stats"].get("total_transactions", "N/A"),
                "average_block_time": holders_data["chain_stats"].get("average_block_time", "N/A"),
                "network_utilization": holders_data["chain_stats"].get("network_utilization_percentage", 0),
            },
            "top10": top10,
        })


@app.route("/api/search")
@rate_limit
def api_search():
    """Search for an address — returns rank, balance, percentile, context."""
    address = request.args.get("address", "").strip()

    if not validate_address(address):
        return jsonify({"error": "Invalid address format. Must be 0x followed by 40 hex chars."}), 400

    addr_lower = address.lower()

    with data_lock:
        idx = holders_data["address_index"].get(addr_lower)
        total = holders_data["total_holders"]
        total_supply = holders_data["total_supply"]

        if idx is not None:
            holder = holders_data["holders"][idx]
            rank = idx + 1
            balance_wei = int(holder.get("balance_wei", "0") or "0")
            balance_nex = wei_to_nex(str(balance_wei))
            percentile = round((1 - rank / total) * 100, 2) if total > 0 else 0
            pct_supply = round(balance_wei / total_supply * 100, 6) if total_supply > 0 else 0

            # Get surrounding holders for context
            context_start = max(0, idx - 3)
            context_end = min(total, idx + 4)
            nearby = []
            for i in range(context_start, context_end):
                h = holders_data["holders"][i]
                nearby.append({
                    "rank": i + 1,
                    "address": h["address"],
                    "balance": wei_to_nex(h["balance_wei"]),
                    "is_current": i == idx,
                })

            # Average holder balance
            avg_balance = wei_to_nex(str(total_supply)) / total if total > 0 else 0

            return jsonify({
                "found": True,
                "address": holder["address"],
                "rank": rank,
                "balance": balance_nex,
                "balance_wei": str(balance_wei),
                "percentile": percentile,
                "pct_supply": pct_supply,
                "tx_count": holder.get("tx_count", 0),
                "is_contract": holder.get("is_contract", False),
                "name": holder.get("name", ""),
                "total_holders": total,
                "nearby": nearby,
                "avg_balance": round(avg_balance, 4),
                "vs_average": round(balance_nex / avg_balance, 2) if avg_balance > 0 else 0,
            })
        else:
            # Not in our dataset — try fetching live balance
            try:
                r = requests.get(RPC_BASE, params={
                    "module": "account",
                    "action": "balance",
                    "address": address,
                }, timeout=10)
                result = r.json().get("result", "0")
                live_balance = wei_to_nex(result)

                return jsonify({
                    "found": False,
                    "address": address,
                    "live_balance": live_balance,
                    "message": "Address not found in cached dataset. Live balance shown above.",
                    "total_holders": total,
                })
            except:
                return jsonify({
                    "found": False,
                    "address": address,
                    "message": "Address not found in cached dataset.",
                    "total_holders": total,
                })


@app.route("/api/leaderboard")
@rate_limit
def api_leaderboard():
    """Paginated leaderboard."""
    try:
        page = max(1, int(request.args.get("page", 1)))
        limit = min(100, max(1, int(request.args.get("limit", 50))))
    except ValueError:
        page, limit = 1, 50

    search_q = request.args.get("q", "").strip().lower()

    with data_lock:
        total = holders_data["total_holders"]
        total_supply = holders_data["total_supply"]

        if search_q:
            # Search mode — find matching addresses
            results = []
            for i, h in enumerate(holders_data["holders"]):
                if search_q in h["address"].lower() or search_q in (h.get("name") or "").lower():
                    bal_wei = int(h.get("balance_wei", "0") or "0")
                    results.append({
                        "rank": i + 1,
                        "address": h["address"],
                        "balance": wei_to_nex(h["balance_wei"]),
                        "tx_count": h.get("tx_count", 0),
                        "is_contract": h.get("is_contract", False),
                        "name": h.get("name", ""),
                        "pct_supply": round(bal_wei / total_supply * 100, 6) if total_supply > 0 else 0,
                    })
                    if len(results) >= limit:
                        break
            return jsonify({
                "items": results,
                "total": len(results),
                "page": 1,
                "limit": limit,
                "pages": 1,
                "search": search_q,
            })

        # Normal pagination
        start = (page - 1) * limit
        end = start + limit
        items = []
        for i in range(start, min(end, total)):
            h = holders_data["holders"][i]
            bal_wei = int(h.get("balance_wei", "0") or "0")
            items.append({
                "rank": i + 1,
                "address": h["address"],
                "balance": wei_to_nex(h["balance_wei"]),
                "tx_count": h.get("tx_count", 0),
                "is_contract": h.get("is_contract", False),
                "name": h.get("name", ""),
                "pct_supply": round(bal_wei / total_supply * 100, 6) if total_supply > 0 else 0,
            })

        pages = (total + limit - 1) // limit if total > 0 else 1

        return jsonify({
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": pages,
        })


@app.route("/api/distribution")
@rate_limit
def api_distribution():
    """Holder distribution data."""
    with data_lock:
        return jsonify({
            "distribution": holders_data["distribution"],
            "total_holders": holders_data["total_holders"],
        })


@app.route("/api/transactions")
@rate_limit
def api_transactions():
    """Recent transactions for an address (proxied from Blockscout)."""
    address = request.args.get("address", "").strip()
    if not validate_address(address):
        return jsonify({"error": "Invalid address"}), 400

    try:
        r = requests.get(
            f"{V2_BASE}/addresses/{address}/transactions",
            params={"type": "all"},
            timeout=15
        )
        if r.status_code != 200:
            return jsonify({"transactions": [], "error": "API error"}), 200

        data = r.json()
        items = data.get("items", [])

        txns = []
        for tx in items[:20]:  # last 20 transactions
            txns.append({
                "hash": tx.get("hash", ""),
                "block": tx.get("block_number", 0),
                "from": tx.get("from", {}).get("hash", ""),
                "to": (tx.get("to") or {}).get("hash", ""),
                "value": wei_to_nex(tx.get("value", "0")),
                "fee": wei_to_nex(tx.get("fee", {}).get("value", "0") if isinstance(tx.get("fee"), dict) else tx.get("fee", "0")),
                "timestamp": tx.get("timestamp", ""),
                "method": tx.get("method", ""),
                "status": tx.get("status", ""),
                "type": tx.get("tx_types", [""])[0] if tx.get("tx_types") else "",
            })

        return jsonify({"transactions": txns})
    except Exception as e:
        return jsonify({"transactions": [], "error": str(e)}), 200


@app.route("/api/export")
@rate_limit
def api_export():
    """Export leaderboard to CSV."""
    with data_lock:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Rank", "Address", "Balance (NEX)", "Balance (Wei)", "Tx Count", "Is Contract", "% of Supply"])

        total_supply = holders_data["total_supply"]
        for i, h in enumerate(holders_data["holders"]):
            bal_wei = int(h.get("balance_wei", "0") or "0")
            bal_nex = wei_to_nex(h["balance_wei"])
            pct = round(bal_wei / total_supply * 100, 6) if total_supply > 0 else 0
            writer.writerow([
                i + 1,
                h["address"],
                f"{bal_nex:.4f}",
                str(bal_wei),
                h.get("tx_count", 0),
                "Yes" if h.get("is_contract") else "No",
                f"{pct:.6f}%",
            ])

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=NEX_Holders_{datetime.now().strftime('%Y%m%d')}.csv"}
        )


@app.route("/api/refresh", methods=["POST"])
@rate_limit
def api_refresh():
    """Trigger manual data refresh."""
    with data_lock:
        if holders_data["is_refreshing"]:
            return jsonify({"message": "Refresh already in progress.", "progress": holders_data["refresh_progress"]})

    thread = threading.Thread(target=refresh_data, daemon=True)
    thread.start()
    return jsonify({"message": "Refresh started."})


# ── Startup Init (runs on import — needed for gunicorn) ────────────
print("=" * 60)
print("  NEX Holders Leaderboard Server")
print("  Nexus Testnet")
print("=" * 60)

# Try loading cache first
print("\n[1] Loading cache...")
_cached = load_cache()
if not _cached:
    print("  No cache found. Will fetch data on first load.")

# Start background refresh thread
print("[2] Starting background refresh thread...")
_bg_thread = threading.Thread(target=background_refresh_loop, daemon=True)
_bg_thread.start()


# ── Main (local dev only) ──────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[3] Starting Flask server on http://localhost:{port}")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
