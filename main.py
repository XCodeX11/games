import re
import time
import json
import random
import os
import asyncio
import aiohttp
from threading import Lock

A_FILE = "the_c.json"

A_UA = os.environ.get("Z_UA", "Mozilla/5.0")
try:
    A_SRCS = json.loads(os.environ.get("Z_SOURCES", "[]"))
except Exception:
    A_SRCS = []

A_HD = {"User-Agent": A_UA}
A_WK = 100  
A_TM = 5

f_lk = Lock()

st = {
    "c": 0,
    "b": 0,
    "f": 0,
    "s": 0
}
A_DISC = os.environ.get("Z_DISCORD_WEBHOOK", "")

async def notify_discord(success_count, total_count):
    if not A_DISC:
        return
    try:
        data = load_a()
        max_exp = 0
        now = int(time.time())
        
        for k, v in data.get("dynamic_state", {}).items():
            cookie_str = v.get("last_working_cookie", "")
            match = re.search(r"exp=(\d+)", cookie_str)
            if match:
                exp_val = int(match.group(1))
                if exp_val > max_exp:
                    max_exp = exp_val
                    
        time_status = "Unknown"
        if max_exp > now:
            time_status = f"VALID (expires in {fmt_t(max_exp - now)})"
        elif max_exp > 0:
            time_status = f"EXPIRED ({fmt_t(now - max_exp)} ago)"

        status_flag = "🟢 SUCCESS" if success_count == total_count else "🔴 ATTENTION"
        
        payload = {
            "username": "Matrix Synchronization Bot",
            "embeds": [{
                "title": f"{status_flag} - Sync Process Complete",
                "color": 3066993 if success_count == total_count else 15158332,
                "fields": [
                    {"name": "Execution Summary", "value": f"`{success_count} / {total_count}` successfully rotated.", "inline": True},
                    {"name": "Ecosystem Baseline", "value": f"Latest Window: `{time_status}`", "inline": False}
                ],
                "footer": {"text": f"Event Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"}
            }]
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(A_DISC, json=payload, headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=5)):
                pass
    except Exception:
        pass

def load_a():
    if not os.path.exists(A_FILE):
        exit(1)
    try:
        with open(A_FILE, "r") as f:
            return json.load(f)
    except Exception:
        exit(1)

def save_a(data):
    with f_lk:
        with open(A_FILE, "w") as f:
            json.dump(data, f, indent=2)

def build_x(k, data):
    tgt = data["initial_targets"].get(k)
    if not tgt:
        return None, None
        
    flg = data["flags"].get(k, {}).get("done", "NO")
    dyn = data["dynamic_state"].get(k, {})
    lst = dyn.get("last_working_cookie", "")
    
    base = tgt.split("?")[0]
    hd = A_HD.copy()

    if flg == "YES" and lst:
        return f"{base}?{lst}", hd
        
    return tgt, hd

async def gather_n(session):
    res = []
    ptrn = re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}\b')
    for src in A_SRCS:
        try:
            async with session.get(src, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    text = await r.text()
                    res.extend(ptrn.findall(text))
        except Exception:
            continue
    return list(dict.fromkeys(res))

async def exec_w(session, n, k, url, hd, rslv):
    if k in rslv:
        return {"status": "SKIPPED", "k": k}

    px = f"http://{n}"
    try:
        async with session.get(url, headers=hd, proxy=px, timeout=aiohttp.ClientTimeout(total=A_TM)) as resp:
            if resp.status == 200:
                ck = resp.headers.get("Set-Cookie")
                if ck:
                    return {"status": "SUCCESS", "k": k, "n": n, "ck": ck}
                return {"status": "NO_VAL", "k": k, "n": n}
            return {"status": "BLOCKED", "k": k, "n": n}
    except Exception:
        return {"status": "FAILED", "k": k, "n": n}

async def bounded_exec_w(session, sem, n, k, url, hd, rslv):
    async with sem:
        return await exec_w(session, n, k, url, hd, rslv)

def sync_m(k, ck):
    data = load_a()
    cln = ck.split(";")[0].strip()
    
    data["dynamic_state"][k] = {
        "last_working_cookie": cln,
        "updated_at": int(time.time())
    }
    data["flags"][k]["done"] = "YES"
    save_a(data)

def fmt_t(s):
    h, r = divmod(s, 3600)
    m, r_s = divmod(r, 60)
    parts = []
    if h > 0: parts.append(f"{h}h")
    if m > 0 or h > 0: parts.append(f"{m}m")
    parts.append(f"{r_s}s")
    return " ".join(parts)
    
async def main():
    data = load_a()
    async with aiohttp.ClientSession() as session:
        pool = await gather_n(session)

        if not pool:
            return

        active = {}
        for k in data["initial_targets"].keys():
            url, hd = build_x(k, data)
            if url:
                active[k] = {"url": url, "hd": hd}

        rslv = set()
        tasks = []
        sem = asyncio.Semaphore(A_WK)

        for k, info in active.items():
            for n in pool:
                tasks.append(asyncio.create_task(bounded_exec_w(session, sem, n, k, info["url"], info["hd"], rslv)))
                
        random.shuffle(tasks)
        total = len(tasks)

        for coro in asyncio.as_completed(tasks):
            if len(rslv) == len(active):
                for t in tasks:
                    if not t.done():
                        t.cancel()
                break

            try:
                res = await coro
                st["c"] += 1
                status = res["status"]
                k = res["k"]
                
                if status == "SUCCESS" and k not in rslv:
                    st["s"] += 1
                    rslv.add(k)
                    sync_m(k, res["ck"])

                pct = (st["c"] / total) * 100 if total > 0 else 0
                print(f"[{pct:.1f}%] Matrix Processing Loop -> Status Sync: {len(rslv)}/{len(active)} | Op Count: {st['c']}".ljust(85), end="\r")

            except Exception:
                continue

    print()
    await notify_discord(len(rslv), len(active))

if __name__ == "__main__":
    asyncio.run(main())
