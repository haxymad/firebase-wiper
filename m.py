#!/usr/bin/env python3
"""
Firebase Realtime Database multi-threaded recursive wiper.
- Deletes everything using many small requests.
- Automatically drills down (using ?shallow=true) when a node is too large.
"""

import argparse
import json
import queue
import threading
import time
from typing import Optional, Dict, Any, Tuple
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("[ERROR] The 'requests' library is not installed. Please run: pip install requests")
    exit(1)

# ---------- Helper Functions ----------

def build_url(base_url: str, path: str, suffix: str = ".json", shallow: bool = False) -> str:
    if path:
        safe_segments = [quote(seg, safe='') for seg in path.strip('/').split('/')]
        url = f"{base_url.rstrip('/')}/{'/'.join(safe_segments)}{suffix}"
    else:
        url = f"{base_url.rstrip('/')}/{suffix}"
    if shallow:
        url += "?shallow=true"
    return url

def is_size_error(resp: requests.Response) -> bool:
    try:
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            return "exceeds the maximum size" in data["error"]
    except Exception:
        pass
    return False

# ---------- Main Wiper Class ----------

class Wiper:
    def __init__(self, base_url: str, max_workers: int):
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        self.tasks = queue.Queue()
        self.active_tasks = 0
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.stats = {'deleted': 0, 'failed': 0, 'drilled': 0}

    def log(self, msg: str):
        print(msg, flush=True)

    def update_stat(self, key, value=1):
        with self.lock:
            self.stats[key] += value

    def worker(self):
        while not self.stop_event.is_set():
            try:
                path = self.tasks.get(timeout=0.5)
                with self.lock:
                    self.active_tasks += 1
            except queue.Empty:
                with self.lock:
                    if self.active_tasks == 0 and self.tasks.empty():
                        break
                continue
            
            try:
                # 1. Try to delete the path
                delete_url = build_url(self.base_url, path)
                resp = self.session.delete(delete_url, timeout=30)

                if resp.ok:
                    self.update_stat('deleted')
                elif is_size_error(resp):
                    self.update_stat('drilled')
                    # 2. If too big, get its children (shallow)
                    shallow_url = build_url(self.base_url, path, shallow=True)
                    child_resp = self.session.get(shallow_url, timeout=20)
                    if child_resp.ok:
                        children = child_resp.json()
                        if children and isinstance(children, dict):
                            for key in children.keys():
                                child_path = f"{path}/{key}" if path else key
                                self.tasks.put(child_path)
                        else:
                             self.log(f"[ERROR] Cannot delete node '{path}': Too large and has no children to drill into.")
                             self.update_stat('failed')
                    else:
                        self.log(f"[ERROR] Failed to get shallow keys for '{path}': {child_resp.status_code}")
                        self.update_stat('failed')
                else:
                    self.log(f"[ERROR] Failed to delete '{path}': {resp.status_code} - {resp.text[:100]}")
                    self.update_stat('failed')
            except Exception as e:
                self.log(f"[ERROR] Exception while processing '{path}': {e}")
                self.update_stat('failed')
            finally:
                with self.lock:
                    self.active_tasks -= 1
                self.tasks.task_done()

    def run(self):
        self.log("--- Starting Firebase RTDB Recursive Wiper ---")
        
        initial_keys_url = build_url(self.base_url, "", shallow=True)
        try:
            resp = self.session.get(initial_keys_url)
            if not resp.ok:
                self.log(f"[FATAL] Could not get top-level keys. Check URL. Error: {resp.text}")
                return
            initial_keys = resp.json()
            if not initial_keys:
                self.log("Database is already empty.")
                return
            for key in initial_keys.keys():
                self.tasks.put(key)
        except Exception as e:
            self.log(f"[FATAL] Could not connect to Firebase: {e}")
            return

        threads = []
        for _ in range(self.max_workers):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()
            threads.append(t)
            
        while any(t.is_alive() for t in threads):
            with self.lock:
                qsize = self.tasks.qsize()
                self.log(f"[PROGRESS] Deleted: {self.stats['deleted']} | Drilled: {self.stats['drilled']} | Failed: {self.stats['failed']} | Queue: {qsize} | Active: {self.active_tasks}")
            time.sleep(2)

        self.stop_event.set()
        for t in threads:
            t.join()
            
        self.log("\n--- Wipe complete ---")
        self.log(f"Final Stats -> Deleted: {self.stats['deleted']} | Drilled: {self.stats['drilled']} | Failed: {self.stats['failed']}")

def main():
    parser = argparse.ArgumentParser(description="Multi-threaded recursive Firebase RTDB wiper.")
    parser.add_argument("--base-url", required=True, help="Base RTDB URL, e.g., https://<project>-default-rtdb.firebaseio.com")
    parser.add_argument("--max-workers", type=int, default=50, help="Number of worker threads (default: 50)")
    args = parser.parse_args()

    wiper = Wiper(base_url=args.base_url, max_workers=args.max_workers)
    wiper.run()

if __name__ == "__main__":
    main()
