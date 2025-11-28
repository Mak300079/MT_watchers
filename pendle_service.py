import threading
from fastapi import FastAPI
from PandleAssetsWatcher import run_forever  # use the exact filename

app = FastAPI(title="Pendle Watcher Service")

_stop = threading.Event()
_thread = None

@app.get("/health")
def health():
    return {"status": "ok"}

@app.on_event("startup")
def on_start():
    global _thread
    _stop.clear()
    _thread = threading.Thread(target=run_forever, args=(_stop,), daemon=True)
    _thread.start()

@app.on_event("shutdown")
def on_stop():
    _stop.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=10)
