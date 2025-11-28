import threading
from fastapi import FastAPI
from PendleAssetsWatcher import run_forever
from PendleAssetsWatcher import notify 

app = FastAPI(title="Pendle Watcher Service")

_stop = threading.Event()
_thread = None

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/test-notify")
def test_notify():
    notify("Test message from Render")
    return {"sent" : True}

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
