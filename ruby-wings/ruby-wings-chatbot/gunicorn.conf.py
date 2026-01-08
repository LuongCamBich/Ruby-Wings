# gunicorn.conf.py
import os

# Workers & threads
workers = int(os.getenv("GUNICORN_WORKERS", "1"))
threads = int(os.getenv("GUNICORN_THREADS", "2"))

# Timeout (dùng GUNICORN_TIMEOUT, KHÔNG đụng TIMEOUT của app)
timeout = int(os.getenv("GUNICORN_TIMEOUT", "300"))

# Bind port (Render cấp)
bind = f"0.0.0.0:{os.getenv('PORT', '10000')}"

# Logging
loglevel = "info"
accesslog = "-"
errorlog = "-"

# Safety
graceful_timeout = timeout
keepalive = 5
