from flask import Flask, jsonify
import psutil
import time

app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check endpoint to monitor application health.
    Returns system performance metrics like CPU, memory, and uptime.
    """
    health_status = {
        "status": "healthy",
        "uptime": get_uptime(),
        "cpu_usage": psutil.cpu_percent(interval=1),
        "memory_usage": psutil.virtual_memory().percent,
        "disk_usage": psutil.disk_usage('/').percent,
    }
    return jsonify(health_status), 200

def get_uptime():
    """Calculate system uptime."""
    boot_time = psutil.boot_time()
    current_time = time.time()
    uptime_seconds = current_time - boot_time
    return convert_seconds_to_human_readable(uptime_seconds)

def convert_seconds_to_human_readable(seconds):
    """Convert seconds to a human-readable format."""
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"