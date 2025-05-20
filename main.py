from flask import Flask, request, jsonify, abort, send_file
from flask_cors import CORS
import os
import json

app = Flask(__name__)
CORS(app)  # Enable CORS if your Colab notebook is accessing remotely

# Define file paths
DATA_DIR = os.path.join(os.getcwd(), "datacenter")
ADDRESSES_FILE = os.path.join(os.getcwd(), "addresses", "address.json")
COOKIES_FILE = os.path.join(DATA_DIR, "cookies.json")
PROXIES_FILE = os.path.join(DATA_DIR, "proxies.json")
ACTIVESESSION_FILE = os.path.join(DATA_DIR, "activesession.json")
CHROMEDRIVER_FILE = os.path.join(os.getcwd(), "chromedriver")
def read_file(path):
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        try:
            return json.load(f)
        except Exception as e:
            print(f"Error reading {path}: {e}")
            return []

def write_file(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

@app.route("/addresses/address.json", methods=["GET", "PUT"])
def addresses():
    if request.method == "GET":
        return jsonify(read_file(ADDRESSES_FILE))
    elif request.method == "PUT":
        data = request.get_json()
        if data is None:
            abort(400, description="No JSON provided")
        write_file(ADDRESSES_FILE, data)
        return jsonify({"status": "success"})

@app.route("/datacenter/cookies.json", methods=["GET", "PUT"])
def cookies():
    if request.method == "GET":
        return jsonify(read_file(COOKIES_FILE))
    elif request.method == "PUT":
        data = request.get_json()
        if data is None:
            abort(400, description="No JSON provided")
        write_file(COOKIES_FILE, data)
        return jsonify({"status": "success"})

@app.route("/datacenter/proxies.json", methods=["GET", "PUT"])
def proxies():
    if request.method == "GET":
        return jsonify(read_file(PROXIES_FILE))
    elif request.method == "PUT":
        data = request.get_json()
        if data is None:
            abort(400, description="No JSON provided")
        write_file(PROXIES_FILE, data)
        return jsonify({"status": "success"})

@app.route("/datacenter/activesession.json", methods=["GET", "PUT"])
def activesession():
    if request.method == "GET":
        return jsonify(read_file(ACTIVESESSION_FILE))
    elif request.method == "PUT":
        data = request.get_json()
        if data is None:
            abort(400, description="No JSON provided")
        write_file(ACTIVESESSION_FILE, data)
        return jsonify({"status": "success"})
    
@app.route("/download/chromedriver", methods=["GET"])
def download_chromedriver():
    """
    Endpoint to download the chromedriver executable stored locally.
    """
    if not os.path.exists(CHROMEDRIVER_FILE):
        abort(404, description="Chromedriver file not found")
    return send_file(CHROMEDRIVER_FILE, as_attachment=True)

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs(os.path.join(os.getcwd(), "addresses"), exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    # Run server on port 8000
    app.run(host="0.0.0.0", port=8000, debug=True)
