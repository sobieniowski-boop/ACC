"""
OAuth2 flow to obtain Amazon Ads API refresh_token.

Usage:
    python scripts/get_ads_refresh_token.py

This will:
1. Open your browser to Amazon login page
2. Start a local HTTP server on port 9000 to catch the callback
3. Exchange the auth code for access_token + refresh_token
4. Print the refresh_token (save it to .env)
"""

import http.server
import urllib.parse
import webbrowser
import json
import sys

try:
    import requests
except ImportError:
    print("Installing requests...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

# ── ACC Ads LWA credentials (loaded from .env) ────────────────────────
import pathlib, re as _re
_env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
_env_vars: dict[str, str] = {}
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _m = _re.match(r'^([A-Z_]+)=(.*)$', _line.strip())
        if _m:
            _env_vars[_m.group(1)] = _m.group(2).strip('"').strip("'")

CLIENT_ID = _env_vars.get("AMAZON_ADS_CLIENT_ID", "")
CLIENT_SECRET = _env_vars.get("AMAZON_ADS_CLIENT_SECRET", "")
if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: AMAZON_ADS_CLIENT_ID / AMAZON_ADS_CLIENT_SECRET not found in .env")
    sys.exit(1)
REDIRECT_URI = "http://localhost:9000/callback"
SCOPES = "advertising::campaign_management"

# Amazon LWA endpoints
AUTH_URL = "https://www.amazon.com/ap/oa"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """Handles the OAuth callback from Amazon."""

    auth_code = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            OAuthCallbackHandler.auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>&#10004; Autoryzacja OK!</h1>"
                b"<p>Mozesz zamknac te karte i wrocic do VS Code.</p>"
                b"</body></html>"
            )
        elif "error" in params:
            error = params.get("error", ["?"])[0]
            desc = params.get("error_description", [""])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"<h1>Blad: {error}</h1><p>{desc}</p>".encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress HTTP logs


def main():
    # Step 1: Build authorization URL
    auth_params = urllib.parse.urlencode({
        "client_id": CLIENT_ID,
        "scope": SCOPES,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
    })
    auth_link = f"{AUTH_URL}?{auth_params}"

    print("=" * 60)
    print("  Amazon Ads API — OAuth2 Flow")
    print("=" * 60)
    print()
    print("Otwieram przegladarke z linkiem autoryzacyjnym...")
    print(f"  {auth_link}")
    print()
    print("Jesli przegladarka nie otworzy sie automatycznie,")
    print("skopiuj powyzszy link i wklej go recznie.")
    print()
    print("Czekam na callback na http://localhost:9000/callback ...")
    print()

    # Step 2: Start local server and open browser
    server = http.server.HTTPServer(("localhost", 9000), OAuthCallbackHandler)
    webbrowser.open(auth_link)

    # Wait for single request (the callback)
    while OAuthCallbackHandler.auth_code is None:
        server.handle_request()

    server.server_close()
    auth_code = OAuthCallbackHandler.auth_code
    print(f"Otrzymano auth code: {auth_code[:20]}...")

    # Step 3: Exchange auth code for tokens
    print("Wymieniam auth code na refresh_token...")
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    })

    if resp.status_code != 200:
        print(f"BLAD {resp.status_code}: {resp.text}")
        sys.exit(1)

    tokens = resp.json()
    refresh_token = tokens.get("refresh_token", "")
    access_token = tokens.get("access_token", "")

    print()
    print("=" * 60)
    print("  SUKCES! Tokeny uzyskane.")
    print("=" * 60)
    print()
    print(f"refresh_token = {refresh_token}")
    print()
    print("Dodaj do pliku .env:")
    print()
    print(f'AMAZON_ADS_CLIENT_ID={CLIENT_ID}')
    print(f'AMAZON_ADS_CLIENT_SECRET={CLIENT_SECRET}')
    print(f'AMAZON_ADS_REFRESH_TOKEN={refresh_token}')
    print(f'AMAZON_ADS_REGION=EU')
    print()

    # Also save to file for safety
    with open("ads_tokens.json", "w") as f:
        json.dump(tokens, f, indent=2)
    print("Tokeny zapisane takze w ads_tokens.json (backup)")


if __name__ == "__main__":
    main()
