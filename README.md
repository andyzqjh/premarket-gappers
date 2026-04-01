# Premarket Gappers Dashboard

Local Flask dashboard for:

- premarket gappers
- watchlist-by-sector tabs
- AI ticker reasoning
- scheduled Singapore scans at `8:30 AM` and `8:30 PM`
- Obsidian exports
- a `Market Brief` tab for macro and sector summaries

## Open It Locally

1. Open a terminal in this folder.
2. Install dependencies:

```powershell
pip install -r requirements.txt
pip install "finvizfinance @ git+https://github.com/lit26/finvizfinance.git"
```

3. Copy `.env.example` to `.env` and fill in your keys.
4. Start the app:

```powershell
python app.py
```

5. Open:

`http://127.0.0.1:5000`

## Use It From Home

Best option: keep the office desktop on and use Tailscale.

1. Install Tailscale on the office desktop and your home machine.
2. In `.env`, set:

```env
APP_HOST=0.0.0.0
APP_PORT=5000
```

3. Start the app again with `python app.py`.
4. From home, open:

`http://<office-tailscale-ip>:5000`

Do not expose port `5000` directly to the public internet.

## Notes

- The app currently prefers `OpenRouter` first.
- Default OpenRouter model is `openai/gpt-5.4-mini`.
- Scheduled scans run in `Asia/Singapore`.
