#!/bin/bash

# Move to the script's directory (project root)
cd "$(dirname "$0")"

echo "[1/4] Starting ngrok tunnel on port 8000..."
# Kill any existing ngrok instances to avoid port 4040 conflicts
killall ngrok 2>/dev/null
sleep 1

ngrok http 8000 > /dev/null &
NGROK_PID=$!

echo "Waiting for ngrok to provision HTTPS URL..."
sleep 4

PUBLIC_URL=$(curl -s http://127.0.0.1:4040/api/tunnels | grep -o 'https://[a-zA-Z0-9.\-]*ngrok-free.app' | head -n 1)

if [ -z "$PUBLIC_URL" ]; then
    echo "ERROR: Could not retrieve ngrok URL. Make sure ngrok is installed and authenticated."
    kill $NGROK_PID
    exit 1
fi

echo "✅ Secured Remote URL: $PUBLIC_URL"

echo "[2/4] Updating .env file..."
if grep -q "^WEB_APP_URL=" .env; then
    sed -i "s|^WEB_APP_URL=.*|WEB_APP_URL=$PUBLIC_URL|g" .env
else
    echo "WEB_APP_URL=$PUBLIC_URL" >> .env
fi
export WEB_APP_URL=$PUBLIC_URL

echo "[3/4] Starting FastAPI Web App (uvicorn)..."
# Force global system python so it doesn't break if you have another project's venv active in the terminal
/usr/bin/python3 -m uvicorn app.web_app:app --host 127.0.0.1 --port 8000 &
UVICORN_PID=$!

echo "[4/4] Starting Telegram Bot Orchestrator..."
pkill -9 -f "telegram_bot.py" || true
"$(pwd)/../../.venv/bin/python" app/tools/telegram_bot.py &
BOT_PID=$!

echo ""
echo "=================================================================="
echo "   🚀 PERSONAL RESEARCH AGENT LIVES! "
echo "   🌐 Dashboard Endpoint: $PUBLIC_URL"
echo "   💬 Bot Status: ONLINE"
echo "=================================================================="
echo "Leave this terminal open. Press [Ctrl+C] to gracefully stop everything."

# Trap SIGINT to kill background processes on exit
trap "echo -e '\nShutting down processes...'; kill $NGROK_PID $UVICORN_PID $BOT_PID 2>/dev/null; exit 0" INT TERM

# Wait for children to keep script running
wait $BOT_PID $UVICORN_PID $NGROK_PID
