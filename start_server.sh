#!/data/data/com.termux/files/usr/bin/bash
# Termux: keeps qx_server.py alive (restarts it after a crash). Run:  bash start_server.sh
termux-wake-lock 2>/dev/null
cd "$(dirname "$0")"
while true; do
  env -i HOME="$HOME" PATH="$PATH" PREFIX="$PREFIX" ANDROID_ROOT="$ANDROID_ROOT" ANDROID_DATA="$ANDROID_DATA" python qx_server.py
  echo "server stopped, restarting in 15 s ..."
  sleep 15
done
