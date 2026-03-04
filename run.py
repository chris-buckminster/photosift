#!/usr/bin/env python3
"""
PhotoSift - Launch Script
Usage: python run.py [--port 8787] [--no-browser]
"""
import argparse, sys, webbrowser, threading, time

def main():
    parser = argparse.ArgumentParser(description="PhotoSift - Smart Photo Organizer")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    url = f"http://{args.host}:{args.port}"

    if not args.no_browser:
        def open_browser():
            time.sleep(1.5)
            print(f"\n  ✦ PhotoSift is running at {url}")
            print(f"  ✦ Press Ctrl+C to stop\n")
            webbrowser.open(url)
        threading.Thread(target=open_browser, daemon=True).start()
    else:
        print(f"\n  ✦ PhotoSift is running at {url}\n  ✦ Press Ctrl+C to stop\n")

    import uvicorn
    uvicorn.run("backend.main:app", host=args.host, port=args.port, log_level="warning", reload=False)

if __name__ == "__main__":
    main()
