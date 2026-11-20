#!/usr/bin/env python3
"""Entry point: python run.py [--host H] [--port P] [--data-dir D]"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.server import create_app

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--data-dir", default="data")
    args = p.parse_args()
    app = create_app(data_dir=args.data_dir)
    app.run(host=args.host, port=args.port, threaded=True)
