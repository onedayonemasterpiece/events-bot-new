#!/usr/bin/env python3
import sys
from run_transport_schedule_kaggle import main

if __name__ == "__main__":
    raise SystemExit(main(["--provider", "bus", *sys.argv[1:]]))
