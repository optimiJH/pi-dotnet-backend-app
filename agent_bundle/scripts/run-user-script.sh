#!/usr/bin/env bash
set -e

# Explicitly go to the pi home directory instead of using ~
cd /home/jackwu/pi_project

# Run the Python script with full path
/usr/bin/python3 test_sensehat.py
