#!/usr/bin/env python3

# This script is used to simulate the Renoir and check the environment variables

import os

RENOIR_HOST_ID = os.environ.get('RENOIR_HOST_ID', 'host id not set')
RENOIR_CONFIG = os.environ.get('RENOIR_CONFIG', 'config not set')
RENOIR_DISTRIBUTED_CONFIG = os.environ.get('RENOIR_DISTRIBUTED_CONFIG', 'distributed config not set')

print(f"RENOIR_HOST_ID: {RENOIR_HOST_ID}")
print(f"RENOIR_CONFIG: {RENOIR_CONFIG}")
print(f"RENOIR_DISTRIBUTED_CONFIG: {RENOIR_DISTRIBUTED_CONFIG}")