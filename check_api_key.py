"""
check_api_key.py
Quick script to verify your TMDB API key is configured correctly
Run this BEFORE running the full pipeline

Usage:
    python check_api_key.py
"""

import os
import sys
import requests

print("="*70)
print("TMDB API KEY VERIFICATION")
print("="*70)

# Step 1: Check if new_config.py exists
print("\n1. Checking for new_config.py...")
config_path = "new_config.py"

if not os.path.exists(config_path):
    print(f"   ✗ ERROR: {config_path} not found!")
    print(f"   → Create new_config.py in project root")
    sys.exit(1)
else:
    print(f"   ✓ Found {config_path}")

# Step 2: Load the config
print("\n2. Loading configuration...")
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location("new_config", config_path)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    
    API_KEY = config.TMDB_API_KEY
    print(f"   ✓ Config loaded successfully")
except Exception as e:
    print(f"   ✗ ERROR loading config: {e}")
    sys.exit(1)

# Step 3: Check API key format
print("\n3. Validating API key...")

if not API_KEY:
    print(f"   ✗ ERROR: API_KEY is empty!")
    print(f"   → Set TMDB_API_KEY in new_config.py")
    sys.exit(1)

if API_KEY == "YOUR_API_KEY_HERE" or API_KEY == "YOUR_TMDB_API_KEY_HERE":
    print(f"   ✗ ERROR: API_KEY is still the placeholder!")
    print(f"   → Replace with your actual TMDB API key")
    print(f"\n   Get your key from: https://www.themoviedb.org/settings/api")
    sys.exit(1)

if len(API_KEY) < 20:
    print(f"   ⚠ WARNING: API key seems too short ({len(API_KEY)} chars)")
    print(f"   → TMDB keys are usually 32 characters")

print(f"   ✓ API key format looks valid")
print(f"   → Key: {API_KEY[:8]}...{API_KEY[-4:]} ({len(API_KEY)} chars)")

# Step 4: Test actual API connection
print("\n4. Testing API connection...")

test_url = "https://api.themoviedb.org/3/movie/299534"
params = {'api_key': API_KEY}

try:
    response = requests.get(test_url, params=params, timeout=10)
    
    if response.status_code == 200:
        data = response.json()
        print(f"   ✓ API connection successful!")
        print(f"   → Test movie: {data.get('title', 'Unknown')}")
        print(f"   → Release: {data.get('release_date', 'Unknown')}")
    elif response.status_code == 401:
        print(f"   ✗ ERROR: Unauthorized (401)")
        print(f"   → Your API key is invalid or inactive")
        print(f"\n   Check:")
        print(f"   1. Key is copied correctly (no extra spaces)")
        print(f"   2. Key is active on TMDB website")
        print(f"   3. Get new key: https://www.themoviedb.org/settings/api")
        sys.exit(1)
    elif response.status_code == 404:
        print(f"   ✗ ERROR: Not found (404)")
        print(f"   → Check the API endpoint URL")
        sys.exit(1)
    else:
        print(f"   ✗ ERROR: Unexpected status code {response.status_code}")
        print(f"   → Response: {response.text[:200]}")
        sys.exit(1)
        
except requests.exceptions.RequestException as e:
    print(f"   ✗ ERROR: Connection failed")
    print(f"   → {str(e)}")
    print(f"\n   Check your internet connection")
    sys.exit(1)

# Step 5: Check movie IDs
print("\n5. Checking movie IDs configuration...")

if hasattr(config, 'MOVIE_IDS'):
    MOVIE_IDS = config.MOVIE_IDS
    print(f"   ✓ Found {len(MOVIE_IDS)} movie IDs to fetch")
    print(f"   → IDs: {MOVIE_IDS[:5]}...")
else:
    print(f"   ⚠ WARNING: MOVIE_IDS not found in config")

# Summary
print("\n" + "="*70)
print("✅ ALL CHECKS PASSED!")
print("="*70)
print("\nYour configuration is correct. You can now run:")
print("  python Scripts\\pipeline.py")
print("\nOR:")
print("  jupyter notebook TMDB_EDA.ipynb")
print("="*70)