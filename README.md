# incwo-tools
Incwo tools

This tool identifies and disable duplicate products from Incwo based on their reference number. It keeps the most recently modified product and disable older duplicates.

It identifies also empty reference number incwo.

It can also disable article already removed from pepinierux.

## Setup

```bash
cd src
python3 -m venv venv
. venv/bin/activate
pip install requests mysql-connector-python
```

## Usage

### 1. Fetch Data and Generate CSV
First, download the latest product data and convert it to a CSV file.

```bash
# If you have credentials and want to download fresh data:
export INCWO_AUTH="your_email:your_password"
python3 rundict.py --download

# OR if you already have XML files in the 'out' directory:
python3 incwo_products.py --csv
```


This will create `incwo_data.csv`.

### 2. Find and Remove Duplicates
Run the cleanup script. It reads `incwo_data.csv`.

```bash
# Real deletion (requires INCWO_AUTH):
export INCWO_AUTH="your_email:your_password"

# Dry run (simulate only, safe to run):
python rundict.py --disable-duplicates --dry-run
python rundict.py --disable-empty-reference --dry-run
python rundict.py --disable-factux-removed --dry-run
```
