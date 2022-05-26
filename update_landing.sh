#! /bin/bash

BASEDIR=./

# running opendatabcn collector script for accidents
echo "Checking for accidents updates from opendataBCN..."
python opendata_collect.py accidents

# running opendatabcn collector script for lloguer preu
echo "Checking for lloguer preu updates from opendataBCN..."
python opendata_collect.py lloguer_preu