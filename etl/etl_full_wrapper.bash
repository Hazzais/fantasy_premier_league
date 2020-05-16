#!/bin/bash

cd "$(dirname "$0")"

export PYTHONPATH=$PYTHONPATH:..

echo "Performing ETL on fpl data..."

python run_extract.py --log-file logs/extract.log
if [[ $? -ne 0 ]]; then
  echo "Error in extract. Cancelling."
  exit 1
else
  echo "Extract complete"
fi

python run_transform.py --log-file logs/transform.log
if [[ $? -ne 0 ]]; then
  echo "Error in transform. Cancelling."
  exit 1
else
  echo "Transform complete"
fi

python run_load.py localhost 5432 fpl harry db_fpl --log-file logs/load.log
if [[ $? -ne 0 ]]; then
  echo "Error in load. Cancelling."
  exit 1
else
  echo "Load complete"
fi

echo "...ETL on rpl data complete"
