#!/bin/bash

# Load variables from .env file
export $(grep -v '^#' .env | xargs)

docker run --name etl_db -e POSTGRES_PASSWORD=$DB_PASS -d -p 5432:5432 postgres

# Install dependencies from requirements.txt
pip install -r requirements.txt

# Check if the listener.py file exists
if [ ! -f "listener.py" ]; then
    echo "Error: listener.py not found."
    exit 1
fi

# Run the Python script
python -c 'import listener; listener.main()'

# Check if the etl.py file exists
if [ ! -f "etl.py" ]; then
    echo "Error: elt.py not found."
    exit 1
fi

# Call the etl script
python -c 'import etl; etl.main()'
