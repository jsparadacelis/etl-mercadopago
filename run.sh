#!/bin/bash

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Please install Python3 and try again."
    exit 1
fi

# Check if the listener.py file exists
if [ ! -f "listener.py" ]; then
    echo "Error: listener.py not found."
    exit 1
fi

# Run the Python script
python3 -c 'import listener; listener.main()'

# Call the etl script
python3 -c 'import etl; etl.main()'
