## Steps to Run

python3 -m venv .venv

source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

## Install dependencies
pip install -r requirements.txt

# Create DB
python db.py

# start Ingestion(keep Running)
python ingest.py

# Run analytics in a new terminal
python analytics.py



