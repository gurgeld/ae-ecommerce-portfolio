# ae-ecommerce-portfolio
E-commerce Operations Analytics



# 1. Install required packages
pip install -r requirements.txt

# 2. Run the ingestion pipeline
python src/elt/ingestor.py

# 3. Build dbt models
cd _dbt
dbt build
