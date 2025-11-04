# ae-ecommerce-portfolio
E-commerce Operations Analytics

# Dataset Source
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)

![Model](https://imgur.com/HRhd2Y0)

# Streamlit App
[Dashboard](https://ae-ecommerce-portfolio.streamlit.app/)

# 1. Install required packages
pip install -r requirements.txt

# 2. Run the ingestion pipeline
python src/elt/ingestor.py

# 3. Build dbt models
cd _dbt
dbt build
