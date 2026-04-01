import pandas as pd
import logging
import re

def check_python(text):
    if pd.isna(text):
        return False
    return bool(re.search(r'\bpython\b', text, re.IGNORECASE))

def check_ai(text):
    if pd.isna(text):
        return False
    return bool(re.search(r'\b(ai|ml|machine learning|artificial intelligence)\b', text, re.IGNORECASE))

def check_remote(text):
    if pd.isna(text):
        return False
    keywords = ["remote", "work from home", "home office"]
    text = text.lower()
    return any(k in text for k in keywords)

def extract_location(loc):
    if isinstance(loc, dict):
        area = loc.get('area')
        if isinstance(area, list):
            return ', '.join(area)
    return None

def extract_category(cat):
    if isinstance(cat, dict):
        return cat.get('label')
    return None

def extract_company(comp):
    if isinstance(comp, dict):
        return comp.get('display_name')
    return None

def transform(data, etl_id):
    result_data = data.copy()
    expected_cols = [
        'description', 'category', 'location', 'company',
        'salary_min', 'salary_max', 'salary_is_predicted'
    ]

    for col in expected_cols:
        if col not in result_data.columns:
            result_data[col] = None

    result_data['is_remote'] = result_data['description'].apply(check_remote)
    result_data['has_python'] = result_data['description'].apply(check_python)
    result_data['has_ai'] = result_data['description'].apply(check_ai)
    result_data['description_length'] = result_data['description'].str.len()

    result_data['job_label'] = result_data['category'].apply(extract_category)
    result_data['location_name'] = result_data['location'].apply(extract_location)
    result_data['company_name'] = result_data['company'].apply(extract_company)

    result_data['etl_id'] = etl_id
    result_data['load_timestamp'] = pd.Timestamp.now()

    cols_to_drop = ['__CLASS__', 'location', 'company']

    result_data = result_data.drop(columns=cols_to_drop, errors='ignore')

    if 'id' in result_data.columns:
        result_data = result_data.drop_duplicates(subset='id')

    final_columns = [
        'id', 'title', 'company', 'location',
        'job_label', 'is_remote', 'has_python', 
        'has_ai', 'description_length', 'etl_id', 
        'load_timestamp', 'created', 'latitude', 
        'longitude', 'redirect_url', 'contract_type', 
        'salary_min', 'salary_max', 'salary_is_predicted'
    ]

    result_data = result_data[[col for col in final_columns if col in result_data.columns]]

    return result_data