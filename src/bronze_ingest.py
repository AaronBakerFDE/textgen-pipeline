import pandas as pd


def bronze_ingest(storis_path: str, afi_path: str) -> pd.DataFrame:
    storis_data = pd.read_csv(storis_path)
    afi_data = pd.read_csv(afi_path)
    
    merged_df = pd.merge(afi_data, 
                     storis_data, 
                     left_on = "sku", 
                     right_on = "ProductID", 
                     how = "inner", 
                     suffixes=('_afi', '_storis'))
    
    merged_df = merged_df[['itemName','itemSeriesName','color','consumerAssembly','detailedDescription','friendlyDescription','itemCode','itemGeneralLongDescription','seriesFeatures','homestoreProductLine','importDomestic']]
    merged_df = merged_df[1000:1010] 
    merged_df = merged_df.rename(columns={
        'itemName': 'item_name',
        'itemSeriesName': 'item_series_name',
        'color': 'color',
        'consumerAssembly': 'consumer_assembly',
        'detailedDescription': 'detailed_description',
        'friendlyDescription': 'friendly_description',
        'itemCode': 'item_code',
        'itemGeneralLongDescription': 'item_general_description',
        'seriesFeatures': 'seo_features_and_keywords',
        'homestoreProductLine': 'homestore_product_line',
        'importDomestic': 'import_domestic'})
    return merged_df