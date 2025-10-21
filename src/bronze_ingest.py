import pandas as pd

def bronze_ingest(via_path: str, afi_path: str) -> pd.DataFrame:
    df1 = pd.read_csv(via_path, encoding='latin1')
    df1 = df1[["ProductID","WebBenefits","PurchaseStatusCodeID"]]
    df1 = df1[df1["PurchaseStatusCodeID"] == "A"]
    df1.rename(columns={"ProductID":"sku_id",
                        "WebBenefits":"via_product_description",
                        "PurchaseStatusCodeID":"via_purchase_status_code"},
                        inplace=True)
    df2 = pd.read_csv(afi_path)
    df2 = df2[["sku",
               "itemName",
               "itemSeriesName",
               "consumerDescription",
               "retailType",
               "homestoreProductLine",
               "color",
               "seriesFeatures",
               "detailedDescription",
               "status"]]
    df2 = df2[df2["status"] == "Current"]
    df2.rename(columns={"sku":"sku_id",
                        "itemName":"name",
                        "itemSeriesName":"series_name",
                        "consumerDescription":"short_description",
                        "retailType":"retail_type",
                        "homestoreProductLine":"product_line",
                        "color":"color",
                        "seriesFeatures":"afi_features",
                        "detailedDescription":"afi_product_description",
                        "status":"afi_purchase_status"},
                        inplace=True)
    combined = pd.merge(df2, df1, on ="sku_id", how = "outer")
    return combined
