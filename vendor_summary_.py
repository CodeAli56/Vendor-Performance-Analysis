import sqlite3
import pandas as pd
from ingestiondb import ingest_db
import logging

# do not use logging.basicConfig, cz it will create only one log file the very first one, after that it will just append all the logs from any file to the same first log file.
vendor_logger = logging.getLogger("vendor")
vendor_handler = logging.FileHandler("logs/vendordb.log", mode="a")
vendor_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
vendor_logger.addHandler(vendor_handler)
vendor_logger.setLevel(logging.DEBUG)


def create_vendor_summary(conn):
    #creating sub tables with relevant columns.
    
    freight_summary = pd.read_sql("Select vendorNumber, round(sum(freight),2) as FreightCost from vendor_invoice group by vendorNumber", conn)

    prices_summary = pd.read_sql(
    """ Select
            p.vendorNumber, 
            p.vendorName, 
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            sum(p.Quantity) as TotalPurchasedQuantity,
            sum(p.Dollars) as TotalPurchasedDollar
        from purchases p join purchase_prices pp on p.Brand = pp.Brand
        where p.purchasePrice > 0
        group by 1,2,3      
        order by 8                    
    """, conn)

    sales_summary = pd.read_sql(
    """
        select
            vendorNo as VendorNumber,
            Brand,
            round(sum(SalesDollars), 2) as TotalSalesDollars,
            round(sum(SalesPrice), 2) as TotalSalesPrice,
            round(sum(SalesQuantity), 2) as TotalSalesQuantity,
            round(sum(ExciseTax),2 ) as TotalExcisetax           
        from sales
        group by 1, 2
        order by 3
    """, conn)

    priceSalesSummary = pd.merge(prices_summary, sales_summary, left_on=["VendorNumber", "Brand"], right_on=["VendorNumber", "Brand"], how='left')

    vendor_summary = pd.merge(priceSalesSummary, freight_summary, on="VendorNumber", how='left')

    return vendor_summary


def cleaning_vendor(df):
    # cleaning the data
    df['Volume'] = df['Volume'].astype("float")
    df["VendorName"] = df["VendorName"].apply(lambda x: x.strip())
    df.fillna(value=0, inplace=True)
    df["Description"] = df["Description"].apply(lambda x: x.strip())

    # creating relevant columns for analysis.
    df["GrossProfit"] = (df["TotalSalesDollars"] - df['TotalPurchasedDollar']).round(2)
    df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars']*100).round(2)
    df['StockTurnOver'] = df['TotalSalesQuantity'] / df['TotalPurchasedQuantity']
    df['SalesToPurchaseRatio'] = (df['TotalSalesDollars'] / df["TotalPurchasedDollar"]).round(2)

    return df


if __name__ == "__main__":
    # connecting to database
    conn = sqlite3.connect("inventory.db")

    vendor_logger.info("Creating vendor Summary Table .........")
    summary_df = create_vendor_summary(conn)

    vendor_logger.info("Cleaning Data .........")
    clean_df = cleaning_vendor(summary_df)

    vendor_logger.info("Ingesting data ........")
    ingest_db(clean_df, "vendor_summary", conn)
    vendor_logger.info("Completed ..........")