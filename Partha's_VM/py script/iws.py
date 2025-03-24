#This script will fetch item wise sales data from db and uploads in cc_dashboard gsheet
import csv
import psycopg2
import pandas as pd
import numpy as np
import paramiko
from sshtunnel import SSHTunnelForwarder
import gspread
import re
from gspread_dataframe import set_with_dataframe
import logging
logging.basicConfig(
    filename="log_iws.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
try:
    # Create an SSH tunnel to the EC2 instance
    with SSHTunnelForwarder(
        ('65.1.183.184', 22),
        ssh_username='ubuntu',
        ssh_pkey='/home/ec2-user/kumarrohit/emami-prod.pem',
        remote_bind_address=('emami-fr-prod-db.crknf6guwalh.ap-south-1.rds.amazonaws.com', 5432),
        local_bind_address=('localhost', 5432)
    ) as tunnel:
        # Establish a connection to the RDS PostgreSQL database
        connection = psycopg2.connect(
            host='localhost',
            port=tunnel.local_bind_port,
            database='efrprod',
            user='emamireaduser',
            password='emamireadaccess'
        )
        cursor = connection.cursor()
        # Perform database operations here
        logging.info('Executing DB query.....')
        cursor.execute(
'''
                SELECT
                line_items.order_id as ord_id,	
                line_items.variant_id,
                products.name as product_name,	
                categories.name as category,
                line_items.quantity,	
                line_items.mrp_paise * 1.0 / 100 as MRP,	
                line_items.sales_price_paise * 1.0 / 100 as Sales_price,	
                line_items.total_paise * 1.0 / 100 as Total_Price,	
                line_items.discount_amount_paise * 1.0 / 100 as Discount,
                --osl.modified_by_id,
                users.name as modified_by,
                CASE 
                    WHEN o.customer_id IS NULL AND osl.channel = 1 THEN 'Mobile'
                    WHEN osl.channel = 0 AND COALESCE(o.delivery_remarks, '') LIKE '%FRSAATH%' THEN 'Saathi'
                    WHEN osl.channel = 0 AND osl.modified_by_id in(610295,383599,383941,436264) then 'Ecom_CC'
                    WHEN osl.channel = 0 THEN 'Call Center'
                    WHEN osl.channel = 1 THEN 'Saathi_App'
                    ELSE 'Website'
                END as channel,
                osl.state_changed_on + time '5:30' as confirmed_on,
                CASE 
                    WHEN o.state = 0 THEN 'cart'
                    WHEN o.state = 1 THEN 'pre_checkout'
                    WHEN o.state = 2 THEN 'Checkout'
                    WHEN o.state = 6 THEN 'Delivered'
                    WHEN o.state = 7 THEN 'Cancelled'
                    WHEN o.state = 14 THEN 'Return_completed'
                    ELSE 'In-Progress' 
                END as status,
                dc.dc_code,
                ip.property_value as d_profile,
                ip2.property_value as actute_chronic
                FROM 
                line_items 
                LEFT JOIN variants ON line_items.variant_id = variants.id	
                LEFT JOIN products ON variants.product_id = products.id	
                LEFT JOIN product_types ON products.product_type_id = product_types.id	
                LEFT JOIN categories ON categories.id = product_types.category_id
                LEFT JOIN order_status_logs osl ON osl.order_id = line_items.order_id
                LEFT JOIN orders o ON o.id = osl.order_id
                LEFT JOIN distribution_centers dc ON dc.id = o.store_id
                LEFT JOIN item_properties ip ON ip.item_id = line_items.variant_id AND ip.property_id IN ('1351')
                LEFT JOIN item_properties ip2 ON ip2.item_id = line_items.variant_id AND ip2.property_id IN ('1352')
                LEFT JOIN users on users.id = osl.modified_by_id
                WHERE 
                line_items.order_id IN ( 
                    SELECT order_id 
                    FROM order_status_logs as ols
                    LEFT JOIN orders ON orders.id = ols.order_id
                    WHERE 
                        state_changed_on >= '2025-01-31 18:30:00.000000' 
                        AND to_state = 3
                        AND city_id = 13
                )
                AND osl.to_state = 3
                ORDER BY confirmed_on DESC
''')

        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] # Get column names from cursor description
        logging.info(f'{len(records)} fetched from DB anmd saved in df')
        df = pd.DataFrame(records, columns=columns) # Convert records to DataFrame

        # Update Google Sheets
        gsheet_name = 'CC Order Dashboard'  # Google sheet to be updated
        tab_name = 'Sales'  # Tab to be updated
        
        # Function to write data to Google Sheets
        def write_df_to_gsheet(gsheet_name, tab_name, df):
            gc = gspread.service_account(filename="/home/ec2-user/kumarrohit/summary-automation-project-fd46b6ab2eba.json")
            sh = gc.open_by_key("1_ibELjBzTBBjKeNkgsIm5EzrDo5vamHZraOjMjDqNvI")  # Key of the Google Sheet
            worksheet = sh.worksheet(tab_name)
            set_with_dataframe(worksheet, df)
            logging.info(f'{gsheet_name} sheet updated' )
        
        write_df_to_gsheet(gsheet_name, tab_name, df)
        
        
except (Exception, psycopg2.Error) as error:
    logging.info("Error connecting to PostgreSQL:", error)

finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed\n")

