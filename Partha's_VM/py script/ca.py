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
    filename="log_ca.txt",
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
            o.id as order_id,
            o.created_at + time '5:30' as cart_created_at,
            --o.user_id,
            o.order_total_paise/100*1.0 as cart_value,
            --o.state,
            --osl.channel,
            u.name,
            ph.number,
            ph.otp_verified_at + time '5:30' as cust_registered_date,
            EXTRACT(DAY FROM o.created_at) as day
        FROM public.orders as o
        left join public.order_status_logs as osl 
        on osl.order_id = o.id
        left join users as u
        on u.id = o.user_id
        left join phones as ph
        on ph.user_id = o.user_id
        WHERE 
            EXTRACT(MONTH FROM o.created_at) = EXTRACT(MONTH FROM CURRENT_DATE) 
            AND EXTRACT(YEAR FROM o.created_at) = EXTRACT(YEAR FROM CURRENT_DATE) 
            AND o.state = 0 
            AND o.city_id = 13
            AND osl.to_state = 0
            AND osl.channel in ('1','4')
        ORDER BY cart_created_at DESC
        ''')

        records = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] # Get column names from cursor description
        logging.info(f'{len(records)} fetched from DB anmd saved in df')
        df = pd.DataFrame(records, columns=columns) # Convert records to DataFrame
        
except (Exception, psycopg2.Error) as error:
    logging.info("Error connecting to PostgreSQL:", error)

finally:
    # Close the database connection
    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed")

logging.info('Started configuring gsheet details')
# Define Google Sheet details
gsheet_name = 'Add to Cart Customer Details'
tab_name = 'Cart_Abondoned_Orders'
json_key_path = "/home/ec2-user/kumarrohit/summary-automation-project-fd46b6ab2eba.json"
sheet_key = "14C3k9An4SSyDAWihuXi_IOC4w8wBLSEa574bAmhDZX0"

# Function to connect to Google Sheets and update the worksheet
def write_df_to_gsheet(tab_name, df, clear_sheet=False):
    gc = gspread.service_account(filename=json_key_path)
    sh = gc.open_by_key(sheet_key)
    worksheet = sh.worksheet(tab_name)
    if clear_sheet:
        worksheet.clear()
        print(f"Google Sheet '{tab_name}' cleared successfully.")
    set_with_dataframe(worksheet, df)
    print(f"Google Sheet '{tab_name}' updated successfully at:", pd.Timestamp.now())
from datetime import datetime
# Check if today is the 1st day of the month
if datetime.today().day == 1:
    write_df_to_gsheet(tab_name, df, clear_sheet=True)  # Clear before updating on the 4th
else:
    write_df_to_gsheet(tab_name, df)  # Only update data without clearing


