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
import schedule as schedule
import time

def order_summ():
    # EC2 instance connection details
    ec2_host = '65.1.183.184'
    ec2_username = 'ubuntu'
    ec2_pem_key_path = '/home/ec2-user/kumarrohit/emami-prod.pem'

    # AWS RDS PostgreSQL database connection details
    rds_host = 'emami-fr-prod-db.crknf6guwalh.ap-south-1.rds.amazonaws.com'
    rds_port = 5432
    rds_database = 'efrprod'
    rds_user = 'emamireaduser'
    rds_password = 'emamireadaccess'


    # Configure logging
    logging.basicConfig(
        filename="log_odtest.txt",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    try:
        # Create an SSH tunnel to the EC2 instance
        with SSHTunnelForwarder(
            (ec2_host, 22),
            ssh_username=ec2_username,
            ssh_pkey=ec2_pem_key_path,
            remote_bind_address=(rds_host, rds_port),
            local_bind_address=('localhost', 5432)
        ) as tunnel:
            # Establish a connection to the RDS PostgreSQL database
            connection = psycopg2.connect(
                host='localhost',
                port=tunnel.local_bind_port,
                database=rds_database,
                user=rds_user,
                password=rds_password
            )
            cursor = connection.cursor()
            logging.info('Cursor Created')
            # Perform database operations here
            logging.info('Executing DB query.....')
            cursor.execute(
    '''
                    SELECT order_Table.*
                    ,CASE WHEN
                        od IS NULL THEN 'Not First Order'
                        ELSE '1st Order' END AS new_flag
                    ,CASE
                        WHEN timeout_at IS NOT NULL THEN 'timeout'
                        WHEN rejected_at IS NOT NULL THEN 'rejected'
                        WHEN accepted_at IS NOT NULL THEN 'accepted'
                        ELSE 'pending' end as order_action
                    --,promo.coupon_code
                    FROM
                    (SELECT		
                     osl.order_id		
                    ,o.user_id as created_by_id		
                    ,CASE		
                        WHEN o.state =0 THEN 'cart'	
                        WHEN o.state =1 THEN 'pre_checkout'	
                        WHEN o.state =2 THEN 'Checkout'	
                        WHEN o.state =6 THEN 'Delivered'	
                        WHEN o.state =7 THEN 'Cancelled'	
                        WHEN o.state =14  THEN 'Return_completed'	
                        ELSE 'In-Progress'	
                    END AS status		
                    ,o.city_id		
                    ,o.delivery_remarks		
                    ,o.payment_method		
                    ,o.auto_completed		
                    ,o.order_total_paise*1.0/100 AS order_value		
                    ,o.shipping_total_paise*1.0/100 AS Shipping_charge		
                    ,o.doctor_names		
                    --,o.delivery_slot_id, o.store_id, o.shipping_address_id, o.billing_address_id		
                    ,osl.confirmed_on		
                    ,CASE		
                        WHEN osl.channel = 0 THEN 'Call Center'	
                        WHEN osl.channel = 1 THEN 'Mobile'	
                        ELSE 'Website'	
                    END as channel_name		
                    ,CASE		
                        WHEN o.customer_id IS NULL AND osl.channel = 1 THEN 'Mobile'	
                        WHEN osl.channel = 0 AND COALESCE(o.delivery_remarks, '') LIKE '%FRSAATH%' THEN 'Saathi'
                        WHEN osl.channel = 0 AND osl.modified_by_id in(610295,383599,383941,436264) then 'Ecom_CC'
                        WHEN osl.channel = 0 THEN 'Call Center'	
                        WHEN osl.channel = 1 THEN 'Saathi App'	
                        ELSE 'Website'	
                    END as channel2		
                    ,CASE 		
                        WHEN o.state =0 THEN 'cart'	
                        WHEN o.state =1 THEN 'pre_checkout'	
                        WHEN o.state =2 THEN 'Checkout'	
                        WHEN o.state =3 THEN 'order_received'	
                        WHEN o.state =4 THEN 'shipped'	
                        WHEN o.state =5 THEN 'out_for_delivery'	
                        WHEN o.state =6 THEN 'Delivered'	
                        WHEN o.state =7 THEN 'Cancelled'	
                        WHEN o.state =14 THEN 'Return_completed'	
                        WHEN o.state =17 THEN 'rescheduled'	
                            ELSE 'In-Progress' END AS detailed_status
                    ,customer_id		
                    ,users.name AS modified_by		
                    ,w.code AS fulfillment_center	
                    ,Wa.code AS Actual_Mapped_Dc
                    ,DS.slot_description		
                    ,DS.slot_date AS Expected_Delivery		
                    ,DS.slot_date + CAST(SPLIT_PART(DS.slot_description, '-', 1) || ':00' AS TIME) AS exp_delivery_start		
                    ,DS.slot_date + CAST(TRIM(SPLIT_PART(DS.slot_description, '-', 2)) || ':00' AS TIME) AS exp_delivery_end		
                    ,co.state_changed_on + time '5:30' as cancelled_date		
                    ,co.reason		
                    ,co.remarks		
                    ,CASE		
                        WHEN so.modified_by_id = 94098 THEN 'Ecogreen API'	
                        WHEN so.modified_by_id = 25 THEN 'Vinculum API'	
                        WHEN so.modified_by_id = 44306 THEN 'Delite'	
                        WHEN so.modified_by_id = 162007 THEN 'Kumar Rohit'	
                        WHEN so.modified_by_id = 175710 THEN 'Arun Kumar'	
                        ELSE 'Others'END AS shipped_by	
                    ,so.state_changed_on + time '5:30'AS shipped		
                    ,ofd.state_changed_on + time '5:30'AS out_for_delivery		
                    ,del.state_changed_on + time '5:30'AS delivered_date		
                    ,iv.amount_paise/100*1.0 AS invoiced_amt		
                    ,iv.invoiced_at		
                    ,iv.wallet_amount		
                    ,ph.number		
                    ,us.name AS user_name		
                    ,us.created_at AS registration_date		
                    ,us.registration_source		
                    ,areas.pincode
                    --,op.promotion_id
                    --,op.promotion_total_paise/100*1.0 as amount_discounted
                    --,op.cash_back_total_paise/100*1.0 as cashback
                    --,promo.coupon_code
                    ,ROW_NUMBER() OVER(PARTITION BY o.user_id ORDER BY osl.confirmed_on) AS nth_order		
                    --,CASE WHEN 		
                    --	ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY osl.confirmed_on ASC) = 1	
                    --	THEN '1st Order' ELSE 'Not First Order'		
                    --END AS new_flag_month	
                    ,CASE 		
                         WHEN del.state_changed_on + time '5:30' IS NULL THEN 'Undelivered'		
                         WHEN del.state_changed_on + time '5:30' 		
                         < DS.slot_date + CAST(SPLIT_PART(DS.slot_description, '-', 1) || ':00' AS TIME) THEN 'Early'	
                         WHEN del.state_changed_on + time '5:30' 		
                         > DS.slot_date + CAST(TRIM(SPLIT_PART(DS.slot_description, '-', 2)) || ':00' AS TIME) 	
                         THEN 'Delay'	
                         ELSE 'Between Slot'		
                    END AS delivery_flag
                    ,oah_t.created_at + time '5:30' as timeout_at
                    ,oah_a.created_at + time '5:30' as accepted_at
                    ,oah_r.created_at + time '5:30' as rejected_at

                    FROM		
                        (SELECT	
                        order_id	
                        ,MAX(modified_by_id) modified_by_id	
                        ,MAX(channel) channel	
                        ,MAX(state_changed_on + time '5:30') AS confirmed_on	
                        FROM order_status_logs	
                        WHERE state_changed_on >'2025-01-31 18:30' AND to_state = 3	
                        GROUP BY order_id) AS osl	
                    LEFT JOIN orders o ON o.id = osl.order_id		
                    LEFT JOIN users ON users.id = osl.modified_by_id		
                    LEFT JOIN warehouses w ON w.id = o.fulfillment_center_id
                    LEFT JOIN warehouses wa ON wa.id = o.store_id
                    LEFT JOIN delivery_slots DS ON o.delivery_slot_id = DS.id		
                    LEFT JOIN order_status_logs co ON co.order_id = osl.order_id AND co.to_state = 7 -- Calcelled Orders		
                    LEFT JOIN order_status_logs so ON so.order_id = osl.order_id AND so.to_state = 4 -- Shipped Orders		
                    LEFT JOIN order_status_logs ofd ON ofd.order_id = osl.order_id AND ofd.to_state = 5 -- out for delivery		
                    LEFT JOIN order_status_logs del ON del.order_id = osl.order_id AND del.to_state = 6 -- out for delivery		
                    LEFT JOIN invoices iv ON iv.order_id = osl.order_id		
                    LEFT JOIN phones ph ON ph.user_id = o.user_id AND ph.deleted_at IS NULL		
                    LEFT JOIN users us ON us.id = o.user_id 		
                    LEFT JOIN addresses ON addresses.id = o.shipping_address_id --to Get the area id 		
                    LEFT JOIN areas ON areas.id = addresses.area_id
                    LEFT JOIN order_assignment_histories oah_t on oah_t.order_id = osl.order_id AND oah_t.action = 'time_out'
                    LEFT JOIN order_assignment_histories oah_a on oah_a.order_id = osl.order_id AND oah_a.action = 'accept'
                    LEFT JOIN order_assignment_histories oah_r on oah_r.order_id = osl.order_id AND oah_r.action = 'reject'
                    --LEFT JOIN order_promotions op on osl.order_id = op.order_id

                    ORDER BY osl.confirmed_on DESC) as order_Table
                    LEFT JOIN 
                    (SELECT od FROM
                    (SELECT ord.*,
                    ROW_NUMBER() OVER (PARTITION BY ord.user_id ORDER BY ord.confirmed_on) AS order_number 
                    FROM
                    (
                    SELECT 
                    order_id as od
                    ,MAX(u.created_at) as registered_at
                    ,MAX(o.user_id) as user_id
                    ,MAX(state_changed_on + time '5:30') as confirmed_on
                    ,MAX(channel) as channel
                    ,MAX(modified_by_id) as modified_by
                    FROM order_status_logs osl 
                    LEFT JOIN orders o on o.id = osl.order_id
                    LEFT JOIN users u on u.id = o.user_id
                    WHERE to_state = 3 and state_changed_on > '2022-12-31 18:30:00'
                    GROUP BY od
                    ) AS ord) 
                    AS ordr
                    WHERE order_number = 1
                    AND COALESCE(registered_at, '1970-01-01') >= CURRENT_DATE - INTERVAL '12 MONTH'
                    --AND EXTRACT(MONTH FROM confirmed_on) = EXTRACT(MONTH FROM CURRENT_DATE)
                    AND confirmed_on >= '2025-01-31 18:30:00'
                    --AND EXTRACT(YEAR FROM confirmed_on) = EXTRACT(YEAR FROM CURRENT_DATE)
                    ) as first_order
                    on first_order.od = order_Table.order_id
                    --LEFT JOIN promotions promo on promo.id = order_Table.promotion_id
                    order by order_Table.confirmed_on DESC
    ''')

            records = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] # Get column names from cursor description
            df1 = pd.DataFrame(records, columns=columns) # Convert records to DataFrame
            df = df1[['order_id', 'created_by_id', 'status', 'city_id', 'delivery_remarks',
             'payment_method', 'new_flag', 'order_value', 'shipping_charge',
             'detailed_status', 'out_for_delivery', 'confirmed_on', 'channel_name',
             'shipped', 'fulfillment_center', 'slot_description', 'expected_delivery',
             'modified_by', 'reason', 'remarks', 'cancelled_date',
             'delivered_date', 'channel2', 'invoiced_amt', 'shipped_by',
             'exp_delivery_start', 'exp_delivery_end', 'delivery_flag', 'number','user_name' ,'pincode','wallet_amount'
             ,'order_action','actual_mapped_dc','registration_date']]
            logging.info("Data fetched and saved c42 to df")

    except (Exception, psycopg2.Error) as error:
        logging.info("Error connecting to PostgreSQL:", error)

    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection is closed")

    # Updating Data into Google sheets
    logging.info(f'Updating Data into Google sheets')
    gsheet_name = 'Summary_Epharmacy' #This google sheet will be updated
    tab_name = 'C42_Combined' # This particular tab is to be updated

    def write_df_to_gsheet (gsheet_name,tab_name,df):#Updating In Summary_Epharmacy
        gc = gspread.service_account(filename="summary-automation-project-fd46b6ab2eba.json")
        sh = gc.open_by_key("1etqrto99N3Tmv9Z-svwa4OtFjUewpP0KwMiflq36hrg") #Key Of the google sheet - Summary_Epharmacy
        worksheet = sh.worksheet(tab_name)
        set_with_dataframe(worksheet,df)
    write_df_to_gsheet(gsheet_name,tab_name,df)
    logging.info('C42 combined updated')

    # Updating In CC Daily Order Dashboard : =IMPORTRANGE("1etqrto99N3Tmv9Z-svwa4OtFjUewpP0KwMiflq36hrg","C42_Combined!A:AB")
    gsheet_name = 'CC Daily Order Dashboard' #This google sheet will be updated
    tab_name = 'C42' # This particular tab is to be updated
    def write_df_to_gsheet (gsheet_name,tab_name,df):
        gc = gspread.service_account(filename="summary-automation-project-fd46b6ab2eba.json")
        sh = gc.open_by_key("1_ibELjBzTBBjKeNkgsIm5EzrDo5vamHZraOjMjDqNvI") #Key Of the google sheet - Summary_Epharmacy
        worksheet = sh.worksheet(tab_name)
        set_with_dataframe(worksheet,df)
    write_df_to_gsheet(gsheet_name,tab_name,df)
    logging.info('cc dashboard sheet updated')
    
logging.info('Schedule the task to run every hour')
schedule.every().hour.at(":20").do(order_summ)
while True:
    schedule.run_pending()
    time.sleep(5)