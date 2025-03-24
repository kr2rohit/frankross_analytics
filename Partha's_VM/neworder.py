import common_func as cf

query = '''SELECT log.order_id, ord.store_id 
           FROM   order_status_logs log, orders ord 
           WHERE  log.from_state = 2 and log.to_state = 3 
           AND    log.state_changed_on > NOW() - INTERVAL '15 minutes' 
           AND    log.order_id = ord.id  
           AND    NOT EXISTS ( SELECT 'x' FROM order_status_logs log2 
                                WHERE log.order_id = log2.order_id 
                                AND log2.to_state = 7 )
           ORDER BY ord.store_id ; '''

admin_recipients = [9051239786, 6292324196]
cf.process(query, admin_recipients, id_index = 0, 
           store_index = 1, prefix = "Recent new orders-", use_store_recipient = True)
