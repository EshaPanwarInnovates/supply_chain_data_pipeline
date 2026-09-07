from kafka.admin import KafkaAdminClient, NewTopic
import os
admin=KafkaAdminClient(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092'), 
                       client_id='supply-chain-admin')
try: 
    admin.create_topics([NewTopic('supply-chain-events',1,1)])
except Exception: pass
admin.close()
