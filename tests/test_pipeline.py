from pathlib import Path

def test_project_structure():
    root=Path(__file__).parents[1]
    for p in ['docker-compose.yml',
              'README.md','ingestion/generate_data.py',
              'ingestion/kafka_producer.py',
              'spark/batch/run_batch.py',
              'spark/streaming/order_stream.py',
              'airflow/dags/supply_chain_pipeline.py']:
        assert (root/p).exists(), p
