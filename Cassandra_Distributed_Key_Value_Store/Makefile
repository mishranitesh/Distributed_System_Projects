start_replica:
	python3 keyvalue_store.py $(name) $(port) $(consistency_mechanism) $(filename) $(use_file)

setup_connection:
	python3 setup_replica_connections.py $(filename)

start_client:
	python3 client.py $(coordinator_ip) $(coordinator_port)

clean:
	rm -rf ./*.pyc ./__pycache__ ./*.log
