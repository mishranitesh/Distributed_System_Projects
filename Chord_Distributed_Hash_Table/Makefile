# Command like --> make start_server port=9095

all: start_server

start_server:
	python3 ./src/ChordServer.py $(port)

test_basic_client:
	python3 ./src/ChordClient_Basic.py $(ip) $(port)

test_advanced_client:
	python3 ./src/ChordClient_Advanced.py $(ip) $(port)

clean:
	rm -rf ./src/*.pyc ./src/__pycache__ ./gen-py/chord/__pycache__/

