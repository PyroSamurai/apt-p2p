test:
	python khashmir.py | grep -v "factory <" | grep -v "<POST /RPC2 HTTP/1.0>"