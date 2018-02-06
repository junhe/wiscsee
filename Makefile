test_all:
	sudo python -m unittest discover -s tests -v -p '*test*.py' > test-log

run_demo:
	./run_testclass.sh tests.test_demo

setup:
	./setup.env.sh

f2fsgc:
	mkdir -p bin
	cd ./foreign && gcc -o forcef2fsgc forcef2fsgc.c && mv forcef2fsgc ../bin/

