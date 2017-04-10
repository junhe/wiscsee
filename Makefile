test:
	sudo python -m unittest discover -s tests -v -p '*test*.py' > tmp

testworkflow:
	./run_testclass.sh tests.test_workflow

f2fsgc:
	mkdir -p bin
	cd ./foreign && gcc -o forcef2fsgc forcef2fsgc.c && mv forcef2fsgc ../bin/

setup:
	./setup.env.sh


