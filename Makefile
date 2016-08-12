test:
	sudo python -m unittest discover -s tests -v -p '*test*.py' > tmp

testblockpools:
	./run_testclass.sh tests.test_devblockpool && ./run_testclass.sh tests.test_tagblockpool  && ./run_testclass.sh tests.test_blockpool

testnkftl:
	./run_testclass.sh tests.test_nkftl

testcontractbench:
	./run_testclass.sh tests._accpatterns.test_contractbench

patternbench:
	sudo python -m benchmarks.patternbench -t "pattern_on_fs()"

fiobench:
	sudo python -m benchmarks.fiobench -t 'compare_fs()'

appbench:
	sudo python -m benchmarks.appbench -t 'bench()'

reproduce:
	sudo python -m benchmarks.appbench -t 'reproduce()'

contractbench:
	sudo python -m benchmarks.lbabench -t 'contract_bench()'

leveldb:
	sudo python -m benchmarks.appbench -t 'leveldbbench()'

sqlite:
	sudo python -m benchmarks.appbench -t 'sqlitebench()'

newsqlbench:
	sudo python -m benchmarks.appbench -t 'newsqlbench()'

f2fsgc:
	mkdir -p bin
	cd ./foreign && gcc -o forcef2fsgc forcef2fsgc.c && mv forcef2fsgc ../bin/

