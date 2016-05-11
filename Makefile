test:
	sudo python -m unittest discover -s tests -v -p '*test*.py' > tmp

patternbench:
	sudo python -m benchmarks.patternbench -t "pattern_on_fs()"
