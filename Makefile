
.PHONY: all docs clean

all: docs

docs:
	make -C docs html

clean:
	-rm -rf build
	make -C docs clean

