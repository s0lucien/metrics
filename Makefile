.PHONY: all clean test

all:
	dune build

clean:
	dune clean

test:
	dune runtest

deps:
	sudo apt install gnuplot
	opam install alcotest mtime uuidm