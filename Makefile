.PHONY: all
all: echo uid

.PHONY: echo
echo:
	mkdir -p build
	go build -o ./build/echo ./cmd/echo
	maelstrom test -w echo --bin ./build/echo --node-count 1 --time-limit 10

.PHONY: uid
uid:
	mkdir -p build
	go build -o ./build/uid ./cmd/uid
	maelstrom test -w unique-ids --bin ./build/uid --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

.PHONY: clean
	rm -rf build
