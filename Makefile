.PHONY: all
all: echo uid broadcast

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

.PHONY: broadcast
broadcast:
	mkdir -p build
	go build -o ./build/broadcast ./cmd/broadcast
	# Challenge 3a: Single-Node Broadcast
	# maelstrom test -w broadcast --bin ./build/broadcast --node-count 1 --time-limit 20 --rate 10
	# Challenge 3b: Multi-Node Broadcast
	# maelstrom test -w broadcast --bin ./build/broadcast --node-count 5 --time-limit 20 --rate 10
	# Challenge 3c: Fault Tolerant Broadcast
	maelstrom test -w broadcast --bin ./build/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

.PHONY: clean
	rm -rf build
