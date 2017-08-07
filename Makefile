

bench_spike: bench_spike.cpp
	g++ bench_spike.cpp -o bench_spike -Iinclude -std=c++11 -lmysqlclient -pthread -O2 -ggdb

connect_speed: connect_speed.cpp
	g++ connect_speed.cpp -o connect_speed -Iinclude -std=c++11 -lmysqlclient -pthread -O2 -ggdb

all: connect_speed bench_spike
