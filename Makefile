

bench_spike: bench_spike.cpp
	g++ bench_spike.cpp -o bench_spike -Iinclude -std=c++11 -lmysqlclient -pthread -O2 -ggdb

unique_queries: unique_queries.cpp
	g++ unique_queries.cpp -o unique_queries -Iinclude -std=c++11 -lmysqlclient -pthread -O2 -ggdb

connect_speed: connect_speed.cpp
	g++ connect_speed.cpp -o connect_speed -Iinclude -std=c++11 -lmysqlclient -pthread -O2 -ggdb

connect_speed8: connect_speed.cpp
	g++ connect_speed.cpp /home/rcannao/opt/mysql/8.0.4/lib/libmysqlclient.a -o connect_speed8 -Iinclude -I/home/rcannao/opt/mysql/8.0.4/mi -std=c++11 -pthread -O2 -ggdb -ldl -lssl -lcrypto

all: connect_speed bench_spike
