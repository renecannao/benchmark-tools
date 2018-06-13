#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <boost/histogram.hpp>
#include <iostream>

// using histogram from https://github.com/HDembinski/histogram

//namespace bh = boost::histogram;
//using namespace bh::literals;

unsigned long long monotonic_time() {
	struct timespec ts;
	//clock_gettime(CLOCK_MONOTONIC_COARSE, &ts); // this is faster, but not precise
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (((unsigned long long) ts.tv_sec) * 1000000) + (ts.tv_nsec / 1000);
}

struct cpu_timer
{
	cpu_timer() {
		begin = monotonic_time();
	}
	~cpu_timer()
	{
		unsigned long long end = monotonic_time();
		std::cerr << double( end - begin ) / 1000000 << " secs.\n" ;
		begin=end-begin;
	};
	unsigned long long begin;
};

/*
auto gh0 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 2000, "us"));
auto gh1 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 40000, "us"));
auto gh2 = bh::make_static_histogram(bh::axis::regular<>(25, 0, 1000, "ms"));
auto gh3 = bh::make_static_histogram(bh::axis::regular<>(10, 0, 10, "s"));
*/
pthread_mutex_t mutex;

int queries_per_connections=1;
int num_threads=1;
int count=0;
char *username=NULL;
char *password=NULL;
char *host=(char *)"localhost";
int port=3306;
int multiport=1;
char *schema=(char *)"information_schema";
int silent;
int keep_open=0;
int local=0;
int queries=0;
int uniquequeries=0;
int histograms=-1;
unsigned int g_connect_OK=0;
unsigned int g_connect_ERR=0;
unsigned int g_select_OK=0;
unsigned int g_select_ERR=0;

unsigned int status_connections = 0;
unsigned int connect_phase_completed = 0;
unsigned int query_phase_completed = 0;

__thread int g_seed;

inline int fastrand() {
	g_seed = (214013*g_seed+2531011);
	return (g_seed>>16)&0x7FFF;
}


void * my_conn_thread(void *arg) {
/*
	auto h0 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 2000, "us"));
	auto h1 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 40000, "us"));
	auto h2 = bh::make_static_histogram(bh::axis::regular<>(25, 0, 1000, "ms"));
	auto h3 = bh::make_static_histogram(bh::axis::regular<>(10, 0, 10, "s"));
*/

	g_seed = time(NULL) ^ getpid() ^ pthread_self();
	unsigned int connect_OK=0;
	unsigned int connect_ERR=0;
	unsigned int select_OK=0;
	unsigned int select_ERR=0;
	char arg_on=1;
	int i, j;
	char query[128];
	unsigned long long b, e, ce;
	MYSQL **mysqlconns=(MYSQL **)malloc(sizeof(MYSQL *)*count);
	if (mysqlconns==NULL) {
		exit(EXIT_FAILURE);
	}
	for (i=0; i<count; i++) {
		MYSQL *mysql=mysql_init(NULL);
		if (mysql==NULL) {
			exit(EXIT_FAILURE);
		}	
		MYSQL *rc=mysql_real_connect(mysql, host, username, password, schema, (local ? 0 : ( port + rand()%multiport ) ), NULL, 0);
		if (rc==NULL) {
			if (silent==0) {
				fprintf(stderr,"%s\n", mysql_error(mysql));
			}
			exit(EXIT_FAILURE);
		}
		mysqlconns[i]=mysql;
		__sync_add_and_fetch(&status_connections,1);
	}
	__sync_fetch_and_add(&connect_phase_completed,1);

	while(__sync_fetch_and_add(&connect_phase_completed,0) != num_threads) {
	}
	MYSQL *mysql;
	for (j=0; j<queries; j++) {
		int r1=fastrand()%count;
		int r2=fastrand()%uniquequeries;
		int r3=fastrand()%uniquequeries;
		sprintf(query,"SELECT %d FROM tablename%d , table%d", r1, r2,r3);
		//sprintf(query,"SELECT LAST_INSERT_ID()");
		if (j%queries_per_connections==0) {
			mysql=mysqlconns[r1];
		}
		if (mysql_query(mysql,query)) {
			if (silent==0) {
				fprintf(stderr,"%s\n", mysql_error(mysql));
			}
			select_ERR++;
			__sync_fetch_and_add(&g_select_ERR,1);
		} else {
			MYSQL_RES *result = mysql_store_result(mysql);
			mysql_free_result(result);
			select_OK++;
			__sync_fetch_and_add(&g_select_OK,1);
		}
	}
	__sync_fetch_and_add(&query_phase_completed,1);

	return NULL;
}

int main(int argc, char *argv[]) {
	pthread_mutex_init(&mutex,NULL);
	int opt;
	while ((opt = getopt(argc, argv, "H:kst:b:c:u:p:h:P:D:q:U:M:")) != -1) {
		switch (opt) {
		case 'H':
			histograms = atoi(optarg);
			if (histograms > 3) {
				fprintf(stderr,"Incorrect number of histograms\n");
				exit(EXIT_FAILURE);
			}
			break;
		case 't':
			num_threads = atoi(optarg);
			break;
		case 'b':
			queries_per_connections = atoi(optarg);
			break;
		case 'c':
			count = atoi(optarg);
			break;
		case 'U':
			uniquequeries = atoi(optarg);
			break;
		case 'q':
			queries = atoi(optarg);
			break;
		case 'u':
			username = strdup(optarg);
			break;
		case 'p':
			password = strdup(optarg);
			break;
		case 'h':
			host = strdup(optarg);
			break;
		case 'D':
			schema = strdup(optarg);
			break;
		case 'P':
			port = atoi(optarg);
			break;
		case 'M':
			multiport = atoi(optarg);
			break;
		case 'k':
			keep_open = 1;
			break;
		case 's':
			silent = 1;
			break;
		default: /* '?' */
			fprintf(stderr, "Usage: %s -c count -u username -p password [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -U target_unique_query ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ] [ -b queries_per_connection ]\n", argv[0]);
			exit(EXIT_FAILURE);
		}
	}
	if (
		(queries_per_connections == 0) ||
		(count == 0) ||
		(username == NULL) ||
		(password == NULL)
	) {
		fprintf(stderr, "Usage: %s -c count -u username -p password [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -U target_unique_query ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ] [ -b queries_per_connections ]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	int i;
	int rc;
	if (strcmp(host,"localhost")==0) {
		local = 1;
	}
	if (uniquequeries == 0) {
		if (queries) uniquequeries=queries;
	}
	if (uniquequeries) {
		uniquequeries=(int)sqrt(uniquequeries);
	}
	mysql_library_init(0, NULL, NULL);

	pthread_t *thi=(pthread_t *)malloc(sizeof(pthread_t)*num_threads);
	if (thi==NULL) exit(EXIT_FAILURE);
	unsigned long long start_time=monotonic_time();
	unsigned long long begin_conn;
	unsigned long long end_conn;
	unsigned long long begin_query;
	unsigned long long end_query;
	
	for (i=0; i<num_threads; i++) {
		if ( pthread_create(&thi[i], NULL, my_conn_thread , NULL) != 0 )
    		perror("Thread creation");
	}
	{
		i=0;
		begin_conn = monotonic_time();
		unsigned long long prev_time = begin_conn;
		unsigned long long prev_conn = __sync_fetch_and_add(&status_connections,0);
		while(__sync_fetch_and_add(&connect_phase_completed,0) != num_threads) {
			usleep(10000);
			i++;
			if (i==50) {
				unsigned long long curr_conn = __sync_fetch_and_add(&status_connections,0);
				unsigned long long curr_time = monotonic_time();
				//fprintf(stderr,"Connections: %d\n",__sync_fetch_and_add(&status_connections,0));
				std::cerr << "Status : Created " << curr_conn << " total , new " << curr_conn - prev_conn << " connections in "  << double( curr_time - prev_time ) / 1000 << " millisecs. : " << double((curr_conn-prev_conn)*1000000/(curr_time - prev_time)  ) << " Conn/s\n" ;
				i=0;
				prev_conn = curr_conn;
				prev_time = curr_time;
				
			}
		}
		end_conn = monotonic_time();
		std::cerr << "Created " << __sync_fetch_and_add(&status_connections,0) << " connections in "  << double( end_conn - begin_conn ) / 1000 << " millisecs.\n" ;
	}
	{
		i=0;
		begin_query = monotonic_time();
		unsigned long long prev_time = begin_query;
		unsigned long long prev_conn = __sync_fetch_and_add(&g_select_OK,0);
		while(__sync_fetch_and_add(&query_phase_completed,0) != num_threads) {
			usleep(10000);
			i++;
			if (i==100) {
				unsigned long long curr_conn = __sync_fetch_and_add(&g_select_OK,0);
				unsigned long long curr_time = monotonic_time();
				std::cerr << "Status : Executed " << curr_conn << " total , new " << curr_conn - prev_conn << " queries in "  << double( curr_time - prev_time ) / 1000 << " millisecs. : " << double((curr_conn-prev_conn)*1000000/(curr_time - prev_time)  ) << " QPS\n" ;
				//fprintf(stderr,"Queries [OK/ERR]: %d / %d\n", __sync_fetch_and_add(&g_select_OK,0), __sync_fetch_and_add(&g_select_ERR,0));
				i=0;
				prev_conn = curr_conn;
				prev_time = curr_time;
			}
		}
		end_query = monotonic_time();
		std::cerr << "Executed " << __sync_fetch_and_add(&g_select_OK,0) << " queries in "  << double( end_query - begin_query ) / 1000 << " millisecs.\n" ;
	}
	for (i=0; i<num_threads; i++) {
		pthread_join(thi[i], NULL);
	}
	std::cerr << "Created " << __sync_fetch_and_add(&status_connections,0) << " connections in "  << double( end_conn - begin_conn ) / 1000 << " millisecs.\n" ;
	std::cerr << "Executed " << __sync_fetch_and_add(&g_select_OK,0) << " queries in "  << double( end_query - begin_query ) / 1000 << " millisecs.\n" ;

	exit(EXIT_SUCCESS);
}
