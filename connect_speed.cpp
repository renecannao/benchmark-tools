//#define USE_HISTOGRAM
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <mysql/my_config.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#ifdef USE_HISTOGRAM
#include <boost/histogram.hpp>
#endif
#include <iostream>

#if MYSQL_VERSION_MAJOR > 5
#define MYSQL_WITH_SSL
#endif 

#ifndef MYSQL_WITH_SSL
#if MYSQL_VERSION_MAJOR == 5 && MYSQL_VERSION_MINOR == 7 && MYSQL_VERSION_PATCH > 10
#define MYSQL_WITH_SSL
#endif 
#endif

#ifdef USE_HISTOGRAM
// using histogram from https://github.com/HDembinski/histogram

namespace bh = boost::histogram;
using namespace bh::literals;
#endif

unsigned long long monotonic_time() {
	struct timespec ts;
	//clock_gettime(CLOCK_MONOTONIC_COARSE, &ts); // this is faster, but not precise
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (((unsigned long long) ts.tv_sec) * 1000000) + (ts.tv_nsec / 1000);
}

#ifdef USE_HISTOGRAM
auto gh0 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 2000, "us"));
auto gh1 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 40000, "us"));
auto gh2 = bh::make_static_histogram(bh::axis::regular<>(25, 0, 1000, "ms"));
auto gh3 = bh::make_static_histogram(bh::axis::regular<>(10, 0, 10, "s"));
#endif
pthread_mutex_t mutex;

int interval_us=-1;
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
#ifdef USE_HISTOGRAM
int histograms=-1;
#endif
unsigned int g_connect_OK=0;
unsigned int g_connect_ERR=0;
unsigned int g_select_OK=0;
unsigned int g_select_ERR=0;
#ifdef MYSQL_WITH_SSL
char *ssl = NULL;
char *auth = NULL;
#endif

void * my_conn_thread(void *arg) {
#ifdef USE_HISTOGRAM
	auto h0 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 2000, "us"));
	auto h1 = bh::make_static_histogram(bh::axis::regular<>(20, 0, 40000, "us"));
	auto h2 = bh::make_static_histogram(bh::axis::regular<>(25, 0, 1000, "ms"));
	auto h3 = bh::make_static_histogram(bh::axis::regular<>(10, 0, 10, "s"));
#endif
	unsigned int connect_OK=0;
	unsigned int connect_ERR=0;
	unsigned int select_OK=0;
	unsigned int select_ERR=0;
	char arg_on=1;
	int i, j;
	char query[128];
	unsigned long long b, e, ce;
	for (i=0; i<count; i++) {
		MYSQL *mysql=mysql_init(NULL);
		b=monotonic_time(); // begin
#ifdef MYSQL_WITH_SSL
		unsigned int ssl_arg = SSL_MODE_DISABLED;
		if (ssl) {
			if (strcmp(ssl,"disabled")==0) {
				ssl_arg = SSL_MODE_DISABLED;
			}
			if (strcmp(ssl,"preferred")==0) {
				ssl_arg = SSL_MODE_PREFERRED;
			}
			if (strcmp(ssl,"required")==0) {
				ssl_arg = SSL_MODE_REQUIRED;
			}
		}
		mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_arg);
		if (auth) {
			int rc = 0;
			rc = mysql_options(mysql, MYSQL_DEFAULT_AUTH, auth);
			//rc = mysql_options(mysql, MYSQL_DEFAULT_AUTH, "mysql_native_password");
			if (rc) {
				fprintf(stderr, "Unable to set auth plugin %s\n", mysql_error(mysql));
				exit(EXIT_FAILURE);
			}
		}
#endif
		MYSQL *rc=mysql_real_connect(mysql, host, username, password, schema, (local ? 0 : ( port + rand()%multiport ) ), NULL, 0);
		//mysql_set_character_set(mysql, "utf8");
		if (queries==0) {
			// we computed this only if queries==0
			ce=monotonic_time(); // connection established
		}
		if (rc) {
			connect_OK++;
			if (queries)
				setsockopt(mysql->net.fd, SOL_SOCKET, SO_REUSEADDR, (char *)&arg_on, sizeof(arg_on));
			for (j=0; j<queries; j++) {
				int r1=rand()%100;
				sprintf(query,"SELECT %d", r1);

				if (mysql_query(mysql,query)) {
					if (silent==0) {
						fprintf(stderr,"%s\n", mysql_error(mysql));
					}
					select_ERR++;
				} else {
					MYSQL_RES *result = mysql_store_result(mysql);
					mysql_free_result(result);
					select_OK++;
				}
			}
			if (keep_open==0)
				mysql_close(mysql);
		} else {
			connect_ERR++;
			if (silent==0) {
				fprintf(stderr, "Failed to connect to database: Error: %s\n", mysql_error(mysql));
			}
		}
		e=monotonic_time(); // connection ended
		unsigned long long l;
		if (queries==0) {
			l=(ce-b);
		} else {
			l=(e-b);
		}
#ifdef USE_HISTOGRAM
		if (histograms >= 0 && histograms <= 3) {
			if (l < 40000 && histograms < 2) {
				if (l<2000 && histograms == 0) {
					h0.fill(l);
				} else {
					h1.fill(l);
				}
			} else {
				if (l < 1000000 && histograms < 3) {
					h2.fill(((float)l)/1000);
				} else {
					h3.fill(((float)l)/1000000);
				}
			}
		}
#endif
		l=e-b;
		if (l < interval_us) {
			usleep(interval_us-(e-b));
		}
	}
#ifdef USE_HISTOGRAM
	pthread_mutex_lock(&mutex);
	for (const auto& bin : h0.axis(0_c)) {
		if (h0.value(bin.idx)) {
			gh0.fill((bin.left+bin.right)/2, bh::weight(h0.value(bin.idx)));
		}
	}
	for (const auto& bin : h1.axis(0_c)) {
		if (h1.value(bin.idx)) {
			gh1.fill((bin.left+bin.right)/2, bh::weight(h1.value(bin.idx)));
		}
    }
	for (const auto& bin : h2.axis(0_c)) {
		if (h2.value(bin.idx)) {
			gh2.fill((bin.left+bin.right)/2, bh::weight(h2.value(bin.idx)));
		}
    }
	for (const auto& bin : h3.axis(0_c)) {
		if (h3.value(bin.idx)) {
			gh3.fill((bin.left+bin.right)/2, bh::weight(h3.value(bin.idx)));
		}
    }
	pthread_mutex_unlock(&mutex);
#endif

	__sync_fetch_and_add(&g_connect_OK,connect_OK);
	__sync_fetch_and_add(&g_connect_ERR,connect_ERR);
	__sync_fetch_and_add(&g_select_OK,select_OK);
	__sync_fetch_and_add(&g_select_ERR,select_ERR);
	return NULL;
}

int main(int argc, char *argv[]) {
	pthread_mutex_init(&mutex,NULL);
	int opt;
#ifdef MYSQL_WITH_SSL
	while ((opt = getopt(argc, argv, "H:kst:i:c:u:p:h:P:M:D:q:S:a:")) != -1) {
#else
	while ((opt = getopt(argc, argv, "H:kst:i:c:u:p:h:P:M:D:q:")) != -1) {
#endif
		switch (opt) {
#ifdef USE_HISTOGRAM
		case 'H':
			histograms = atoi(optarg);
			if (histograms > 3) {
				fprintf(stderr,"Incorrect number of histograms\n");
				exit(EXIT_FAILURE);
			}
			break;
#endif
		case 't':
			num_threads = atoi(optarg);
			break;
		case 'i':
			interval_us = atoi(optarg);
			break;
		case 'c':
			count = atoi(optarg);
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
#ifdef MYSQL_WITH_SSL
		case 'S':
			ssl = strdup(optarg);
			break;
		case 'a':
			auth = strdup(optarg);
			break;
#endif
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
#ifdef MYSQL_WITH_SSL
			fprintf(stderr, "Usage: %s -i interval_us -c count -u username -p password [ -h host ] [ -P port ] [ -M multiport ] [ -D schema ] [ -q queries ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ][ -S {disabled|preferred|required} ][ -a auth_plugin ]\n", argv[0]);
#else
			fprintf(stderr, "Usage: %s -i interval_us -c count -u username -p password [ -h host ] [ -P port ] [ -M multiport ] [ -D schema ] [ -q queries ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ]\n", argv[0]);
#endif
			exit(EXIT_FAILURE);
		}
	}
	if (
		(interval_us == -1) ||
		(count == 0) ||
		(username == NULL) ||
		(password == NULL)
	) {
#ifdef MYSQL_WITH_SSL
		fprintf(stderr, "Usage: %s -i interval_us -c count -u username -p password [ -h host ] [ -P port ] [ -M multiport ] [ -D schema ] [ -q queries ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ][ -S {disabled|preferred|required} ][ -a auth_plugin ]\n", argv[0]);
#else
		fprintf(stderr, "Usage: %s -i interval_us -c count -u username -p password [ -h host ] [ -P port ] [ -M multiport ] [ -D schema ] [ -q queries ] [ -H {0|1|2|3} ] [ -s ] [ -k ] [ -t threads ]\n", argv[0]);
#endif
		exit(EXIT_FAILURE);
	}

#ifdef MYSQL_WITH_SSL
	if (ssl) {
		if (
			strcmp(ssl,(char *)"disabled")
			&&
			strcmp(ssl,(char *)"preferred")
			&&
			strcmp(ssl,(char *)"required")
		) {
			fprintf(stderr,"Invalid SSL setting\n");
			exit(EXIT_FAILURE);
		}
	}
#endif

	int i;
	int rc;
	if (strcmp(host,"localhost")==0) {
		local = 1;
	}
	mysql_library_init(0, NULL, NULL);

	pthread_t *thi=(pthread_t *)malloc(sizeof(pthread_t)*num_threads);
	if (thi==NULL) exit(EXIT_FAILURE);
	unsigned long long start_time=monotonic_time();
	for (i=0; i<num_threads; i++) {
		if ( pthread_create(&thi[i], NULL, my_conn_thread , NULL) != 0 )
    		perror("Thread creation");
	}
	for (i=0; i<num_threads; i++) {
		pthread_join(thi[i], NULL);
	}
	unsigned long long end_time=monotonic_time();
	free(thi);
	if (queries) {
		fprintf(stderr,"Connections[OK/ERR]: (%u,%u) . Queries: (%u,%u) . Clock time: %llums\n", g_connect_OK, g_connect_ERR, g_select_OK, g_select_ERR, (end_time-start_time)/1000);
	} else {
		fprintf(stderr,"Connections[OK/ERR]: (%u,%u) . Clock time: %llums\n", g_connect_OK, g_connect_ERR, (end_time-start_time)/1000);
	}
#ifdef USE_HISTOGRAM
	for (const auto& bin : gh0.axis(0_c)) {
		if (gh0.value(bin.idx)) {
			//fprintf(stdout,"[%.1fms, %.1fms): %u\n", (bin.left/1000), (bin.right/1000), (unsigned int)(gh0.value(bin.idx)));
        	std::cout << "[" << bin.left/1000 << "ms, " << bin.right/1000 << "ms): " << gh0.value(bin.idx) << std::endl;
		}
	}
    for (const auto& bin : gh1.axis(0_c)) {
		if (gh1.value(bin.idx)) {
        	std::cout << "[" << bin.left/1000 << "ms, " << bin.right/1000 << "ms): " << gh1.value(bin.idx) << std::endl;
		}
 	}
    for (const auto& bin : gh2.axis(0_c)) {
		if (gh2.value(bin.idx)) {
        	std::cout << "[" << bin.left << "ms, " << bin.right << "ms): " << gh2.value(bin.idx) << std::endl;
		}
	}
    for (const auto& bin : gh3.axis(0_c)) {
		if (gh3.value(bin.idx)) {
        	std::cout << "[" << bin.left << "ms, " << bin.right << "ms): " << gh3.value(bin.idx) << std::endl;
		}
	}
#endif
	exit(EXIT_SUCCESS);
}
