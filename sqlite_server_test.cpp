#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
//#include <mysql/mysql.h>
#include <mysql.h>
//#include <mysql/my_config.h>
//#include <my_config.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <assert.h>

#include <iostream>
#include <algorithm>
#include <vector>
#include <atomic>

#if MYSQL_VERSION_MAJOR > 5
#define MYSQL_WITH_SSL
#endif 

#ifndef MYSQL_WITH_SSL
#if MYSQL_VERSION_MAJOR == 5 && MYSQL_VERSION_MINOR == 7 && MYSQL_VERSION_PATCH > 10
#define MYSQL_WITH_SSL
#endif 
#endif

#define NUM_INT_COLS 3

using namespace std;

unsigned long long monotonic_time() {
	struct timespec ts;
	//clock_gettime(CLOCK_MONOTONIC_COARSE, &ts); // this is faster, but not precise
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (((unsigned long long) ts.tv_sec) * 1000000) + (ts.tv_nsec / 1000);
}

#define NUM_TABLES 16

pthread_mutex_t mutex;

int interval_ms=-1;
int num_threads=1;
int num_users=0;
int conn_count=0;
char *username=NULL;
char *password=NULL;
char *host=(char *)"localhost";
int port=3306;
char *schema=(char *)"information_schema";
int silent;
int local=0;
int queries=0;
std::atomic<unsigned int> g_connect_OK; 
std::atomic<unsigned int> g_connect_ERR; 
std::atomic<unsigned int> g_select_OK; 
std::atomic<unsigned int> g_select_ERR; 
std::atomic<unsigned int> g_dml_OK; 
std::atomic<unsigned int> g_dml_ERR; 
//unsigned int g_connect_OK=0;
//unsigned int g_connect_ERR=0;
//unsigned int g_select_OK=0;
//unsigned int g_select_ERR=0;
#ifdef MYSQL_WITH_SSL
char *ssl = NULL;
#endif
char *auth = NULL;

bool run_dml(string& query, MYSQL *mysql) {
__retry:
	if (mysql_query(mysql,query.c_str())) {
		if (silent==0) {
			fprintf(stderr,"%s\n", mysql_error(mysql));
		}
		g_dml_ERR++;
		if (strcmp(mysql_error(mysql),(char *)"database is locked")==0) {
			goto __retry;
		}
		return false;
	} else {
		g_dml_OK++;
	}
	return true;
}

bool run_select(string& query, MYSQL *mysql, int *ret) {
	if (mysql_query(mysql,query.c_str())) {
		if (silent==0) {
			fprintf(stderr,"%s\n", mysql_error(mysql));
		}
		g_select_ERR++;
		return false;
	} else {
		MYSQL_RES *result = mysql_store_result(mysql);
		if (ret) {
			if (mysql_num_rows(result)) {
				MYSQL_ROW row = mysql_fetch_row(result);
				*ret = atoi(row[0]);
			} else {
				*ret = 0;
			}
		}
		mysql_free_result(result);
		g_select_OK++;
	}
	return true;
}

MYSQL * create_connection() {
	char * _username = NULL;
	char * _password = NULL;
	MYSQL *mysql=mysql_init(NULL);
	//b=monotonic_time(); // begin
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
#endif
	if (auth) {
		int rc = 0;
		rc = mysql_options(mysql, MYSQL_DEFAULT_AUTH, auth);
		//rc = mysql_options(mysql, MYSQL_DEFAULT_AUTH, "mysql_native_password");
		if (rc) {
			fprintf(stderr, "Unable to set auth plugin %s\n", mysql_error(mysql));
			exit(EXIT_FAILURE);
		}
	}
	if (num_users) {
		_username = (char *)malloc(strlen(username)+128);
		_password = (char *)malloc(strlen(password)+128);
		int id = rand() % num_users;
		id++;
		sprintf(_username,"%s%d",username,id);	
		sprintf(_password,"%s%d",password,id);	
	} else {
		_username = username;
		_password = password;
	}
	mysql_options(mysql, MYSQL_DEFAULT_AUTH, "mysql_native_password");
	MYSQL *rc=mysql_real_connect(mysql, host, _username, _password, schema, (local ? 0 : port), NULL, 0);
	if (num_users) {
		free(_username);
		free(_password);	
	}
	if (rc) {
		char arg_on=1;
		setsockopt(mysql->net.fd, SOL_SOCKET, SO_REUSEADDR, (char *)&arg_on, sizeof(arg_on));
		g_connect_OK++;
	}
	assert(rc);
	return rc;
}

void * my_conn_thread(void *arg) {
	//unsigned int connect_OK=0;
	//unsigned int connect_ERR=0;
	//unsigned int select_OK=0;
	//unsigned int select_ERR=0;
	char * _username = NULL;
	char * _password = NULL;
	int i, j;
	char query[128];
	unsigned long long b, e, ce;
	//MYSQL **mysqlconns = NULL;
	vector<MYSQL *> mysqlconns;
	///mysqlconns=(MYSQL **)malloc(sizeof(MYSQL *)*count);
	MYSQL *mysql = NULL;
	for (i=0; i<conn_count/2; i++) {
		mysql = create_connection();
		mysqlconns.push_back(mysql);
	}
	bool rb;
	for (j=0; j<queries; j++) {
		if (interval_ms) {
				usleep(interval_ms * 1000 * (1+rand()%3));
		}
		int randc = rand();
		if (randc%200 == 0) { // change number of connections
			int n_c = rand()%conn_count + 1;
			//cout << n_c << " " << mysqlconns.size() << endl;
			std::random_shuffle ( mysqlconns.begin(), mysqlconns.end() ); // randomize them
			while (n_c > mysqlconns.size()) {
				MYSQL *mysql = create_connection();
				mysqlconns.push_back(mysql);
			}
			while (n_c < mysqlconns.size()) {
				MYSQL *mysql = mysqlconns.back();
				mysqlconns.pop_back();
				mysql_close(mysql);
			}
		}
		string query;
		int cidx = rand()%mysqlconns.size(); // a random connection
		int id, id2;
		mysql = mysqlconns[cidx];
		int qt = randc%100;
		int hg = 100;
		if (num_users < 20) {
			if ((randc+j) > 0) {
				hg += (randc+j)%num_users;
			} else {
				hg += (randc)%num_users;
			}
		} else {
			if ((randc+j) > 0) {
				hg += (randc+j)%20;
			} else {
				hg += (randc)%20;
			}
		}
		string db = "main_" + to_string(101+randc%num_users);
		mysql_select_db(mysql,db.c_str());
		// for now transactions aren't completely working
//		if (randc%30==0) {
//			query = "START TRANSACTION";
//			rb = run_dml(query, mysql); assert(rb);
//		}
		if (qt < 5) {
			int tn = rand()%NUM_TABLES;
			query = "SELECT /* hostgroup=" + to_string(hg) + " */ id FROM test" + to_string(tn) + " ORDER BY RANDOM() LIMIT 1";
			rb = run_select(query, mysql, &id); assert(rb);
			if (id) {
				query = "DELETE /* hostgroup=" + to_string(hg) + " */ FROM test" + to_string(tn) + " WHERE id BETWEEN " + to_string(id) + " AND " + to_string(id+5);
				rb = run_dml(query, mysql); assert(rb);
			}
		} else if (qt < 15) {
			id = rand()%1000;
			int ne = id%NUM_INT_COLS;
			query = "INSERT /* hostgroup=" + to_string(hg) + " */ INTO test" + to_string(rand()%NUM_TABLES) + " (id";
			for (int i=0; i<ne; i++) {
				query += ",id" + to_string(i+1);
			}
			query += ") VALUES (null";
			for (int i=0; i<ne; i++) {
				query += "," + to_string(id%(7+i));
			}
			query += ")";
			rb = run_dml(query, mysql); assert(rb);
		} else if (qt < 30) {
			int tn = rand()%NUM_TABLES;
			query = "SELECT /* hostgroup=" + to_string(hg) + " */ id FROM test" + to_string(tn) + " ORDER BY RANDOM() LIMIT 1";
			rb = run_select(query, mysql, &id); assert(rb);
			if (id) {
				id2 = rand()%1000;
				query = "UPDATE /* hostgroup=" + to_string(hg) + " */ test" + to_string(tn) + " SET id=id";
				int ne = id%NUM_INT_COLS;
				vector<int> ids;
				for (int i=0; i<ne; i++) {
					ids.push_back(i+1);
				}
				std::random_shuffle ( ids.begin(), ids.end() ); // randomize them
				for (int i=0; i<ne; i++) {
					query += ", id" + to_string(ids[i]) + "=" + to_string(id%(7+i));
				}
				query += " WHERE id BETWEEN " + to_string(id) + " AND " + to_string(id+3);
				rb = run_dml(query, mysql); assert(rb);
			}
		} else {
			int tn = rand()%NUM_TABLES;
			int ne = rand()%6;
			query = "SELECT /* hostgroup=" + to_string(hg) + " */ id";
			for (int i=0; i<ne; i++) {
				query += ",id" + to_string(rand()%NUM_INT_COLS+1);
			}
			query += " FROM test" + to_string(tn) + " ORDER BY id DESC LIMIT 1";
			rb = run_select(query, mysql, &id); assert(rb);
			if (id) {
				id2 = rand()%id;
			} else {
				id2 = 0;
			}
			query = "SELECT /* hostgroup=" + to_string(hg) + " */ id";
			ne = rand()%6;
			for (int i=0; i<ne; i++) {
				query += ",id" + to_string(rand()%NUM_INT_COLS+1);
			}
			query += " FROM test" + to_string(tn) + " WHERE id > " + to_string(id2);
			ne = rand()%6;
			for (int i=0; i<ne; i++) {
				query += " AND id" + to_string(rand()%NUM_INT_COLS+1);
				query += " > 0";
			}
			query += " ORDER BY id LIMIT " + to_string(id%100+1);
			rb = run_select(query, mysql, NULL); assert(rb);
		}
		// for now transactions aren't completely working
//		if (randc%30==0) {
//			query = "COMMIT";
//			rb = run_dml(query, mysql); assert(rb);
//		}
	}
	while (mysqlconns.size()) {
		MYSQL *mysql = mysqlconns.back();
		mysqlconns.pop_back();
		mysql_close(mysql);
	}
	return NULL;
}

int main(int argc, char *argv[]) {
	g_connect_OK=0;
	g_connect_ERR=0;
	g_select_OK=0;
	g_select_ERR=0;
	g_dml_OK=0;
	g_dml_ERR=0;
	pthread_mutex_init(&mutex,NULL);
	int opt;
#ifdef MYSQL_WITH_SSL
	while ((opt = getopt(argc, argv, "st:i:c:u:p:h:P:D:q:U:S:a:")) != -1) {
#else
	while ((opt = getopt(argc, argv, "st:i:c:u:p:h:P:D:q:U:a:")) != -1) {
#endif
		switch (opt) {
		case 't':
			num_threads = atoi(optarg);
			break;
		case 'U':
			num_users = atoi(optarg);
			break;
		case 'i':
			interval_ms = atoi(optarg);
			break;
		case 'c':
			conn_count = atoi(optarg);
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
#endif
		case 'a':
			auth = strdup(optarg);
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
		case 's':
			silent = 1;
			break;
		default: /* '?' */
#ifdef MYSQL_WITH_SSL
			fprintf(stderr, "Usage: %s -i interval_ms -c count -u username -p password [ -U num_users ] [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -s ] [ -t threads ][ -S {disabled|preferred|required} ][ -a auth_plugin ]\n", argv[0]);
#else
			fprintf(stderr, "Usage: %s -i interval_ms -c count -u username -p password [ -U num_users ] [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -s ] [ -t threads ]\n", argv[0]);
#endif
			exit(EXIT_FAILURE);
		}
	}
	if (
		(interval_ms == -1) ||
		(conn_count == 0) ||
		(username == NULL) ||
		(password == NULL)
	) {
#ifdef MYSQL_WITH_SSL
		fprintf(stderr, "Usage: %s -i interval_ms -c count -u username -p password [ -U num_users ] [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -s ] [ -t threads ][ -S {disabled|preferred|required} ][ -a auth_plugin ]\n", argv[0]);
#else
		fprintf(stderr, "Usage: %s -i interval_ms -c count -u username -p password [ -U num_users ] [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -s ] [ -t threads ]\n", argv[0]);
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

	MYSQL *mysql = create_connection();
	for (i=0; i<NUM_TABLES; i++) {
		bool rb;
		string query;
		query = "DROP TABLE IF EXISTS test" + to_string(i);
		rb = run_dml(query, mysql); assert(rb);
		usleep(50000);
		query = "CREATE TABLE test" + to_string(i) + " (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL";
		for (int i=0;i<NUM_INT_COLS; i++) {
			query += ", id" + to_string(i+1) + " INT";
		}
		query += ")";
		rb = run_dml(query, mysql); assert(rb);
		usleep(50000);
	}
	mysql_close(mysql);

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
	unsigned int  g_connect_OK_ =  g_connect_OK;
	unsigned int  g_connect_ERR_ =  g_connect_ERR;
	unsigned int  g_select_OK_ =  g_select_OK + g_dml_OK;
	unsigned int  g_select_ERR_ =  g_select_ERR + g_dml_ERR;
	if (queries) {
		fprintf(stderr,"Connections[OK/ERR]: (%u,%u) . Queries: (%u,%u) . Clock time: %llums\n", g_connect_OK_, g_connect_ERR_, g_select_OK_, g_select_ERR_, (end_time-start_time)/1000);
	} else {
		fprintf(stderr,"Connections[OK/ERR]: (%u,%u) . Clock time: %llums\n", g_connect_OK_, g_connect_ERR_, (end_time-start_time)/1000);
	}

	exit(EXIT_SUCCESS);
}
