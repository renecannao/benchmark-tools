#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <mysql/mysql.h>
#include <string.h>
#include <string>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <boost/histogram.hpp>
#include <iostream>
#include <fstream>
#include <mutex>
#include "json.hpp"

using nlohmann::json;

// using histogram from https://github.com/HDembinski/histogram

//namespace bh = boost::histogram;
//using namespace bh::literals;

namespace Color {
    enum Code {
        FG_RED      = 31,
        FG_GREEN    = 32,
        FG_BLUE     = 34,
        FG_DEFAULT  = 39,
        BG_RED      = 41,
        BG_GREEN    = 42,
        BG_BLUE     = 44,
        BG_DEFAULT  = 49
    };
    class Modifier {
        Code code;
        public:
        Modifier(Code pCode) : code(pCode) {}
        friend std::ostream&
            operator<<(std::ostream& os, const Modifier& mod) {
                return os << "\033[" << mod.code << "m";
            }
    };
    class DefColor {
        friend std::ostream&
            operator<<(std::ostream& os, const DefColor& mod) {
                return os << "\033[" << Code::FG_DEFAULT << "m";
            }
    };
    class Green {
        friend std::ostream&
            operator<<(std::ostream& os, const Green& mod) {
                return os << "\033[" << Code::FG_GREEN << "m";
            }
    };
    class Red {
        friend std::ostream&
            operator<<(std::ostream& os, const Red& mod) {
                return os << "\033[" << Code::FG_RED << "m";
            }
    };
}


struct TestCase {
    std::string command;
    json expected_vars;
};

std::vector<TestCase> testCases;

#define MAX_LINE 1024

int readTestCases(const std::string& fileName) {
    FILE* fp = fopen(fileName.c_str(), "r");
    if (!fp) return 0;

    char buf[MAX_LINE], col1[MAX_LINE], col2[MAX_LINE];
    int n = 0;
    for(;;) {
        if (fgets(buf, sizeof(buf), fp) == NULL) break;
        n = sscanf(buf, " \"%[^\"]\", \"%[^\"]\"", col1, col2);
        if (n == 0) break;

        char *p = col2;
        while(*p++) if(*p == '\'') *p = '\"';

        json vars = json::parse(col2);
        testCases.push_back({col1, vars});
    }

    fclose(fp);
    return 1;
}

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

int queries_per_connections=1;
int num_threads=1;
int count=0;
char *username=NULL;
char *password=NULL;
char *host=(char *)"localhost";
int port=3306;
int multiport=1;
char *schema=(char *)"information_schema";
int silent = 0;
int sysbench = 0;
int keep_open=0;
int local=0;
int queries=0;
int uniquequeries=0;
int histograms=-1;
unsigned int g_connect_OK=0;
unsigned int g_connect_ERR=0;
unsigned int g_select_OK=0;
unsigned int g_select_ERR=0;

unsigned int g_passed=0;
unsigned int g_failed=0;

unsigned int status_connections = 0;
unsigned int connect_phase_completed = 0;
unsigned int query_phase_completed = 0;

__thread int g_seed;

inline int fastrand() {
	g_seed = (214013*g_seed+2531011);
	return (g_seed>>16)&0x7FFF;
}

void parseResultJsonColumn(MYSQL_RES *result, json& j) {
        if(!result) return;
        MYSQL_ROW row;

        int num_fields = mysql_num_fields(result);

        while ((row = mysql_fetch_row(result)))
            j = json::parse(row[0]);
}

void parseResult(MYSQL_RES *result, json& j) {
        if(!result) return;
        MYSQL_ROW row;

        int num_fields = mysql_num_fields(result);

        while ((row = mysql_fetch_row(result)))
            j[row[0]] = (row[1] ? row[1] : "NULL");
 }

void dumpResult(MYSQL_RES *result) {
        if(!result) return;
        MYSQL_ROW row;

        int num_fields = mysql_num_fields(result);

        while ((row = mysql_fetch_row(result)))
        {
            for(int i = 0; i < num_fields; i++)
            {
                printf("%s ", row[i] ? row[i] : "NULL");
            }
            printf("\n");
        }
 }

void queryVariables(MYSQL *mysql, json& j, std::stringstream& ss) {
    char *query = (char*)"SELECT * FROM performance_schema.session_variables WHERE variable_name IN "
                         " ('hostname', 'sql_log_bin', 'sql_mode', 'init_connect', 'time_zone', 'autocommit', 'sql_auto_is_null', 'sql_safe_updates', 'session_track_gtids', 'max_join_size', 'net_write_timeout', 'sql_select_limit','character_set_results');";
    if (mysql_query(mysql, query)) {
        if (silent==0) {
            fprintf(stderr,"%s\n", mysql_error(mysql));
        }
    } else {
        MYSQL_RES *result = mysql_store_result(mysql);
        parseResult(result, j);

		ss << "mysql actuals : " << j.dump() << "\n";

        mysql_free_result(result);
        __sync_fetch_and_add(&g_select_OK,1);
    }
}

void queryInternalStatus(MYSQL *mysql, json& j) {
    char *query = (char*)"PROXYSQL INTERNAL SESSION";

    if (mysql_query(mysql, query)) {
        if (silent==0) {
            fprintf(stderr,"%s\n", mysql_error(mysql));
        }
    } else {
        MYSQL_RES *result = mysql_store_result(mysql);
        parseResultJsonColumn(result, j);

        mysql_free_result(result);
        __sync_fetch_and_add(&g_select_OK,1);
    }

    // value types in mysql and in proxysql are different
    // we should convert proxysql values to mysql format to compare
    for (auto& el : j.items()) {
        if (el.key() == "conn") {
			std::string str_value;
			str_value = j["conn"]["sql_safe_updates"];
			std::transform(str_value.begin(), str_value.end(), str_value.begin(), ::toupper);
			j["conn"]["sql_safe_updates"] = str_value;

            // sql_auto_is_null {0|1}
            if (el.value()["sql_auto_is_null"] == "1") {
                el.value().erase("sql_auto_is_null");
                j["conn"]["sql_auto_is_null"] = "ON";
            }
            else if (el.value()["sql_auto_is_null"] == "0") {
                el.value().erase("sql_auto_is_null");
                j["conn"]["sql_auto_is_null"] = "OFF";
            }

			// character_set_results
            if (el.value()["character_set_results"] == "NULL") {
                el.value().erase("character_set_results");
                j["conn"]["character_set_results"] = "";
            }

			// sql_select_limit
            if (el.value()["sql_select_limit"] == "DEFAULT") {
                el.value().erase("sql_select_limit");
                j["conn"]["sql_select_limit"] = "18446744073709551615";
            }

        }
    }
}


void * my_conn_thread(void *arg) {
	g_seed = time(NULL) ^ getpid() ^ pthread_self();
	unsigned int connect_OK=0;
	unsigned int connect_ERR=0;
	unsigned int select_OK=0;
	unsigned int select_ERR=0;
	char arg_on=1;
	int i, j;
	unsigned long long b, e, ce;
	MYSQL **mysqlconns=(MYSQL **)malloc(sizeof(MYSQL *)*count);
	std::vector<json> varsperconn(count);

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
	json vars;
	unsigned int connum;
	for (j=0; j<queries; j++) {
		int fr = fastrand();
		int r1=fr%count;
        int r2=fastrand()%testCases.size();
		std::stringstream ss;
		ss << "Connection number : " << connum << "\n";;

		if (j%queries_per_connections==0) {
			mysql=mysqlconns[r1];
			vars = varsperconn[r1];
			connum = r1;
		}

		ss << testCases[r2].command.c_str() << "\n";

        if (mysql_query(mysql, testCases[r2].command.c_str())) {
            if (silent==0) {
                fprintf(stderr,"%s\n", mysql_error(mysql));
            }
		} else {
			MYSQL_RES *result = mysql_store_result(mysql);
			mysql_free_result(result);
			select_OK++;
			__sync_fetch_and_add(&g_select_OK,1);
		}
        for (auto& el : testCases[r2].expected_vars.items()) {
            vars[el.key()] = el.value();
        }
		ss << "expected: " << vars.dump() << "\n";

		int sleepDelay = fastrand()%100;
		usleep(sleepDelay * 1000);

		char query[128];
		sprintf(query, "SELECT %d;", sleepDelay);
		if (mysql_query(mysql,query)) {
            select_ERR++;
			__sync_fetch_and_add(&g_select_ERR,1);
		} else {
			MYSQL_RES *result = mysql_store_result(mysql);
			mysql_free_result(result);
			select_OK++;
			__sync_fetch_and_add(&g_select_OK,1);
		}


		json mysql_vars;
        queryVariables(mysql, mysql_vars, ss);

        json proxysql_vars;
        queryInternalStatus(mysql, proxysql_vars);

        for (auto& el : vars.items()) {
            auto k = mysql_vars.find(el.key());
            auto s = proxysql_vars["conn"].find(el.key());

            if (k.value() != el.value() || s.value() != el.value()) {
                ss << Color::Red() << "FAIL" << Color::DefColor();
                __sync_fetch_and_add(&g_failed, 1);
            }
            else {
                ss << Color::Green() << "PASS" << Color::DefColor();
                __sync_fetch_and_add(&g_passed, 1);
            }

            std::string s_value;
            if (s.value().is_string())
                s_value = s.value().get<std::string>().c_str();
            if(s.value().is_number())
                s_value = std::to_string(s.value().get<int>());

            ss << " - variable: '" << el.key().c_str() << "', expected: '" << el.value().get<std::string>().c_str()
                        << "', mysql: '" << k.value().get<std::string>().c_str() << "', proxysql: '" << s_value.c_str() << "'\n";

        }

        ss << "\n\n";

		{
			std::mutex mtx;
			std::lock_guard<std::mutex> lock(mtx);
			std::cerr << ss.str();
        }
	}
	__sync_fetch_and_add(&query_phase_completed,1);

	return NULL;
}

int main(int argc, char *argv[]) {
	int opt;
	std::string fileName;

	while ((opt = getopt(argc, argv, "ksYt:b:c:u:p:h:P:D:q:U:M:f:")) != -1) {
		switch (opt) {
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
		case 'f':
		    fileName = optarg;
		    break;
		case 'Y':
			sysbench = 1;
			break;
		default: /* '?' */
			fprintf(stderr, "Usage: %s -c count -u username -p password -f csv_file [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -U target_unique_query ] [ -s ] [ -k ] [ -t threads ] [ -b queries_per_connection ][ -Y ]\n", argv[0]);
			exit(EXIT_FAILURE);
		}
	}
	if (
		(queries_per_connections == 0) ||
		(count == 0) ||
		(username == NULL) ||
		(password == NULL) ||
		(fileName.empty())
	) {
		fprintf(stderr, "Usage: %s -c count -u username -p password -f csv_file [ -h host ] [ -P port ] [ -D schema ] [ -q queries ] [ -U target_unique_query ] [ -s ] [ -k ] [ -t threads ] [ -b queries_per_connections ][ -Y ]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

    if (!readTestCases(fileName)) {
        fprintf(stderr, "Cannot read %s\n", fileName.c_str());
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
/*	{
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
				// std::cerr << "Status : Created " << curr_conn << " total , new " << curr_conn - prev_conn << " connections in "  << double( curr_time - prev_time ) / 1000 << " millisecs. : " << double((curr_conn-prev_conn)*1000000/(curr_time - prev_time)  ) << " Conn/s\n" ;
				i=0;
				prev_conn = curr_conn;
				prev_time = curr_time;

			}
		}
		end_conn = monotonic_time();
		// std::cerr << "Created " << __sync_fetch_and_add(&status_connections,0) << " connections in "  << double( end_conn - begin_conn ) / 1000 << " millisecs.\n" ;
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
				// std::cerr << "Status : Executed " << curr_conn << " total , new " << curr_conn - prev_conn << " queries in "  << double( curr_time - prev_time ) / 1000 << " millisecs. : " << double((curr_conn-prev_conn)*1000000/(curr_time - prev_time)  ) << " QPS\n" ;
				//fprintf(stderr,"Queries [OK/ERR]: %d / %d\n", __sync_fetch_and_add(&g_select_OK,0), __sync_fetch_and_add(&g_select_ERR,0));
				i=0;
				prev_conn = curr_conn;
				prev_time = curr_time;
			}
		}
		end_query = monotonic_time();
		// std::cerr << "Executed " << __sync_fetch_and_add(&g_select_OK,0) << " queries in "  << double( end_query - begin_query ) / 1000 << " millisecs.\n" ;
	}*/
	for (i=0; i<num_threads; i++) {
		pthread_join(thi[i], NULL);
	}
	std::cerr << "Created " << __sync_fetch_and_add(&status_connections,0) << " connections in "  << double( end_conn - begin_conn ) / 1000 << " millisecs.\n" ;
	std::cerr << "Executed " << __sync_fetch_and_add(&g_select_OK,0) << " queries in "  << double( end_query - begin_query ) / 1000 << " millisecs.\n" ;

	std::cerr << Color::Red() << "\n\nTotal FAIL " << __sync_fetch_and_add(&g_failed, 0) << Color::DefColor() << "\n";
	std::cerr << Color::Green() << "Total PASS " << __sync_fetch_and_add(&g_passed, 0) << Color::DefColor() << "\n\n";

	exit(EXIT_SUCCESS);
}
