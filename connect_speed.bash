#!/usr/bin/bash
#
# build multiple versions of proxysql and benchmark
#


####################################################################################################

PORT='$(expr $OFFSET + 6033)'

fn_multiport () {
	local MPORT=""
	for M in $(seq $MULTI); do
		MPORT+="0.0.0.0:$(expr $OFFSET + 6032 + $M);"
	done
	echo "${MPORT::-1}"
}

fn_build () {
	OFFSET=0
	for VER in ${VERS}; do

		let OFFSET+=10000

		#rm -rf proxysql.${VER} || true
		echo "============================================================================================="
		if [[ ! -d proxysql.${VER} ]]; then
			echo "Checking out $VER"
			git clone https://github.com/sysown/proxysql.git proxysql.${VER}
			git -C proxysql.${VER} checkout ${VER} 2> /dev/null
		fi

		pushd proxysql.${VER} &> /dev/null
		if [[ ! -f proxysql ]]; then
			echo "Building ${VER} ..."
			make -j$(nproc) &> /dev/null
		fi

		sed -ir "0,/threads=[0-9]\+/{s//threads=$(expr $NCPUS '/' $MULTI)/}" ./src/proxysql.cfg
		#sed -ir "0,/threads=[0-9]\+/{s//threads=$NCPUS" ./src/proxysql.cfg
		grep 'threads=' src/proxysql.cfg | head -1
		sed -ir "0,/max_connections=[0-9]\+/{s//max_connections=65536/}" ./src/proxysql.cfg
		grep 'max_connections=' src/proxysql.cfg | head -1
		sed -ir "0,/max_user_connections=[0-9]\+/{s//max_user_connections=65536/}" ./src/proxysql.cfg
		grep 'max_user_connections=' src/proxysql.cfg | head -1
		sed -ir "s/\"0.0.0.0:[0-9]\?6032\"/\"0.0.0.0:$(expr $OFFSET + 6032)\"/" ./src/proxysql.cfg
#		sed -ir "s/0.0.0.0:[0-9]\?6033/0.0.0.0:$(expr $OFFSET + 6033);0.0.0.0:$(expr $OFFSET + 6034);0.0.0.0:$(expr $OFFSET + 6035);0.0.0.0:$(expr $OFFSET + 6036)/" ./src/proxysql.cfg
		sed -ir "s/\"0.0.0.0:[0-9]\?6033.\+/\"$(fn_multiport)\"/" ./src/proxysql.cfg
		grep '0.0.0.0' src/proxysql.cfg
		rm -f ./src/*.db

		#./src/proxysql --idle-threads --sqlite3-server -f -c ./src/proxysql.cfg -D ./src &
		./src/proxysql -f -c ./src/proxysql.cfg -D ./src  &> /dev/null &
		sleep 5

		mysql --default-auth=mysql_native_password -u admin -padmin -h 127.0.0.1 -P $(expr $OFFSET + 6032) -e "INSERT INTO mysql_users (username,password) VALUES ('sbtest','sbtest'); LOAD mysql users TO RUNTIME; SAVE mysql users TO DISK;"
#		mysql --default-auth=mysql_native_password -u admin -padmin -h 127.0.0.1 -P $(expr $OFFSET + 6032) -e "SET mysql-interfaces = '0.0.0.0:$(expr $OFFSET + 6033),0.0.0.0:$(expr $OFFSET + 6034),0.0.0.0:$(expr $OFFSET + 6035),0.0.0.0:$(expr $OFFSET + 6036)';"
		mysql --default-auth=mysql_native_password -u admin -padmin -h 127.0.0.1 -P $(expr $OFFSET + 6032) -e "SELECT @@version;" -E

		popd &> /dev/null
	done
}

fn_benchmark () {
	OFFSET=0
	for VER in ${VERS}; do

		SUMM=0
		let OFFSET+=10000

		#CMD="./connect_speed -i 0 -c 1000 -u sbtest -p sbtest -h 127.0.0.1 -P $(expr $OFFSET + 6033) -t 10 -q 0"
		CMD=$(eval echo "${1}")

		echo "============================================================================================="
		echo "Benchmark ${VER} on port $(expr $OFFSET + 6033) ..."
		echo "CMD: 'sleep $PAUSE; ${CMD}'"

		for N in $(seq $LOOP); do

			#grep </proc/net/tcp -c '^ *[0-9]\+: [0-9A-F: ]\{27\} 01 '
			#cat /proc/net/softnet_stat | awk '{print $2}' | grep -v 00000000

			LOG=$($CMD 2>&1 | uniq -c)

#			netstat -ntpa | grep -i WAIT

			#grep </proc/net/tcp -c '^ *[0-9]\+: [0-9A-F: ]\{27\} 01 '
			#cat /proc/net/softnet_stat | awk '{print $2}' | grep -v 00000000

			echo "${LOG}"
			let SUMM+=$(echo ${LOG} | grep -Po '\d+(?=ms)')


			sleep ${PAUSE}
		done
		CONN=$(echo $CMD | grep -Po "(?<=\-c )[0-9]+")
		TRDS=$(echo $CMD | grep -Po "(?<=\-t )[0-9]+")
		TOTAL=$(expr $CONN '*' $TRDS '*' $LOOP)
		CPS=$(expr $TOTAL '*' 1000 '/' $SUMM)
		echo "SUMMARY: ${LOOP} runs, total $TOTAL connections, in ${SUMM}ms, at $CPS conn/s"

	done

	echo "============================================================================================="
}

fn_sysstats () {
	echo "============================================================================================="
	netstat -ntpl | grep 603 | uniq -c
	echo "============================================================================================="
	sysctl -w net.core.somaxconn=65535
	sysctl -w net.core.netdev_max_backlog=65535
	sysctl -w vm.max_map_count=536870912
	echo "============================================================================================="
	ulimit -n 64000
	ulimit -a
}

####################################################################################################
# RUN
####################################################################################################

NCPUS=64
MULTI=8
VERS="v2.3.2 v1.4.16"
#VERS="v2.0.17"
LOOP=1
PAUSE=0

killall proxysql
sleep 5

fn_build
sleep 30

fn_sysstats

#fn_benchmark "./connect_speed -i 0 -c 1000 -u sbtest -p sbtest -h 127.0.0.1 -P $PORT -M $MULTI -t 1 -q 1"
fn_benchmark "./connect_speed -i 0 -c 10000 -u sbtest -p sbtest -h 127.0.0.1 -P $PORT -M $MULTI -t 128 -q 0"

#killall proxysql
