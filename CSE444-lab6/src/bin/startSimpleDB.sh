#!/bin/bash

function setSimpleDBRoot
{
	local this
	local lls
	local link
	local bin

	this="${BASH_SOURCE-$0}"
	while [ -h "$this" ]; do
	  lls=`ls -ld "$this"`
	  link=`expr "$lls" : '.*-> \(.*\)$'`
	  if expr "$link" : '.*/.*' > /dev/null; then
	    this="$link"
	  else
	    this=`dirname "$this"`/"$link"
	  fi
	done

	# convert relative path to absolute path
	bin=`dirname "$this"`
	script=`basename "$this"`
	bin=`cd "$bin"; pwd`
	#this="$bin/$script"

	echo "$bin/..";
}

function isLinux
{
	test "$(uname)" = "Linux"
}

function isMac
{
	test "$(uname)" = "Darwin"
}

function isCygwin
{
	os="$(uname)"
	test "${os:0:6}" = "CYGWIN"
}

function xEnabled
{
	if isCygwin
	then
		return false
	fi

	(xterm -e "") 2>&1 > /dev/null 
}

#--------------------------------------------init--------------------------------------
SIMPLEDB_ROOT=${SIMPLEDB_ROOT="$(setSimpleDBRoot)"}


osName="$(uname)"
if [ "$osName" = Darwin ] || [ "${osName:0:6}" = CYGWIN ] || [ "$osName" = Linux ]
#if [ "$osName" = Darwin ] ||  [ "$osName" = Linux ]
then
	true
else
	echo "Unsupported OS. Currently only Linux, Mac and Windows with \ 
	Cygwin are supported"
	#echo "Unsupported OS. Currently only Linux and Mac OS \ 
	#are supported"
	exit;
fi

if [ $# -lt 1 ]
then
	echo "Usage: ./startSimpleDB.sh catalogFile [-explain] [-f queryFile]"
	exit 1
fi

catalogFile=$(cd $(dirname $1);pwd)/$(basename $1)
shift

if isCygwin
then
	catalogFile="$(cygpath --windows $catalogFile)"
fi

CLASSPATH_SEPARATOR=":"

if isLinux || isCygwin
then
	workerHosts=$(cat "$SIMPLEDB_ROOT/conf/workers.conf" | sed 's/[ \t]\+//g' | \
		      sed 's/#.*$//g' | sed '/^$/d' | sed 's/:[0-9]\+$//g' | sort | uniq)
	workers=$(cat "$SIMPLEDB_ROOT/conf/workers.conf" | sed 's/[ \t]\+//g' | \
		      sed 's/#.*$//g' | sed '/^$/d')
	serverAddr=$(cat "$SIMPLEDB_ROOT/conf/server.conf" | sed 's/[ \t]\+//g' | \
		      sed 's/#.*$//g' | sed '/^$/d')
	if isCygwin
	then
		CLASSPATH_SEPARATOR=";"
	fi
elif isMac
then
	workerHosts=$(cat "$SIMPLEDB_ROOT/conf/workers.conf" | sed -E 's/[ 	]+//g' | \
		      sed -E 's/#.*$//g' | sed -E '/^$/d' | sed -E 's/:[0-9]+//g' | sort | uniq)
	workers=$(cat "$SIMPLEDB_ROOT/conf/workers.conf" | sed -E 's/[ 	]+//g' | \
		      sed -E 's/#.*$//g' | sed -E '/^$/d')
	serverAddr=$(cat "$SIMPLEDB_ROOT/conf/server.conf" | sed -E 's/[ 	]+//g' | \
		      sed -E 's/#.*$//g' | sed -E '/^$/d')
fi


#--------------------------------------------data splitting--------------------------------------
echo "Start splitting data files to worker partitions"

echo "catalogFile is : $catalogFile"

#HeapFileSplitter should store the splitted data files to SIMPLEDB_ROOT/data
#The number of splits equals to the number of workers
#For each worker host:port
#The data for this worker lies in SIMPLEDB_ROOT/data/host_port
#The filename of the catalog is fixed as catalog.schema
cd "$SIMPLEDB_ROOT"; java -classpath "bin/src${CLASSPATH_SEPARATOR}lib/*" \
  simpledb.HeapFileSplitter $catalogFile

chmod -R u+rw,g+rw,o+rw data
chmod -R u+rw,g+rw,o+rw lib
chmod -R u+rw,g+rw,o+rw conf
chmod -R u+rw,g+rw,o+rw bin


#--------------------------------------------sync workers--------------------------------------
echo "Start copying simpledb files to workers"
for host in $workerHosts
do
	echo "Copying to $host"
	ssh $host "export JAVA_HOME=`/usr/libexec/java_home -v '1.6*'`; java -version"
	ssh $host "mkdir -p /tmp/simpledb/data;mkdir /tmp/simpledb/bin;mkdir /tmp/simpledb/lib;mkdir /tmp/simpledb/conf"
	rsync -a "$SIMPLEDB_ROOT/bin/" $host:/tmp/simpledb/bin
	rsync -a "$SIMPLEDB_ROOT/lib/" $host:/tmp/simpledb/lib
	rsync -a "$SIMPLEDB_ROOT/conf/" $host:/tmp/simpledb/conf
	echo "Done"
done
echo "Finish copying simpledb files"

#--------------------------------------------sync data--------------------------------------
echo "Starting copying data files to workers"
for worker in $workers
do
	if isMac
	then
		host=$(echo $worker | sed -E 's/:[0-9]+$//g')
		port=$(echo $worker | sed -E 's/^[^:]+://g')
	else
		host=$(echo $worker | sed 's/:[0-9]\+$//g')
		port=$(echo $worker | sed 's/^[^:]\+://g')
	fi
	echo "Copying to $host:$port"
	ssh $host "mkdir -p /tmp/simpledb/data/$port"
	rsync -a "$SIMPLEDB_ROOT/data/${host}_${port}/" $host:/tmp/simpledb/data/$port
	echo "Done"
done
echo "Finish copying data files"



#--------------------------------------------start workers--------------------------------------
echo "Starting workers"

terminal=xterm
titleOption=-title
if ! [ -z "$(which gnome-terminal)" ]
then
	terminal=gnome-terminal
	titleOption=-t
#elif ! [ -z "$(which konsole)" ]
#then
#	terminal=konsole
fi

for worker in $workers
do 
	if isMac
	then
		host=$(echo $worker | sed -E 's/:[0-9]+$//g') 
		port=$(echo $worker | sed -E 's/^[^:]+://g') 
	else
		host=$(echo $worker | sed 's/:[0-9]\+$//g') 
		port=$(echo $worker | sed 's/^[^:]\+://g') 
	fi
	if isLinux || isCygwin
	then
		if ! xEnabled 
		then
		    (exec ssh $host "cd /tmp/simpledb; java -version; java -classpath \
		    \"bin/src${CLASSPATH_SEPARATOR}lib/*\" simpledb.parallel.Worker ${host}:$port $serverAddr" 2>&1 | \
		    sed "s/^/$host:$port\\$ /g" ) & 
		else
		    ${terminal} ${titleOption} "Worker: $host:$port. Do not close this window when SimpleDB is running." -e \
		    "bash -c \"ssh $host \\\"cd /tmp/simpledb; java -classpath \
		    \\\\\\\"bin/src${CLASSPATH_SEPARATOR}lib/*\\\\\\\" \
		    simpledb.parallel.Worker ${host}:$port $serverAddr\\\" | \
		    sed \\\"s/^/$host:$port\\\\$ /g\\\" \" " & 
		fi
	else
		#mac

		osascript -e "tell app \"Terminal\"
			do script \"echo -e \\\"\\\\033]0;Worker: $host:$port. Do not close this window when SimpleDB is running.\\\\007\\\"; ssh $host \\\"cd /tmp/simpledb; export JAVA_HOME=`/usr/libexec/java_home -v '1.6*'`; java -version;java -classpath \\\\\\\"bin/src:lib/*\\\\\\\" simpledb.parallel.Worker ${host}:$port $serverAddr \\\" \"
		end tell"
	fi

done

#outputStyle="X"
#if ! xEnabled
#then
#	outputStyle="T"
#fi

javaOptions=

if isCygwin
then
	javaOptions=-Djline.terminal=jline.UnixTerminal
fi



#--------------------------------------------start server--------------------------------------
echo "Finish starting workers, now starting the server"
cd "$SIMPLEDB_ROOT"
exec java $javaOptions -classpath "bin/src${CLASSPATH_SEPARATOR}lib/*" simpledb.parallel.Server $catalogFile $*
