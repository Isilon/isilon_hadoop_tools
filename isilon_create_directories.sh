#!/bin/bash
###########################################################################
##  Script to create Hadoop directory structure on Isilon.
##  Must be run on Isilon system as root.
###########################################################################

if [ -z "$BASH_VERSION" ] ; then
   # probably using zsh...
   echo "Script not run from bash -- reinvoking under bash"
   bash "$0"
   exit $?
fi

declare -a ERRORLIST=()

DIST=""
FIXPERM="n"
POSIX="n"
ZONE="System"
CLUSTER_NAME=""
VERBOSE="n"

function banner() {
   echo "##################################################################################"
   echo "## $*"
   echo "##################################################################################"
}

function usage() {
   echo "$0 --dist <cdh|hwx|bi> [--zone <ZONE>] [--fixperm] [--posix-only] [--verbose] [--append-cluster-name <clustername>] "
   exit 1
}

function fatal() {
   echo "FATAL:  $*"
   exit 1
}

function warn() {
   echo "ERROR:  $*"
   ERRORLIST[${#ERRORLIST[@]}]="$*"
}

function yesno() {
   [ -n "$1" ] && myPrompt=">>> $1 (y/n)? "
   [ -n "$1" ] || myPrompt=">>> Please enter yes/no: "
   read -rp "$myPrompt" yn
   [ "z${yn:0:1}" = "zy" -o "z${yn:0:1}" = "zY" ] && return 0
#   exit "DEBUG:  returning false from function yesno"
   return 1
}

function makedir() {
   if [ "z$1" == "z" ] ; then
      echo "ERROR -- function makedir needs directory as an argument"
   else
      mkdir -p $1
   fi
}

function fixperm() {
   if [ "z$1" == "z" ] ; then
      echo "ERROR -- function fixperm needs directory owner group perm as an argument"
   else
      uid=$(getUserUid $2)
      gid=$(getGroupGid $3)
      chown $uid $1
      chown :$gid $1
      if [ "POSIX" == "y" ] ; then
          chmod -D $1
      fi
      chmod $4 $1
   fi
}

function getHdfsRoot() {
    local hdfsroot
    #Check for Version to process correct syntax - isirad
    if [ "`isi version|cut -c 15`" -lt 8 ]; then
      hdfsroot=$(isi zone zones view $1 | grep "HDFS Root Directory:" | cut -f2 -d :)
    else
      hdfsroot=$(isi hdfs settings view --zone=$1 | grep "Root Directory:" | cut -f2 -d :)
    fi
    echo $hdfsroot
}

#Params: Username
#returns: UID
function getUserUid() {
    local uid
    uid=$(isi auth users view --zone $ZONE $1 | grep "  UID" | cut -f2 -d :)
    echo $uid
}

#Params: GroupName
#returns: GID
function getGroupGid() {
    local gid
    gid=$(isi auth groups view --zone $ZONE $1 | grep "  GID:" | cut -f2 -d :)
    echo $gid
}


if [ "`uname`" != "Isilon OneFS" ]; then
   fatal "Script must be run on Isilon cluster as root."
fi

if [ "$USER" != "root" ] ; then
   fatal "Script must be run as root user."
fi

# Parse Command-Line Args
# Allow user to specify what functions to check
while [ "z$1" != "z" ] ; do
    # echo "DEBUG:  Arg loop processing arg $1"
    case "$1" in
      "--dist")
             shift
             DIST="$1"
             echo "Info: Hadoop distribution:  $DIST"
             ;;
      "--zone")
             shift
             ZONE="$1"
             echo "Info: will use users in zone:  $ZONE"
             ;;
      "--fixperm")
             echo "Info: fix permissions and ownership if directories already exist."
             FIXPERM="y"
             ;;
      "--posix-only")
             echo "Info: remove all existing permissions, including ACEs, before setting POSIX permissions."
             POSIX="y"
             ;;
      "--verbose")
             echo "Info: Invoking verbose output."
             VERBOSE="y"
             ;;
      "--append-cluster-name")
             shift
             CLUSTER_NAME="-$1"
             echo "Info: will add clustername to end of usernames: $CLUSTER_NAME"
             ;;
      *)     echo "ERROR -- unknown arg $1"
             usage
             ;;
    esac
    shift;
done

declare -a dirList

# Per-distribution list of folders with permissions and owners
case "$DIST" in
    "cdh")
        # Format is: dirname#perm#owner#group
        dirList=(\
            "/hbase#755#hbase#hbase" \
            "/solr#775#solr#solr" \
            "/tmp#1777#hdfs#supergroup" \
            "/tmp/logs#1777#mapred#hadoop" \
            "/tmp/hive#777#hive#supergroup" \
            "/user#755#hdfs#supergroup" \
            "/user/hdfs#755#hdfs#hdfs" \
            "/user/history#777#mapred#hadoop" \
            "/user/hive#775#hive#hive" \
            "/user/hive/warehouse#1777#hive#hive" \
            "/user/hue#755#hue#hue" \
            "/user/hue/.cloudera_manager_hive_metastore_canary#777#hue#hue" \
            "/user/impala#775#impala#impala" \
            "/user/oozie#775#oozie#oozie" \
            "/user/flume#775#flume#flume" \
            "/user/spark#751#spark#spark" \
            "/user/spark/applicationHistory#1777#spark#spark" \
            "/user/sqoop2#775#sqoop2#sqoop" \
        )
        ;;
    "hwx")
        # Format is: dirname#perm#owner#group
        # The directory list below is good thru HDP 2.4
        dirList=(\
            "/app-logs#777#yarn#hadoop" \
            "/app-logs/ambari-qa#770#ambari-qa#hadoop" \
            "/app-logs/ambari-qa/logs#770#ambari-qa#hadoop" \
            "/tmp#1777#hdfs#hdfs" \
            "/tmp/hive#777#ambari-qa#hdfs" \
            "/apps#755#hdfs#hadoop" \
            "/apps/falcon#777#falcon#hdfs" \
            "/apps/accumulo/#750#accumulo#hadoop" \
            "/apps/hbase#755#hdfs#hadoop" \
            "/apps/hbase/data#775#hbase#hadoop" \
            "/apps/hbase/staging#711#hbase#hadoop" \
            "/apps/hive#755#hdfs#hdfs" \
            "/apps/hive/warehouse#777#hive#hdfs" \
            "/apps/tez#755#tez#hdfs" \
            "/apps/webhcat#755#hcat#hdfs" \
            "/mapred#755#mapred#hadoop" \
            "/mapred/system#755#mapred#hadoop" \
            "/user#755#hdfs#hdfs" \
            "/user/ambari-qa#770#ambari-qa#hdfs" \
            "/user/hcat#755#hcat#hdfs" \
            "/user/hdfs#755#hdfs#hdfs" \
            "/user/hive#700#hive#hdfs" \
            "/user/hue#755#hue#hue" \
            "/user/oozie#775#oozie#hdfs" \
            "/user/yarn#755#yarn#hdfs" \
            "/system/yarn/node-labels#700#yarn#hadoop" \
        )
        ;;
    "bi")
        # Format is: dirname#perm#owner#group
        #The directory list is good thru IBM BI v 4.2
        dirList=(\
            "/tmp#1777#hdfs#hadoop" \
            "/tmp/hive#777#ambari-qa#hadoop"
            "/user#755#hdfs#hadoop" \
            "/iop#755#hdfs#hadoop" \
            "/apps#755#hdfs#hadoop" \
            "/apps/falcon#777#falcon#hadoop" \
            "/apps/accumulo/#750#accumulo#hadoop" \
            "/apps/hbase#755#hdfs#hadoop" \
            "/apps/hbase/data#775#hbase#hadoop" \
            "/apps/hbase/staging#711#hbase#hadoop" \
            "/apps/hive#755#hdfs#hadoop" \
            "/apps/hive/warehouse#777#hive#hadoop" \
            "/apps/tez#755#tez#hadoop" \
            "/apps/webhcat#755#hcat#hadoop" \
            "/app-logs#755#hdfs#hadoop" \
            "/mapred#755#hdfs#hadoop" \
            "/mr-history#755#hdfs#hadoop" \
            "/user/ambari-qa#770#ambari-qa#hadoop" \
            "/user/hcat#775#hcat#hadoop" \
            "/user/hive#775#hive#hadoop" \
            "/user/oozie#775#oozie#hadoop" \
            "/user/yarn#775#yarn#hadoop" \
            "/user/zookeeper#775#zookeeper#hadoop" \
            "/user/uiuser#775#uiuser#hadoop" \
            "/user/spark#775#spark#hadoop" \
            "/user/sqoop#775#sqoop#hadoop" \
            "/user/solr#775#solr#hadoop" \
            "/user/nagios#775#nagios#hadoop" \
            "/user/bigsheets#775#bigsheets#hadoop" \
            "/user/bigsql#775#bigsql#hadoop" \
            "/user/dsmadmin#775#dsmadmin#hadoop" \
            "/user/flume#775#flume#hadoop" \
            "/user/hbase#775#hbase#hadoop" \
            "/user/knox#775#knox#hadoop" \
            "/user/mapred#775#mapred#hadoop" \
            "/user/bigr#775#bigr#hadoop" \
            "/user/bighome#775#bighome#hadoop" \
            "/user/tauser#775#tauser#hadoop" \
        )
        ;;
    *)
        echo "ERROR -- Invalid Hadoop distribution"
        usage
        ;;
esac

HDFSROOT=$(getHdfsRoot $ZONE)
echo "Info: HDFS root dir is $HDFSROOT"

if [ ! -d $HDFSROOT ] ; then
   fatal "HDFS root $HDFSROOT does not exist!"
fi

# MAIN

if [ "$VERBOSE" == "y" ] ; then
   set -x
fi

banner "Creates Hadoop directory structure on Isilon system HDFS."

# Set permissions on HDFS root
fixperm $HDFSROOT "hdfs$CLUSTER_NAME" "hadoop$CLUSTER_NAME" "755"

prefix=0
# Cycle through directory entries comparing owner, group, perm
# Sample output from "ls -dl"  command below
# drwxrwxrwx    8 hdfs  hadoop  1024 Aug 26 03:01 /tmp

for direntry in ${dirList[*]}; do
   read -a specs <<<"$(echo $direntry | sed 's/#/ /g')"

   if [[ ${specs[0]} == /user/* ]] ; then
     IFS='/' read -a path <<<"${specs[0]}"
     old_path="/user/${path[2]}"
     new_path="/user/${path[2]}$CLUSTER_NAME"
     specs[0]="${specs[0]/$old_path/$new_path}"
   fi
   specs[2]="${specs[2]}$CLUSTER_NAME"
   specs[3]="${specs[3]}$CLUSTER_NAME"

   echo "DEBUG: specs dirname ${specs[0]}; perm ${specs[1]}; owner ${specs[2]}; group ${specs[3]}"
   ifspath=$HDFSROOT${specs[0]}
   # echo "DEBUG:  ifspath = $ifspath"

   #  Get info about directory
   if [ ! -d $ifspath ] ; then
      # echo "DEBUG:  making directory $ifspath"
      makedir $ifspath
      fixperm $ifspath ${specs[2]} ${specs[3]} ${specs[1]}
   elif [ "$FIXPERM" == "y" ] ; then
      # echo "DEBUG:  fixing directory perm $ifspath"
      fixperm $ifspath ${specs[2]} ${specs[3]} ${specs[1]}
   else
      warn "Directory $ifspath exists. To set expected permissions use the --fixperm flag"
   fi

done

if [ "${#ERRORLIST[@]}" != "0" ] ; then
   echo "ERRORS FOUND:"
   i=0
   while [ $i -lt ${#ERRORLIST[@]} ]; do
      echo "ERROR:  ${ERRORLIST[$i]}"
      i=$(($i + 1))
   done
   fatal "ERRORS FOUND making Hadoop admin directory structure -- please fix before continuing"
   exit 1
else
   echo "SUCCESS -- Hadoop admin directory structure exists and has correct ownership and permissions"
fi

echo "Done!"
