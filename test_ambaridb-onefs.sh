#!/bin/sh

# Usage info
show_help() {
cat << EOF
Usage: ${0##*/} [-o ONEFSHOST]
Execute on host with Ambari Server to correct the host and cluster state due to
bug in OneFS 8.0.0.3 and 8.0.1.0 when used with Ambari 2.4.x.

    -o ONEFSHOST   The OneFS host name as it appears in Ambari Server.
EOF
}

ONEFSHOSTNAME=""

OPTIND=1

while getopts ho: opt; do
    case $opt in
        h)
            show_help
            exit 0
            ;;
        o)  ONEFSHOSTNAME=$OPTARG
            ;;
        *)
            show_help >&2
            exit 1
            ;;
    esac
done

if [ -z "$ONEFSHOSTNAME" ]; then
    show_help
    exit 0
fi

echo "Beginning review and edits of Ambari database for OneFS host $ONEFSHOSTNAME."
export PGPASSWORD=bigdata
if [ -f /var/run/ambari-server/ambari-server.pid ]; then
    AMBARION="true"
else
    AMBARION="false"
fi
ONEFSHOSTID=$(psql -At -U ambari -c "SELECT host_id from hosts WHERE host_name='$ONEFSHOSTNAME';")

if [ -z "$ONEFSHOSTID" ]; then
    echo "The Ambari database does not have a host registered with the name $ONEFSHOSTNAME"
    exit 0
fi

ONEFSHOSTSTATE=$(psql -At -U ambari -c "SELECT state from host_version WHERE host_id='$ONEFSHOSTID';")
echo "OneFS state is $ONEFSHOSTSTATE."

if [ "$ONEFSHOSTSTATE" != "CURRENT" ]; then
    if [ $AMBARION = "true" ]; then
        echo "Turning off Ambari Server..."
        ambari-server stop
        AMBARION="false"
    fi
    echo "Correcting OneFS state to CURRENT..."
    psql -At -U ambari -c "UPDATE host_version SET state='CURRENT' WHERE host_id='$ONEFSHOSTID';"
    ONEFSHOSTSTATE=$(psql -At -U ambari -c "SELECT state from host_version WHERE host_id='$ONEFSHOSTID';")
    echo "OneFS state is $ONEFSHOSTSTATE."
fi

ONEFSCLUSTER=$(psql -At -U ambari -c "SELECT cluster_id from clusterhostmapping WHERE host_id='$ONEFSHOSTID';")
ONEFSCLUSTERSTATE=$(psql -At -U ambari -c "SELECT state from cluster_version WHERE cluster_id='$ONEFSCLUSTER';")
echo "The Hadoop cluster with OneFS state is $ONEFSCLUSTERSTATE."

if [ "$ONEFSCLUSTERSTATE" != "CURRENT" ]; then
    if [ $AMBARION = "true" ]; then
        echo "Turning off Ambari Server..."
        ambari-server stop
        AMBARION="false"
    fi
    echo "Correcting Hadoop cluster state to CURRENT..."
    psql -At -U ambari -c "UPDATE cluster_version SET state='CURRENT' WHERE cluster_id='$ONEFSCLUSTER';"
    ONEFSCLUSTERSTATE=$(psql -At -U ambari -c "SELECT state from cluster_version WHERE cluster_id='$ONEFSCLUSTER';")
    echo "The Hadoop cluster with OneFS state is $ONEFSCLUSTERSTATE."
fi

echo "Completed review of Ambari database."

if [ $AMBARION = "false" ]; then
    echo "Turning on Ambari Server..."
    ambari-server start
    AMBARION="true"
fi
