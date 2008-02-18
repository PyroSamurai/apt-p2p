#!/bin/sh

### BEGIN INIT INFO
# Provides:          apt-dht
# Required-Start:    $network
# Required-Stop:
# Should-Start:      $named
# Should-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: apt helper for peer-to-peer downloads of Debian packages
# Description:       Apt-DHT is a helper for downloading Debian packages
#                    files with APT. It will download any needed files from
#                    other Apt-DHT peers in a bittorrent-like manner, and so
#                    reduce the strain on the Debian mirrors.
### END INIT INFO

# /etc/init.d/apt-dht: start and stop the apt-dht daemon

PATH=/sbin:/bin:/usr/sbin:/usr/bin

rundir=/var/run/apt-dht/ 
pidfile=$rundir/apt-dht.pid 
logfile=/var/log/apt-dht.log
application=/usr/sbin/apt-dht
twistd=/usr/bin/twistd
user=aptdht
group=nogroup

[ -r /etc/default/apt-dht ] && . /etc/default/apt-dht

test -x $twistd || exit 0
test -r $application || exit 0

# return true if at least one pid is alive
alive()
{
    if [ -z "$*" ]; then
        return 1
    fi
    for i in $*; do
        if kill -0 $i 2> /dev/null; then
            return 0
        fi
    done

    return 1
}


case "$1" in
    start)
        echo -n "Starting apt-dht"
        [ ! -d $rundir ] && mkdir $rundir
        [ ! -f $logfile ] && touch $logfile
        chown $user $rundir $logfile 
        [ -f $pidfile ] && chown $user $pidfile
        # Make cache files readable
        umask 022
        start-stop-daemon --start --quiet --exec $twistd -- \
            --pidfile=$pidfile --rundir=$rundir --python=$application \
            --logfile=$logfile --no_save
        echo "."        
    ;;

    stop)
        echo -n "Stopping apt-dht"
        start-stop-daemon --stop --quiet --pidfile $pidfile
        #
        # Continue stopping until daemon finished or time over
        #
        count=0
        pid=$(cat $pidfile 2>/dev/null)
        while alive $pid; do
                if [ $count -gt 20 ]; then
                        echo -n " aborted"
                        break;
                elif [ $count = 1 ]; then
                        echo -n " [wait $count"
                elif [ $count -gt 1 ]; then
                        echo -n " $count"
                fi
                count=$(expr $count + 1)
                sleep 1
                start-stop-daemon --stop --quiet --pidfile $pidfile
        done
        if [ $count -gt 1 ]; then
                echo -n "]"
        fi
        echo "."        
    ;;

    restart)
        $0 stop
        $0 start
    ;;
    
    force-reload)
        $0 restart
    ;;

    *)
        echo "Usage: /etc/init.d/apt-dht {start|stop|restart|force-reload}" >&2
        exit 1
    ;;
esac

exit 0
