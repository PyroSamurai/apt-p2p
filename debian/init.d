#!/bin/sh

### BEGIN INIT INFO
# Provides:          apt-p2p
# Required-Start:    $remote_fs $network
# Required-Stop:     $remote_fs
# Should-Start:      $named
# Should-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: apt helper for peer-to-peer downloads of Debian packages
# Description:       Apt-p2p is a helper for downloading Debian packages
#                    files with APT. It will download any needed files from
#                    other Apt-p2p peers in a bittorrent-like manner, and so
#                    reduce the strain on the Debian mirrors.
### END INIT INFO

# /etc/init.d/apt-p2p: start and stop the apt-p2p daemon

PATH=/sbin:/bin:/usr/sbin:/usr/bin

rundir=/var/run/apt-p2p/ 
pidfile=$rundir/apt-p2p.pid 
logfile=/var/log/apt-p2p.log
application=/usr/sbin/apt-p2p
twistd=/usr/bin/twistd
user=apt-p2p
group=nogroup
enable=true

[ -r /etc/default/apt-p2p ] && . /etc/default/apt-p2p

test -x $twistd || exit 0
test -r $application || exit 0

case "x$enable" in
    xtrue|xfalse)   ;;
    *)              echo -n "Value of 'enable' in /etc/default/apt-p2p must be either 'true' or 'false'; "
                    echo "not starting apt-p2p daemon."
                    exit 1
                    ;;
esac

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
        if "$enable"; then
            echo -n "Starting apt-p2p"
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
        else
            echo "apt-p2p daemon not enabled in /etc/default/apt-p2p, not starting..."
        fi
    ;;

    stop)
        echo -n "Stopping apt-p2p"
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
        echo "Usage: /etc/init.d/apt-p2p {start|stop|restart|force-reload}" >&2
        exit 1
    ;;
esac

exit 0
