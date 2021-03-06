# Directory logistics
basedir=$(dirname "$0")
destdir="$basedir/tweets"

# Cleanup
rm -f $destdir/*.dat

# Run stuff
python tweets.py $destdir > /dev/null &

# Trap to finish up
function finish {
    # Killing producer process
    kill $(jobs -p) > /dev/null
    rm -f $destdir/*.dat
}

trap finish SIGINT

# Wait to end
echo "Starting producer processes"
wait $!
