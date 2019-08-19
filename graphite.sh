# sanity check on tiger
while true; do

    krenew

    # sent latest log by email
    #tail -n 200 _baloo.py.log _hathi.py.log | sort | tail -n 100 > /tmp/.email.log
    #mail -s "[prod][am5][aws-tiger] log" o.koch@criteo.com < /tmp/.email.log

    /home/o.koch/image-search/graphite.py

    sleep 20

done

# send computation commands


