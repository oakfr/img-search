# sanity check on tiger
while true; do

    krenew

    # manage disk usage
    tail -30000 /home/o.koch/image-search/_baloo.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/o.koch/image-search/_baloo.py.log
    tail -30000 /home/o.koch/image-search/_hathi.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/o.koch/image-search/_hathi.py.log
    tail -30000 /home/o.koch/image-search/_argus.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/o.koch/image-search/_argus.py.log
    tail -30000 /home/o.koch/image-search/_graphite.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/o.koch/image-search/_graphite.py.log
    tail -30000 /home/o.koch/image-search/nohup.out > /tmp/tmp.log; cat /tmp/tmp.log > /home/o.koch/image-search/nohup.out

    # sent latest log by email
    #tail -n 200 _baloo.py.log _hathi.py.log | sort | tail -n 100 > /tmp/.email.log
    #mail -s "[prod][pa4][aws-tiger] log" o.koch@criteo.com < /tmp/.email.log

    # send error log by email
    tail -n 200 _*.log.error | sort | tail -n 100 > /tmp/.email.error.log
    mail -s "[prod][aws-tiger] error log" o.koch@criteo.com < /tmp/.email.error.log

    /home/o.koch/image-search/argus.py

    sleep 2700

done

# send computation commands


