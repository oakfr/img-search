# send images to AWS
while true; do

    # renew kerberos ticket every so often
    krenew

    /home/o.koch/image-search/baloo.py

    # This does not work -- cuda library load error when launching the job remotely
    #echo 'Launching computation on AWS'
    #ssh ubuntu@176.34.228.64 'nohup /home/ubuntu/image-search/aws_run.sh'

    sleep 10
    
done

# send computation commands


