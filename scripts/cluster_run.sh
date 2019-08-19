# send images to AWS
while true; do

    # renew kerberos ticket every so often
    krenew

    /home/o.koch/image-search/download_images.py

    sleep 10
    
done

# send computation commands


