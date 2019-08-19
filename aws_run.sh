BALOO_SUCCESS_FILE=/opt/input/.success.baloo
BALOO_SUCCESS_FILE_BAK=/opt/input/.success.baloo.bak
PIPE_RUNNING_FILE=/opt/input/_PIPE_RUNNING

export PATH=$PATH:/usr/local/cuda-7.0/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda-7.0/lib64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/OpenBLAS/lib/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda-7.0/lib64

# clean up old directories
find /opt/old.input/ -type d -mtime +9 -exec rm -rf {} \;
find /opt/old.output/ -type d -mtime +9 -exec rm -rf {} \;
find /opt/old.img/ -type d -mtime +9 -exec rm -rf {} \;
find /opt/output-per-partner/ -type d -mtime +9 -exec rm -rf {} \;

# cap number of lines in log files
tail -10000 /home/ubuntu/image-search/_akela.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/ubuntu/image-search/_akela.py.log
tail -10000 /home/ubuntu/image-search/_mowgli.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/ubuntu/image-search/_mowgli.py.log
tail -10000 /home/ubuntu/image-search/_bagheera.py.log > /tmp/tmp.log; cat /tmp/tmp.log > /home/ubuntu/image-search/_bagheera.py.log
tail -30000 /opt/_aws_run.log > /tmp/tmp.log; cat /tmp/tmp.log > /opt/_aws_run.log

echo 'Trying to run AWS computation...'

if [ -f $PIPE_RUNNING_FILE ];
then
    echo 'Pipeline is already running. Exiting.'
    exit 0
fi

touch $PIPE_RUNNING_FILE
echo 'Launching AWS computation.'
nohup /home/ubuntu/image-search/mowgli.py
#nohup /home/ubuntu/image-search/bagheera.py --caffe
#nohup /home/ubuntu/image-search/akela.py 
rm -rf $PIPE_RUNNING_FILE

