rm -rf /home/o.koch/image-search/nohup.out
rm -rf /home/o.koch/nohup.out
rm -rf /home/o.koch/image-search/ESTOP

nohup ./cluster_run.sh &
nohup ./aws_watcher.sh &
nohup ./monitor.sh &
nohup ./graphite.sh &

