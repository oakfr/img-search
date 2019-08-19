Author
==========================================================
Olivier Koch, o.koch@criteo.com


Date created
==========================================================
March 2016


Introduction
==========================================================
This is code for a pipeline that compute image features on a hybrid Hadoop/AWS architecture.

The Hadoop architecture hosts recommendation models that leverages image features computed with GPU on AWS.

The pipeline is designed to run continuously as new data arrives:

1. wait for new image URLs to process on Hadoop
2. send the list of URLs to AWS (protobuf)
3. download images and compute features on AWS
4. aggregate features in text files
5. send the data back to Hadoop for further processing


Dependencies
==========================================================
Python >= 2.6

on AWS side:
CUDA >= 7.0
cuDNN
Caffe
numpy
matplotlib


Environment
==========================================================
This code runs on Linux


Installing CUDA on EC2
==========================================================

Important : installing the wrong version of drivers may lead to problems on EC2.

I installed the following version of the NVIDIA drivers : NVIDIA-Linux-x86_64-361.28.run

Make sure that the command nvidia-smi does not hang or fail after installing the drivers.

I installed CUDA 7.0 : 
./cuda_7.0.28_linux.run -extract=`pwd`/nvidia_installers
then run the scripts in nvidia_installers

I installed the following version of the CUDNN drivers: cudnn-7.0-linux-x64-v4.0-prod.tgz

(installing an earlier version of CUDNN makes tensorflow fail)


Emergency recovery
==========================================================
In most cases, problems will occur on the Hadoop gateway.  Check that first, before checking AWS.

Hadoop gateway : Diagnostic
Login into Hadoop gateway using putty
cd to ~/image-search
Run print_logs.sh
Check the logs for errors (nohup.out)
Typical errors are : kerberos error messages, disk full
If you find errors there, follow the procedure below for Hadoop gateway


Hadoop gateway : Reset procedure

Login to Hadoop gateway
Program a graceful stop of all programs by touching the file ESTOP
Wait for processes to be in stop mode (printing ESTOP in log files) - that may take 30 min
Other programs can survive a brutal kill
kill -9 -1 (which will close your terminal)
Log back into Hadoop gateway
Remove the ESTOP file
Run run-all.sh
Look at logs

AWS : Reset procedure

Launch pageant and add the default key
Login into GPUTests using putty
Stop the cron job (crontab -e then comment the only line there)
Check that file /opt/input/_PIPE_RUNNING does not exist, or wait for it to be gone (that may take 30 min)
Restart the machine : sudo shutdown -r now
(very important : do not forget the -r : only Nicolas H can start our instance!)
Log back into AWS
Reactive cron job
Check logs

Example output from print_logs.sh :
2016-03-13 06:59:52	[hathi.py]	Summary : Sent 2429 MB to HDFS
2016-03-13 07:27:37	[hathi.py]	Summary : Sent 2429 MB to HDFS
2016-03-13 08:09:42	[hathi.py]	Summary : Sent 2429 MB to HDFS
2016-03-13 08:16:32	[hathi.py]	Summary : Sent 2429 MB to HDFS
2016-03-13 12:57:44	[hathi.py]	Summary : Sent 2419 MB to HDFS
2016-03-13 07:12:46	[baloo.py]	Summary: transferred 452 files (4 partners) to AWS
2016-03-13 07:16:14	[baloo.py]	Summary: transferred 45 files (4 partners) to AWS
2016-03-13 12:25:17	[baloo.py]	Summary: transferred 215 files (19 partners) to AWS
2016-03-13 12:42:55	[baloo.py]	Summary: transferred 204 files (13 partners) to AWS
2016-03-13 13:09:57	[baloo.py]	Summary: transferred 282 files (10 partners) to AWS
