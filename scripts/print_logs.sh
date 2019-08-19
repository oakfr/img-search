declare -a LOGFILES=("_hathi.py.log" "_baloo.py.log" "_mowgli.py.log" "_bagheera.py.log" "_akela.py.log")

for LOGFILE in "${LOGFILES[@]}"
do
    if [ -f $LOGFILE ];
    then
        tail -50000 $LOGFILE | grep Summary | tail -10
    fi
done
