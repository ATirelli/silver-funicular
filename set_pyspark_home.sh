#/bin/bash

PYSPARKHOME=~/.miniconda3/camlin

for file in $(find . -name "launch_*"); do
    sed -i  's/SPHOME/'${PYSPARKHOME}'/g' launch_devices.sh
done
