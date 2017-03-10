#!/bin/bash
export HADOOP_CLASSPATH=$CLASSPATH:.:$(hadoop classpath)
rm -rf /home/user01/CS185/LAB1/OUT1
rm -rf /home/user01/CS185/LAB1/OUT2
yarn jar CS185LAB1.jar Lab1.MRdriver /user/user01/audit.log /user/user01/CS185/LAB1/OUT1 /user/user01/CS185/LAB1/OUT2 3
