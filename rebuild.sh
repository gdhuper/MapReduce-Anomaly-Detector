#!/bin/bash
if [ ! -d classes ]; then
	mkdir classes;
fi
hadoop fs -rm -R /user/user01/CS185
export CLASSPATH=.:$(hadoop classpath)

javac -d classes MRmapper1.java
javac -d classes MRreducer1.java
javac -d classes MRmapper2.java
javac -d classes MRreducer2.java
jar -cvf CS185LAB1.jar -C classes/ .
javac -classpath $CLASSPATH:CS185LAB1.jar -d classes MRdriver.java
jar -uvf CS185LAB1.jar -C classes/ .

