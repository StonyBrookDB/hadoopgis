#! /bin/bash

if ! [ $# -eq 2 ]
then
    echo "Missing java program name and target directory prefix "
    exit
fi

javafilename=$1
finaljar=$2

mkdir -p ${finaljar}

hadooppath=`hadoop classpath`
#javac -classpath ${hadooppath} -d ${finaljar}/ ${javafilename}
javac -classpath ${hadooppath} -d ${finaljar}/ *.java
jar -cvf ${finaljar}.jar -C ${finaljar}/ .

