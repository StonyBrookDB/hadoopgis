PARAMOPTS="-i 2 -j 2 -p st_nearest2 -a dataset1 -b dataset2 -k 12"
PARAMOPTS2="-o 0 "${PARAMOPTS}" -x"
PARAMOPTS3=${PARAMOPTS}" -f 1:1,1:2,2:1,2:2,mindist -c tmpData/denormpartition.idx"
PARAMOPTS4="-o 0 "${PARAMOPTS3}
PARAMOPTS5="-o 3 "${PARAMOPTS3}

echo ""
echo "Use precomputed knn indexes: $1"
ts=$(date +%s%N) 
USE_PRECOMPUTED="$1" ./../build/bin/resque_knn ${PARAMOPTS5} < tmpData/inputreducer > tmpData/outputreducer
#./../build/bin/resque ${PARAMOPTS5} < tmpData/inputreducer > tmpData/outputreducer
tt=$((($(date +%s%N) - $ts)/1000000)) 
echo "Time taken: $tt"

