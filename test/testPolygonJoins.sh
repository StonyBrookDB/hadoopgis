#! /bin/bash

# get include path
source ../hadoopgis.cfg


mkdir -p tmpData

echo -e "Generating data.Geometry field is 2 for both data sets"
./datagenerator/generatePolygons.py 0 1000 10000 20000 500000 6 2 > tmpData/dataset1.tsv
./datagenerator/generatePolygons.py 0 1000 10000 20000 800000 6 2 > tmpData/dataset2.tsv

exit

echo -e "Done generating data\n"

echo -e "Extracting MBBs"

# Change to predicate of your choice
PARAMOPTS="-i 2 -j 2 -p st_intersects -a dataset1 -b dataset2"
#PARAMOPTS=" --shpidx1=2 --shpidx2=2 --predicate=st_intersects --prefix1=dataset1 --prefix2=dataset2"
# other example
# knn for k=3 example
#spjoinparam=" --shpidx1=2 --shpidx2=2 -p st_nearest2 -k 3"

PARAMOPTS2="-o 0 "${PARAMOPTS}" -x"
echo "Params for parameter extraction: ${PARAMOPTS2}"

echo "Extracting mbb from set 1"
export mapreduce_map_input_file="tmpData/dataset1.tsv"
echo ${PARAMOPTS2}
./../build/bin/map_obj_to_tile ${PARAMOPTS2} < tmpData/dataset1.tsv > tmpData/dataset1.mbb
echo "Done extracting mbb from set 1\n"

echo "Extracting mbb from set 2"
export mapreduce_map_input_file="tmpData/dataset2.tsv"
./../build/bin/map_obj_to_tile ${PARAMOPTS2} < tmpData/dataset2.tsv > tmpData/dataset2.mbb
echo "Done extracting mbb from set 2\n"

echo "Getting space dimension"
# In MapReduce this needs to call "get_space_dimension 2" in Mapper phase and "get_space_dimension 1" in Reducer phase
cat tmpData/dataset1.mbb tmpData/dataset2.mbb | ./../build/bin/get_space_dimension 2 > tmpData/spaceDims.cfg

echo "Done getting space dimension\n"

# Reading space dimension from output file
read min_x min_y max_x max_y object_count <<< `cat tmpData/spaceDims.cfg`
read total_size <<< `du -cs tmpData/dataset1.tsv tmpData/dataset2.tsv | tail -n 1 | cut -f1`
echo "min_x=${min_x}"
echo "min_y:${min_y}"
echo "max_x:${max_x}"
echo "max_y:${max_y}"
echo "object_count:${object_count}"
echo "total_size:${total_size}"

echo "Computing partition size for block 4MB (data per tile)"
../build/bin/compute_partition_size 10000 1.0 ${total_size} ${object_count} > tmpData/partition_size.cfg
cat tmpData/partition_size.cfg
source tmpData/partition_size.cfg

echo "Normalizing MBBs"
PARTITIONPARAM=" -a ${min_x} -b ${min_y} -c ${max_x} -d ${max_y} -f 1 -n"
cat tmpData/dataset1.mbb tmpData/dataset2.mbb | ../build/bin/mbb_normalizer ${PARTITIONPARAM} > tmpData/normalizedMbbs

echo "Partitioning"
cat tmpData/normalizedMbbs | ../build/bin/bsp -b ${partitionSize} > tmpData/partition.idx
## Different partition methods available
#cat tmpData/normalizedMbbs | ../build/bin/hc -b ${partitionSize} > tmpData/partition.idx
#cat tmpData/normalizedMbbs | ../build/bin/fg -b ${partitionSize} > tmpData/partition.idx
#cat tmpData/normalizedMbbs | ../build/bin/bos -b ${partitionSize} > tmpData/partition.idx

echo -e "Done partitioning\n"

echo "Denormalizing"
DENORMPARAM=" -a ${min_x} -b ${min_y} -c ${max_x} -d ${max_y} -f 1 -o"
cat tmpData/partition.idx | ../build/bin/mbb_normalizer ${DENORMPARAM} > tmpData/denormpartition.idx
echo "Done denormalizing\n"

echo "Map object to tile: "
PARAMOPTS3=${PARAMOPTS}" -f 1:1,1:2,2:1,2:2,mindist -c tmpData/denormpartition.idx"

#offset is 1 for mapper
PARAMOPTS4="-o 0 "${PARAMOPTS3}
echo "Mapping object from data set 1:"
export mapreduce_map_input_file="tmpData/dataset1.tsv"
echo ${PARAMOPTS3}
./../build/bin/map_obj_to_tile ${PARAMOPTS4} < tmpData/dataset1.tsv > tmpData/outputmapper1
echo "Done mapping data set 1\n"


echo "Mapping object from data set 2:"
export mapreduce_map_input_file="tmpData/dataset2.tsv"
echo ${PARAMOPTS3}
./../build/bin/map_obj_to_tile ${PARAMOPTS4} < tmpData/dataset2.tsv > tmpData/outputmapper2
echo -e "Done mapping data set 1\n"

# Simulate MapReduce sorting
echo "Shuffling/sorting"
sort tmpData/outputmapper1 tmpData/outputmapper2 > tmpData/inputreducer 
echo "Done with shuffling/sorting"

echo "Performing spatial processing"
# Perform reducer/resque processing
# Offset is 3 for reducer (join index and legacy field)
PARAMOPTS5="-o 3 "${PARAMOPTS3}
echo ${PARAMOPTS5}
./../build/bin/resque ${PARAMOPTS5} < tmpData/inputreducer > tmpData/outputreducer
echo "Done with spatial processing"

# Simulate MapReduce sorting
echo "Performing duplicate removal"
sort tmpData/outputreducer > tmpData/inputmapperboundary

../build/bin/duplicate_remover uniq < tmpData/inputmapperboundary > tmpData/finalOutput


