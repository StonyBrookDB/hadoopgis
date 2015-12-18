# The option -k is not used by the program below, but is required to be passed as the option parser requires it
PARAMOPTS5='-o 3 -i 2 -j 2 -p st_nearest2 -a dataset1 -b dataset2 -k 12 -f 1:1,1:2,2:1,2:2,mindist -c tmpData/denormpartition.idx'
./../build/bin/preprocess ${PARAMOPTS5} < tmpData/inputreducer > knn_idx_file.txt
