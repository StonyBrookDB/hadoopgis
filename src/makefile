prefix=/usr/local

libbase=${HADOOPGIS_LIB_PATH}
incbase=${HADOOPGIS_INC_PATH}

builddir=../build/bin

OPTFLAGS	= -O2
INCFLAGS	= -I $(incbase)
LIBS		= -lgeos -lspatialindex -lboost_program_options -L $(libbase)

ifdef DEBUG
    OPTFLAGS += -DDEBUG -g
else
    OPTFLAGS += -DNDEBUG
endif

ifdef DEBUGTIME
    OPTFLAGS += -DDEBUGTIME
else
    OPTFLAGS += -DNDEBUGTIME
endif
 
   
CC = gcc
CXX = g++

all: queryproc resque map_obj_to_tile get_space_dimension \
	mbb_normalizer duplicate_remover compute_partition_size \
	hc bsp fg bos

queryproc: controller/queryproc.cpp \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

resque: spatialproc/resque.cpp  \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/rtree_traversal.h \
	common/tokenizer.h \
	extensions/specialmeasures/pathology_metrics.h \
	extensions/specialmeasures/geographical.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

map_obj_to_tile: spatialproc/map_obj_to_tile.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

get_space_dimension: partitioner/get_space_dimension.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

mbb_normalizer: partitioner/mbb_normalizer.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

duplicate_remover: boundaryhandler/duplicate_remover.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

compute_partition_size: partitioner/compute_partition_size.cpp
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

fg: partitioner/fg/fg.cpp \
	common/Timer.hpp \
	common/string_constants.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

hc: partitioner/sfc/hc.cpp \
	common/Timer.hpp \
	common/string_constants.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bsp: partitioner/bsp/bsp.cpp \
	common/string_constants.h \
	common/Timer.hpp \
	partitioner/bsp/BinarySplitNode.hpp \
	partitioner/bsp/bsp_structs.hpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bos: partitioner/bos/bos.cpp \
	common/rtree_traversal.h \
	common/Timer.hpp \
	common/string_constants.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

install:
	mkdir -p ${builddir}

clean:
	@rm -f ${builddir}/*

