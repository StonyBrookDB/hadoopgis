prefix=/usr/local

libbase=${HADOOPGIS_LIB_PATH}
incbase=${HADOOPGIS_INC_PATH}

builddir=../build/bin

OPTFLAGS	= -O1 -g
INCFLAGS	= -I $(incbase) -I ./
LIBS		= -lgeos -lspatialindex -lboost_program_options -lCGAL -L $(libbase) -Wl,-rpath,${libbase}

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
 
ifdef DEBUGSTAT
    OPTFLAGS += -DDEBUGSTAT
else
    OPTFLAGS += -DNDEBUGSTAT
endif

ifdef DEBUGAREA
    OPTFLAGS += -DDEBUGAREA
else
    OPTFLAGS += -DNDEBUGAREA
endif
   
CC = gcc
CXX = g++

all: queryproc queryproc3d resque resque3d \
	map_obj_to_tile map_obj_to_tile_3d get_space_dimension \
	get_space_dimension_3d \
	mbb_normalizer_3d collect_tile_stat combine_stat \
	duplicate_remover compute_partition_size \
	sampler hc bsp fg bos fg3d str slc

queryproc: controller/queryproc.cpp \
	common/resque_constants.h \
	common/queryproc_constants.h \
	common/string_constants.h \
	common/partition_structs.h \
	common/queryproc_aux.h \
	common/tokenizer.h
	$(CXX) -std=c++0x -Wno-deprecated $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

queryproc3d: controller/queryproc3d.cpp \
	common/resque_constants.h \
	common/queryproc3d_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

resque: spatialproc/resque.cpp  \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/rtree_traversal.h \
	common/tokenizer.h \
	extensions/specialmeasures/pathology_metrics.h \
	extensions/specialmeasures/geographical.h
	$(CXX) -Wno-deprecated $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

resque3d: spatialproc/resque3d.cpp  \
	extensions/rtree3d/rtree_traversal3d.h \
	common/resque_constants.h \
	common/spatialproc3d_structs.h \
	common/spatialproc3d_functions.hpp \
	common/spatialproc3d_params.h \
	common/string_constants.h \
	common/tokenizer.h 
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

map_obj_to_tile: spatialproc/map_obj_to_tile.cpp \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/rtree_traversal.h \
	common/tokenizer.h \
	common/spatialproc_structs.h
	$(CXX) -std=c++0x -Wno-deprecated $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

containment_proc: spatialproc/containment_proc.cpp \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/rtree_traversal.h \
	common/tokenizer.h \
	common/spatialproc_structs.h
	$(CXX) -std=c++0x -Wno-deprecated $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

collect_tile_stat: partitioner/collect_tile_stat.cpp \
	common/resque_constants.h \
	common/spatialproc_structs.h \
	common/spatialproc_params.h \
	common/string_constants.h \
	common/rtree_traversal.h \
	common/tokenizer.h \
	common/spatialproc_structs.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

combine_stat: partitioner/combine_stat.cpp \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

map_obj_to_tile_3d: spatialproc/map_obj_to_tile_3d.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

get_space_dimension: partitioner/get_space_dimension.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

get_space_dimension_3d: partitioner/get_space_dimension_3d.cpp \
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

mbb_normalizer_3d: partitioner/mbb_normalizer_3d.cpp \
	common/resque_constants.h \
	common/string_constants.h \
	common/tokenizer.h \
	common/partition3d_structs.h \
	common/partition3d_params.h
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
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	common/string_constants.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

fg3d: partitioner/fg/fg3d.cpp \
	common/Timer.hpp \
	common/string_constants.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

hc: partitioner/sfc/hc.cpp \
	common/Timer.hpp \
	common/string_constants.h \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	common/tile_rectangle.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bsp: partitioner/bsp/bsp.cpp \
	common/string_constants.h \
	common/Timer.hpp \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	partitioner/bsp/BinarySplitNode.hpp \
	partitioner/bsp/bsp_structs.hpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

qt: partitioner/qt/qt.cpp \
	common/string_constants.h \
	common/Timer.hpp \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	partitioner/qt/QuadtreeNode.hpp \
	partitioner/qt/qt_structs.hpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bos: partitioner/bos/bos.cpp \
	common/Timer.hpp \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	common/string_constants.h \
	common/tile_rectangle.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

str: partitioner/str/str.cpp \
	common/Timer.hpp \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	common/string_constants.h \
	common/tile_rectangle.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

slc: partitioner/slc/slc.cpp \
	common/Timer.hpp \
	common/tokenizer.h \
	common/partition_structs.h \
	common/partition_params.h \
	common/string_constants.h \
	common/tile_rectangle.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

sampler: partitioner/sampler.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(OPTFLAGS) -o ${builddir}/$@

partition_vis: visualizer/partition_vis.cpp \
	common/Timer.hpp \
	common/tokenizer.h
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

install:
	mkdir -p ${builddir}

clean:
	@rm -f ${builddir}/*

