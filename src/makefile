prefix=/usr/local

libbase=${HADOOPGIS_LIB_PATH}
incbase=${HADOOPGIS_INC_PATH}

builddir=../build/bin

OPTFLAGS	= -O2
INCFLAGS	= -I $(incbase) -I ./
LIBS		= -lgeos -lspatialindex -lboost_program_options -L $(libbase)
CGALFLAGS	= -lCGAL -Wl,-rpath,${libbase}

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

all: queryprocessor_2d resque_2d manipulate_2d \
	duplicate_remover \
	combine_stat stats_extract_space_dims  \
	hc_2d bsp_2d fg_2d bos_2d slc_2d qt_2d

queryprocessor_2d: framework/queryprocessor_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

stats_extract_space_dims: utilities/stats_extract_space_dims.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

resque_2d: resque/resque_2d.cpp
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

manipulate_2d: transform/manipulate_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

duplicate_remover: boundaryhandler/duplicate_remover.cpp
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

containment_proc: resque/containment_proc.cpp
	$(CXX) -std=c++0x -Wno-deprecated $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

collect_tile_stat: transform/collect_tile_stat.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

combine_stat: transform/combine_stat.cpp
	$(CXX) $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

fg_2d: partitionalgo/fg/fg_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

qt_2d: partitionalgo/qt/qt_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bos_2d: partitionalgo/bos/bos_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

bsp_2d: partitionalgo/bsp/bsp_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

hc_2d: partitionalgo/sfc/hc_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

slc_2d: partitionalgo/slc/slc_2d.cpp
	$(CXX) -std=c++0x $< $(INCFLAGS) $(LIBS) $(OPTFLAGS) -o ${builddir}/$@

str_2d: partitionalgo/str/str_2d.cpp
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

