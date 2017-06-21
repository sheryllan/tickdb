#! /bin/bash
export RS_BUILDKIT2=/home/ylei/development/Linuxi686/ylei_depot/BuildKit2

export RS_BUILDKIT2=$RS_BUILDKIT

if [ "$1X" == "X" ]; then
    INSTALLDIR=$RS_BUILDKIT/alphafram
else
    INSTALLDIR=$1
fi

COMMON_ARGS="-DCMAKE_INSTALL_PREFIX=${INSTALLDIR} -DCMAKE_CXX_COMPILER=$RS_BUILDKIT/gcc/bin/g++ -DCOLOUR_COMPILER=OFF -DBOOSTROOT=$RS_BUILDKIT/boost/ -DCMAKE_INSTALL_RPATH='\$ORIGIN/../lib'"

rm -rf release
mkdir release
cd release
cmake $COMMON_ARGS -DREACTOR_BUILD=ON -DDEBUG=ON -DCMAKE_BUILD_TYPE=Debug .. && make -j VERBOSE=1 && make install
