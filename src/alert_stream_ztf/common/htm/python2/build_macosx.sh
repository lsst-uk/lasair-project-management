# Simple script to build the SWIG interface for HTM.  Relies on the HTM static library being precompiled.
# The script requires the following environment variables to be set:

# export CLANG_FLAGS='-Wno-address-of-temporary -D__macosx -Wno-unused-variable -m64 -O2 -UDIAGNOSE'

sh clean.sh
swig -c++ -python htmCircle.i
c++ -c HTMCircleRegion.cpp -fPIC -I../../../../htm/include -Wno-deprecated -Wall -c -g ${CLANG_FLAGS}
c++ -c htmCircle_wrap.cxx -fPIC  -I../../../../htm/include -Wno-deprecated -fno-strict-aliasing -c -g ${CLANG_FLAGS} `python-config --includes`
c++ -shared  -o _htmCircle.so HTMCircleRegion.o htmCircle_wrap.o -lhtm `python-config --ldflags` -L../../../../htm -L`python-config --prefix`/lib
