#!/bin/bash
# Simple script to build the SWIG interface for HTM.  Relies on the HTM static library being precompiled.

./clean.sh
swig -c++ -python htmCircle.i
g++ -c HTMCircleRegion.cpp -fPIC  -I../include -Wno-deprecated -Wall -c -g -m64 -O2
g++ -c htmCircle_wrap.cxx -fPIC -I../include -Wno-deprecated  -Wall -c -g -m64 -O2 -fno-strict-aliasing `python-config --includes`
g++ -shared  -o _htmCircle.so HTMCircleRegion.o htmCircle_wrap.o -lhtm -L.. -L`python-config --prefix`/lib
