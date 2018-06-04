This HTM code was kindly supplied by Richard West at Leicester.

To get it to compile on Linux, modified the Makefile to remove the lookup.o file.

To get it to compile on MacOSX, must modify SpatialGeneral.h and add #define __macosx
near the beginning of the file.

Can optionally comment out the line:

typedef long unsigned int size_t;

from SpatialInterface.h (though it will still compile OK on some platforms)
