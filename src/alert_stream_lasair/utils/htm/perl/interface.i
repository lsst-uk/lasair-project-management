%module HTM

%{
#include "../include/SpatialInterface.h"
%}

%include "std_vector.i"
namespace std {
   %template(htmRangeVector) vector<htmRange>;
}

%include "../include/SpatialInterface.h"
