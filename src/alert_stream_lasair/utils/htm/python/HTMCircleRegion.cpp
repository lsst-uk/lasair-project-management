//============================================================================
// Name        : HTMCircleRegion.cpp
// Author      : Ken Smith
// Version     :
// Copyright   :
// Description : Prints out the HTM IDs for a given level, ra, dec and radius
//               Assumes column name is always htm<level>ID
//============================================================================

// 2012-07-31 KWS Added ability to get individual HTMids.  Needed for ingester code.

#include "HTMCircleRegion.h"

std::string htmCircleRegion (size_t level, double ra, double dec, double radius)
{
   // create level index required
   htmInterface htm(level, 5);

   // circleRegion expects radius in arcminutes
   std::vector<htmRange> list=htm.circleRegion(ra, dec, radius/60.0);

   stringstream s;

   // long triangleNumber = 0;
   s << " where (htm" << level << "ID between ";
   for(size_t idx=0; idx<list.size(); idx++){
      if (idx != 0)
         s << "or htm" << level << "ID between ";
      s << list[idx].lo << " and " << list[idx].hi << endl;
      //triangleNumber += ((list[idx].hi - list[idx].lo) + 1);
   }

   s << ")" << endl;

   // s << "Total Triangles = " << triangleNumber << endl;
   return s.str();
}

unsigned long htmID (size_t level, double ra, double dec)
{
   htmInterface htm(level, 5);
   uint64 id=htm.lookupID(ra, dec);
   return id;
}

int main(int argc, char *argv[]) {
   int args              =1;
   size_t level          = atoi(argv[args++]);
   double ra            = atof(argv[args++]);
   double dec           = atof(argv[args++]);
   double radius        = atof(argv[args++]);

   printf("%s", htmCircleRegion(level, ra, dec, radius).c_str());

   return 0;
}
