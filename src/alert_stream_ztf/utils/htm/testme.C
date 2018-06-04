#include <iostream>
#include "SpatialInterface.h"

int main(int argc, char *argv[])
{
  htmInterface htm(12, 5);

  double ra=27.000328;
  double dec=30.653442;

  uint64 id=htm.lookupID(ra, dec);
  cout << "id=" << id << endl;

  std::vector<htmRange> list=htm.circleRegion(ra, dec, 1);
  for(int idx=0; idx<list.size(); idx++)
    cout << list[idx].lo << " " << list[idx].hi << endl;


}
