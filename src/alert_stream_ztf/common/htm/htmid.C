//
// Calculate an HTM ID from an RA/dec.

#ifdef STANDARD
#include <stdio.h>
#include <string.h>
#else
#include <my_global.h>
#include <my_sys.h>
#endif
#include <mysql.h>
#include <m_ctype.h>
#include <m_string.h>		// To get strmov()

#define HTM_DEPTH 14

extern "C" {
  my_bool htmid_radec_init(UDF_INIT *, UDF_ARGS *args, char *message);
  long long htmid_radec(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
		     char *error);
  unsigned long long cc_radec2ID(double ra, double dec, int depth);
}

// Initialisation routine
// 
my_bool htmid_radec_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count!=2)
  {
    strcpy(message,"htmid_radec() must have two arguments");
    return 1;
  }

  // Coerce all arguments to be doubles
  for (uint i=0 ; i < args->arg_count; i++)
    args->arg_type[i]=REAL_RESULT;

  // The result cannot be null
  initid->maybe_null=0;
  return 0;
}

// The function itself
//
long long htmid_radec(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
		      char *error)
{
  double ra, dec;

  // Fetch arguments
  ra=*((double*)args->args[0]);
  dec=*((double*)args->args[1]);

  // Calculate the result
  return cc_radec2ID(ra, dec, HTM_DEPTH);
}

