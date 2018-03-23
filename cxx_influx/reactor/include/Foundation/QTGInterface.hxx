#ifndef QTGInterface_hxx
#define QTGInterface_hxx

#define REACTOR_SIDE

#ifdef DEBUG_ONLY
#undef DEBUG_ONLY // Also defined in Foundation/Debug.h
#endif // DEBUG_ONLY

#ifdef __clang__
// clang does not define a separate type for _mm_hint in xmmintrin.h
using _mm_hint = int;
#endif // __clang__

#include <Foundation/QTGInterface.h>

#endif // QTGInterface_hxx
