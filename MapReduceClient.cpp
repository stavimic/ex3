#include "MapReduceClient.h"




// gets a single pair (K1, V1) and calls emit2(K2,V2, context) any
// number of times to output (K2, V2) pairs.
void map(const K1* key, const V1* value, void* context)
{}

// gets a single K2 key and a vector of all its respective V2 values
// calls emit3(K3, V3, context) any number of times (usually once)
// to output (K3, V3) pairs.
void reduce(const IntermediateVec* pairs, void* context){}


