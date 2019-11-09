#include<stdlib.h>
#include<string.h>
#include<iostream>


#define	FORCE_INLINE inline __attribute__((always_inline))

inline uint32_t rotl32 ( uint32_t x, int8_t r );

inline uint64_t rotl64 ( uint64_t x, int8_t r );

#define	ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)


//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i );
FORCE_INLINE uint64_t getblock64 ( const uint64_t * p, int i );
//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

FORCE_INLINE uint32_t fmix32 ( uint32_t h );
//----------

FORCE_INLINE uint64_t fmix64 ( uint64_t k );

//-----------------------------------------------------------------------------

void MurmurHash3_x86_32 ( const void * key, int len,
                          uint32_t seed, void * out );


// unsigned int MurmurHash(const void* x, int seed);

// unsigned int MurmurHashStr(const std::string x, int seed);
