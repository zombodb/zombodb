#include "access/htup_details.h"

#define MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage
#define BITS_PER_BITMAPWORD 32
#define PAGES_PER_CHUNK  (BLCKSZ / 32)
#define WORDS_PER_PAGE	((MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1)
#define WORDS_PER_CHUNK  ((PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1)

typedef struct PagetableEntry
{
    BlockNumber blockno;		/* page number (hashtable key) */
    bool		ischunk;		/* T = lossy storage, F = exact */
    bool		recheck;		/* should the tuples be rechecked? */
    bitmapword	words[Max(WORDS_PER_PAGE, WORDS_PER_CHUNK)];
} PagetableEntry;

typedef enum
{
    TBM_EMPTY,					/* no hashtable, nentries == 0 */
    TBM_ONE_PAGE,				/* entry1 contains the single entry */
    TBM_HASH					/* pagetable is valid, entry1 is not */
} TBMStatus;

struct TIDBitmap
{
    NodeTag		type;			/* to make it a valid Node */
    MemoryContext mcxt;			/* memory context containing me */
    TBMStatus	status;			/* see codes above */
    HTAB	   *pagetable;		/* hash table of PagetableEntry's */
    int			nentries;		/* number of entries in pagetable */
    int			maxentries;		/* limit on same to meet maxbytes */
    int			npages;			/* number of exact entries in pagetable */
    int			nchunks;		/* number of lossy entries in pagetable */
    bool		iterating;		/* tbm_begin_iterate called? */
    PagetableEntry entry1;		/* used when status == TBM_ONE_PAGE */
    /* these are valid when iterating is true: */
    PagetableEntry **spages;	/* sorted exact-page list, or NULL */
    PagetableEntry **schunks;	/* sorted lossy-chunk list, or NULL */
};
