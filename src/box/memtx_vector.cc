#include "memtx_vector.h"

#include <small/small.h>
#include <small/mempool.h>

#include "index.h"
#include "memtx_index.h"
#include "errinj.h"
#include "fiber.h"
#include "trivia/util.h"

#include "tuple.h"
#include "txn.h"
#include "memtx_tx.h"
#include "space.h"
#include "schema.h"
#include "memtx_engine.h"
#include "../../third_party/USearch/c/usearch.h"

struct memtx_vector_index {
	struct index base;
	unsigned dimension;
	usearch_index_t idx;
};

/**
 * How many neighbours one search asks usearch for. The index API does not
 * pass the select limit down, so this is the upper bound on what a single
 * iterator can return.
 */
#define MEMTX_VECTOR_NEIGHBOURS 32

struct index_vector_iterator {
	struct iterator base;
	/** Neighbours found by the search, nearest first. */
	usearch_key_t keys[MEMTX_VECTOR_NEIGHBOURS];
	/** How many of them were found. */
	size_t count;
	/** Position of the next neighbour to return. */
	size_t pos;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

/**
 * A usearch key is the tuple pointer itself: the index keeps no copy of the
 * data and a search hands the tuples back directly.
 */
static inline usearch_key_t
vector_index_key(struct tuple *tuple)
{
	return (usearch_key_t)(uintptr_t)tuple;
}

/** Report a usearch failure through the tarantool diagnostic area. */
static int
vector_index_diag(usearch_error_t error, const char *what)
{
	if (error == NULL)
		return 0;
	diag_set(ClientError, ER_SYSTEM,
		 tt_sprintf("vector index: %s: %s", what, error));
	return -1;
}

static inline int
mp_decode_num(const char **data, uint32_t fieldno, double *ret)
{
	if (mp_read_double(data, ret) != 0) {
		diag_set(ClientError, ER_FIELD_TYPE,
			 int2str(fieldno + TUPLE_INDEX_BASE),
			 field_type_strs[FIELD_TYPE_NUMBER],
			 mp_type_strs[mp_typeof(**data)]);
		return -1;
	}
	return 0;
}

static inline int
mp_decode_vector(double **vector, unsigned dimension,
	         const char *mp, unsigned count, const char *what)
{
	(void)what;
	double c = 0;
    if (count == dimension) {
        for (unsigned i = 0; i < dimension; i++) {
            if (mp_decode_num(&mp, i, &c) < 0)
                return -1;
            (*vector)[i] = c;
        }
    } else {
		diag_set(ClientError, ER_RTREE_RECT,
			 what, dimension, dimension);
		return -1;
    }
	return 0;
}

static inline int
mp_decode_vector_from_key(double **vector, unsigned dimensions,
			  const char *mp, uint32_t part_count)
{
	if (part_count == 1)
		part_count = mp_decode_array(&mp);
	return mp_decode_vector(vector, dimensions, mp, part_count, "Key");
}

static inline int
extract_vector(double **vector, struct tuple *tuple,
	       struct index_def *index_def)
{
	assert(index_def->key_def->part_count == 1);
	assert(!index_def->key_def->is_multikey);
	const char *elems = tuple_field_by_part(tuple,
				index_def->key_def->parts, MULTIKEY_NONE);
	unsigned dimension = index_def->opts.dimension;
	uint32_t count = mp_decode_array(&elems);
	return mp_decode_vector(vector, dimension, elems, count, "Field");
}

static int
memtx_vector_index_get_internal(struct index *base, const char *key,
			       uint32_t part_count, struct tuple **result,
			       bool is_rw)
{
	/* The body below is a stub, so the read-write flavour of the MVCC
	 * clarification has nothing to apply to yet. */
	(void)is_rw;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;

	double *vector;
	if (mp_decode_vector_from_key(&vector, index->dimension, key, part_count))
		unreachable();

	*result = NULL;

	/*if (!rtree_search(&index->idx, &rect, SOP_OVERLAPS, &iterator)) {
		rtree_iterator_destroy(&iterator);
		return 0;
	}
	do {
		struct tuple *tuple = (struct tuple *)
			rtree_iterator_next(&iterator);
		if (tuple == NULL)
			break;
		struct txn *txn = in_txn();
		struct space *space = space_by_id(base->def->space_id);
		*result = memtx_tx_tuple_clarify(txn, space, tuple, base, 0);
	} while (*result == NULL);*/
	return 0;
}

static int
index_vector_iterator_next(struct iterator *i, struct tuple **ret)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	struct space *space;
	struct index *index;
	index_weak_ref_get_checked(&i->index_ref, &space, &index);
	struct txn *txn = in_txn();

	while (itr->pos < itr->count) {
		struct tuple *tuple =
			(struct tuple *)(uintptr_t)itr->keys[itr->pos++];
		tuple = memtx_tx_tuple_clarify(txn, space, tuple, index, 0);
		if (tuple != NULL) {
			*ret = tuple;
			return 0;
		}
	}
	*ret = NULL;
	return 0;
}

static void
index_vector_iterator_free(struct iterator *i)
{
	struct index_vector_iterator *itr = (struct index_vector_iterator *)i;
	mempool_free(itr->pool, itr);
}

/** Implementation of create_iterator for memtx vector index. */
static struct iterator *
memtx_vector_index_create_iterator(struct index *base, enum iterator_type type,
				  const char *key, uint32_t part_count,
				  const char *pos)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;

	if (pos != NULL) {
		diag_set(UnsupportedIndexFeature, base->def, "pagination");
		return NULL;
	}
	/*
	 * A vector index answers one question: which tuples are nearest to
	 * this vector. Anything else, a full scan included, belongs to
	 * another index of the space.
	 */
	if (type != ITER_EQ || part_count == 0) {
		diag_set(UnsupportedIndexFeature, base->def,
			 "iterator type other than EQ with a vector key");
		return NULL;
	}

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	double *vector = xregion_alloc_array(region, double, index->dimension);
	int rc = mp_decode_vector_from_key(&vector, index->dimension,
					   key, part_count);
	usearch_key_t keys[MEMTX_VECTOR_NEIGHBOURS];
	usearch_distance_t distances[MEMTX_VECTOR_NEIGHBOURS];
	usearch_error_t error = NULL;
	size_t count = 0;
	if (rc == 0) {
		count = usearch_search(index->idx, vector, usearch_scalar_f64_k,
				       MEMTX_VECTOR_NEIGHBOURS, keys, distances,
				       &error);
		rc = vector_index_diag(error, "search");
	}
	region_truncate(region, region_svp);
	if (rc != 0)
		return NULL;

	struct index_vector_iterator *it = (struct index_vector_iterator *)
		mempool_alloc(&memtx->iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct index_vector_iterator),
			 "memtx_vector_index", "iterator");
		return NULL;
	}

	iterator_create(&it->base, base);
	it->pool = &memtx->iterator_pool;
	it->base.next_internal = index_vector_iterator_next;
	it->base.next = memtx_iterator_next;
	it->base.position = generic_iterator_position;
	it->base.free = index_vector_iterator_free;
	memcpy(it->keys, keys, count * sizeof(keys[0]));
	it->count = count;
	it->pos = 0;

	return (struct iterator *)it;
}

/** usearch does not grow on its own: make room before adding a vector. */
static int
memtx_vector_index_reserve_more(struct memtx_vector_index *index)
{
	usearch_error_t error = NULL;
	size_t size = usearch_size(index->idx, &error);
	if (vector_index_diag(error, "size") != 0)
		return -1;
	size_t capacity = usearch_capacity(index->idx, &error);
	if (vector_index_diag(error, "capacity") != 0)
		return -1;
	if (size < capacity)
		return 0;
	usearch_reserve(index->idx, capacity < 64 ? 64 : capacity * 2, &error);
	return vector_index_diag(error, "reserve");
}

static int
memtx_vector_index_replace(struct index *base, struct tuple *old_tuple,
			  struct tuple *new_tuple, enum dup_replace_mode mode,
			  struct tuple **result, struct tuple **successor)
{
	(void)mode;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t error = NULL;

	/* The index is unordered: there is no successor to report. */
	*successor = NULL;

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	double *vector = xregion_alloc_array(region, double, index->dimension);

	/*
	 * Drop the old vector first. The other order would leave the search
	 * able to return a tuple that is already gone if the addition fails.
	 */
	if (old_tuple != NULL) {
		usearch_remove(index->idx, vector_index_key(old_tuple), &error);
		if (vector_index_diag(error, "remove") != 0)
			goto fail;
	}
	if (new_tuple != NULL) {
		if (extract_vector(&vector, new_tuple, base->def) != 0)
			goto fail;
		if (memtx_vector_index_reserve_more(index) != 0)
			goto fail;
		usearch_add(index->idx, vector_index_key(new_tuple), vector,
			    usearch_scalar_f64_k, &error);
		if (vector_index_diag(error, "add") != 0)
			goto fail;
	}

	region_truncate(region, region_svp);
	*result = old_tuple;
	return 0;
fail:
	region_truncate(region, region_svp);
	return -1;
}

static ssize_t
memtx_vector_index_size(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t error = NULL;
	size_t size = usearch_size(index->idx, &error);
	if (vector_index_diag(error, "size") != 0)
		return -1;
	return size;
}

static ssize_t
memtx_vector_index_bsize(struct index *base)
{
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_error_t error = NULL;
	size_t bsize = usearch_memory_usage(index->idx, &error);
	if (vector_index_diag(error, "memory usage") != 0)
		return -1;
	return bsize;
}

static void
memtx_vector_index_destroy(struct index *base)
{
	usearch_error_t error = NULL;
	struct memtx_vector_index *index = (struct memtx_vector_index *)base;
	usearch_free(index->idx, &error);
	free(index);
}

static const struct index_vtab memtx_vector_index_vtab_base = {
	/* .destroy = */ memtx_vector_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ generic_index_update_def,
	/* .depends_on_pk = */ generic_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		generic_index_def_change_requires_rebuild,
	/* .size = */ memtx_vector_index_size,
	/* .bsize = */ memtx_vector_index_bsize,
	/* .quantile = */ generic_index_quantile,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ generic_index_random,
	/* .count = */ generic_index_count,
	/* .get = */ memtx_index_get,
	/* .create_iterator = */ memtx_vector_index_create_iterator,
	/* .create_iterator_with_offset = */
	generic_index_create_iterator_with_offset,
	/* .create_arrow_stream = */ generic_index_create_arrow_stream,
	/* .create_read_view = */ generic_index_create_read_view,
	/* .info = */ generic_index_info,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
};

static const struct memtx_index_vtab memtx_vector_index_vtab = {
	/* .base = */ memtx_vector_index_vtab_base,
	/* .get_internal = */ memtx_vector_index_get_internal,
	/* .replace = */ memtx_vector_index_replace,
	/* .begin_build = */ generic_memtx_index_begin_build,
	/* .reserve = */ generic_memtx_index_reserve,
	/* .build_next = */ generic_memtx_index_build_next,
	/* .end_build = */ generic_memtx_index_end_build,
};
struct index *
memtx_vector_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	assert(def->iid > 0);
	assert(def->key_def->part_count == 1);
	assert(def->key_def->parts[0].type == FIELD_TYPE_ARRAY);
	assert(def->opts.is_unique == false);

	// TODO: check dimension count.
	assert(def->opts.dimension >= 1 && def->opts.dimension < 1000);

	// TODO: try different distance types.

	struct memtx_vector_index *index =
		(struct memtx_vector_index *)xcalloc(1, sizeof(*index));
	index_create(&index->base, (struct engine *)memtx,
		     &memtx_vector_index_vtab.base, def);

	/* Zero first: the rest of the options mean "use the default". */
	usearch_init_options_t opts = {};
	opts.metric_kind = usearch_metric_cos_k;
	opts.quantization = usearch_scalar_f64_k;
	opts.dimensions = (size_t)def->opts.dimension;

	usearch_error_t error = NULL;
	index->idx = usearch_init(&opts, &error);
	if (vector_index_diag(error, "init") != 0) {
		free(index);
		return NULL;
	}

	index->dimension = def->opts.dimension;
	return &index->base;
}
