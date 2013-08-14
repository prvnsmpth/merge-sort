#ifndef HEAP_H
#define HEAP_H

#include <stdlib.h>

#define heap_front(h) ((h)->data[0])
#define DEFAULT_SIZE 16

typedef int (*comparison_fn_t) (const void *, const void *);

typedef struct {
	unsigned int size;
	unsigned int count;
	comparison_fn_t comparator;
	void **data;
} heap;


void heap_init(heap *h, comparison_fn_t comparator);
void heap_push(heap *h, void *elem);
void *heap_pop(heap *h);
void heapify(void **arr, unsigned int size, comparison_fn_t comparator);
void heap_set_data(heap *h, void **arr, unsigned int count, unsigned int size);
void heap_destroy(heap *h);

#endif