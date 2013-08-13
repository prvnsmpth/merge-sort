#include <stdio.h>
#include <stdlib.h>
#include "heap.h"

void heap_init(heap *h, comparison_fn_t comparator)
{
	h->size = DEFAULT_SIZE;
	h->count = 0;
	h->comparator = comparator;
	h->data = malloc(sizeof(void *) * h->size);
}

void heap_push(heap *h, void *elem)
{
	// resize the heap if necessary
	if (h->count == h->size)
	{		
		h->size <<= 1;
		h->data = realloc(h->data, sizeof(void *) * h->size);
	}

	// insert elem at right position
	unsigned int i = h->count, parent = (i - 1) >> 1;
	while (i > 0)
	{
		if (h->comparator(elem, h->data[parent]) > 0) break;
		h->data[i] = h->data[parent];
		i = parent;
		parent = (i - 1) >> 1;
	}
	h->data[i] = elem;
	h->count++;
}

void *heap_pop(heap *h)
{
	if (h->count == 0)
		return NULL;
	void *front = h->data[0];
	void *last = h->data[h->count - 1];
	h->data[0] = last;
	unsigned int i = 0;
	unsigned int swap = (i << 1) + 1, other;
	
	// bubble down the root
	while (swap < h->count)
	{
		other = swap + 1;
		if (other < h->count && h->comparator(h->data[swap], h->data[other]) > 0)
			swap = other;
		if (h->comparator(last, h->data[swap]) < 0) break;
		h->data[i] = h->data[swap];
		h->data[swap] = last;
		i = swap;
		swap = (i << 1) + 1;
	}
	h->count--;

	// resize the heap if necessary
	if (h->count <= h->size >> 2 && h->size > DEFAULT_SIZE)
	{
		h->size >>= 1;
		h->data = realloc(h->data, sizeof(void *) * h->size);
	}

	return front;
}

void heapify(void **arr, unsigned int count, comparison_fn_t comparator)
{
	unsigned int i, elem, swap, other;
	elem = (count >> 1) - 1;
	while (1)
	{
		i = elem;
		void *item = arr[i];
		swap = (i << 1) + 1;
		while (swap < count)
		{
			other = swap + 1;
			if (other < count && comparator(arr[swap], arr[other]) > 0)
				swap = other;
			if (comparator(item, arr[swap]) < 0) break;
			arr[i] = arr[swap];
			arr[swap] = item;
			i = swap;
			swap = (i << 1) + 1;
		}
		if (!elem) break;
		elem--;
	}
}

void heap_set_data(heap *h, void **arr, unsigned int count, unsigned int size)
{
	h->data = arr;
	h->count = count;
	h->size = size;
}