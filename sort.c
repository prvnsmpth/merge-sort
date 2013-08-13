#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "heap.h"

#define DISK_BLOCK_SIZE 4096

typedef struct {
    int type;
    int size;
} attribute;

typedef struct {
    unsigned int numattrs;
    unsigned int record_size;
    unsigned int header_size;
    attribute *attrlist;
    unsigned int num_comp_attrs;
    int *comparable_attrs;
} metadata;

metadata _meta;

int compare_records(const void *record1, const void *record2)
{   
    int i;
    for (i = 0; i < _meta.num_comp_attrs; i++)
    {
    	int attr_pos = _meta.comparable_attrs[i];
    	short int asc = (attr_pos > 0 ? 1 : 0);
    	if (attr_pos < 0) attr_pos = -attr_pos;
    	attr_pos--;
    	attribute a = _meta.attrlist[attr_pos];
    	int cmp;
    	int offset = get_attr_offset(attr_pos);
    	switch (a.type)
    	{
    		case 1: {
    			int val1 = *(int *)(record1 + offset);
    			int val2 = *(int *)(record2 + offset);
    			cmp = (val1 > val2) - (val1 < val2); 
    			break; 
    		}  			
    		case 2: {
    			double val1 = *(double *)(record1 + offset);
    			double val2 = *(double *)(record2 + offset);
    			cmp = (val1 > val2) - (val1 < val2);
    			break;
    		}
    		case 3: {
    			char *val1 = (char *)record1 + offset;
    			char *val2 = (char *)record2 + offset;
    			cmp = strcmp(val1, val2);
    			break;
    		}
    	}
    	if (cmp == 0)
			continue;
		return (asc ? cmp : -cmp);
    }

    return 0;  
}

int get_attr_offset(int attr_pos)
{
	int j = 0, offset = 0;
	for (; j < attr_pos; j++)
	{
		offset += _meta.attrlist[j].size;
	}
	return offset;
}

void write_metadata(FILE *file, metadata m)
{
    fwrite(&m.numattrs, sizeof(unsigned int), 1, file);
    int j;
    for (j = 0; j < m.numattrs; j++)
    {
        fwrite(&m.attrlist[j].type, sizeof(int), 1, file);
        fwrite(&m.attrlist[j].size, sizeof(int), 1, file);
    }
}

int get_run_number(const void *p, char **base_pointers, unsigned int num_runs)
{
    int i;
    for (i = 0; i < num_runs && p > (void *)base_pointers[i]; i++);
    return (p == (void *) base_pointers[i] ? i + 1 : i);
}

void sort(char *inputfile, char *outputfile, int numattrs, int attributes[], int bufsize)
{
    // open stream to read data file
    printf("Buffer size: %d bytes\n", bufsize);
    printf("Block size: %d bytes\n", DISK_BLOCK_SIZE);
    printf("Reading from: %s\n", inputfile);
    FILE *in = fopen(inputfile, "r");
    
    // read metadata
    metadata m;
    m.record_size = m.header_size = 0;
    fread(&m.numattrs, sizeof(unsigned int), 1, in);
    m.header_size += sizeof(unsigned int) + 2 * m.numattrs * sizeof(int);    
    m.attrlist = (attribute *) malloc(sizeof(attribute) * m.numattrs);
    printf("Header size: %d bytes\n", m.header_size);
    int i, j;
    for (i = 0; i < m.numattrs; i++)
    {
        fread(&m.attrlist[i].type, sizeof(int), 1, in);
        fread(&m.attrlist[i].size, sizeof(int), 1, in);
        m.record_size += m.attrlist[i].size;
    }
    printf("Record size: %d bytes\n", m.record_size);
    _meta = m;
    _meta.num_comp_attrs = numattrs;
    _meta.comparable_attrs = attributes;

    // max no. of disk blocks that fit in the buffer
    int num_records_in_buffer = bufsize / m.record_size; 
    int num_records_in_block = DISK_BLOCK_SIZE / m.record_size;
    int num_blocks_in_buffer = bufsize / DISK_BLOCK_SIZE;
    printf("Records in block: %d\n", num_records_in_block);
    printf("Blocks in buffer: %d \n", num_records_in_buffer);
    
    // the buffer
    char *buffer = (char *) malloc(bufsize);
    
    // create the initial set of runs
    size_t blocks_read;
    i = 0;
    char run_name[10];
    FILE *out;
    unsigned int num_recs_read = 0;
    unsigned int num_recs_wrote = 0;
    while (1)
    {
        blocks_read = fread(buffer, num_records_in_block * m.record_size, 
        		num_blocks_in_buffer, in);
        if (blocks_read == 0)
        	break;
        num_recs_read += blocks_read * num_records_in_block;

        // sort the chunk
        qsort(buffer, blocks_read * num_records_in_block, m.record_size, compare_records);

        // write sorted chunk to a run file
        i++;
        sprintf(run_name, "run_%d.bin", i);
        out = fopen(run_name, "w");
        write_metadata(out, m);
        size_t wrote = fwrite(buffer, num_records_in_block * m.record_size, 
        	blocks_read, out);
        num_recs_wrote += wrote * num_records_in_block;
        fclose(out);
    }
    printf("Read %d records from input file. Wrote %d records\n", num_recs_read, num_recs_wrote);

    // number of generated runs
    int num_runs = i;

    /********************************* MERGE ********************************/

    // max no. of runs we can merge at once
    int max_merge_runs = (bufsize / DISK_BLOCK_SIZE) - 1;

    // decide how many runs to merge at once
    int merge_at_once = (num_runs < max_merge_runs ? num_runs : max_merge_runs);

    // how much to read from one run file
    int blocks_per_run = (bufsize / (merge_at_once + 1)) / DISK_BLOCK_SIZE;
    int bytes_per_run = blocks_per_run * num_records_in_block * m.record_size;

    // print out the parameters
    printf("%d runs generated. %d runs can be merged at once.\n", num_runs, max_merge_runs);
    printf("%d blocks (%d bytes) to be read from each run file.\n", blocks_per_run, bytes_per_run);

    // read from runs into buffer
    FILE **runs = (FILE **) malloc(merge_at_once * sizeof(FILE *));
    char **run_pointers = (char **) malloc((merge_at_once + 1) * sizeof(char *));
    char **run_bp = (char **) malloc((merge_at_once + 1) * sizeof(char *));
    char **run_ep = (char **) malloc((merge_at_once + 1) * sizeof(char *));
    char **heap_data = (char **) malloc(merge_at_once * sizeof(char *));
    for (i = 0; i < merge_at_once; i++)
    {    	
    	sprintf(run_name, "run_%d.bin", i + 1);
    	runs[i] = fopen(run_name, "r");
        fseek(runs[i], m.header_size, SEEK_SET);
        char *buffer_start_addr = buffer + i * bytes_per_run;
    	fread(buffer_start_addr, num_records_in_block * m.record_size, blocks_per_run, runs[i]);        
        run_bp[i] = heap_data[i] = buffer_start_addr;
        run_pointers[i] = run_bp[i] + m.record_size;
        run_ep[i] = run_bp[i] + bytes_per_run;
        printf("Block %d: %p to %p\n", i + 1, run_bp[i], run_ep[i]);
    }

    // the output buffer
    run_bp[i] = run_pointers[i] = buffer + i * bytes_per_run;

    // create the heap
    heap *HEAP = (heap *) malloc(sizeof(heap));
    heap_init(HEAP, compare_records);
    heapify((void **)heap_data, merge_at_once, compare_records);
    heap_set_data(HEAP, (void **)heap_data, merge_at_once, merge_at_once);
    
    // open a temporary run file for storing merged contents of all the generated runs
    FILE *temp_out = fopen("run_temp.bin", "w");
    write_metadata(temp_out, m);
    fclose(temp_out); 

    num_recs_read = 0;
    int push = 0;
    int read_from_runs = 0;
    while (1)
    {
        // get min record from heap, and compute the index of the parent run
        void *min_rec = heap_pop(HEAP); 
        if (!min_rec)
        {
            // write out whatever is left in the output buffer and exit
            if (run_pointers[merge_at_once] > run_bp[merge_at_once])
            {
                FILE *output_run = fopen("run_temp.bin", "a");
                size_t check = fwrite(run_bp[merge_at_once], 
                    1, run_pointers[merge_at_once] - run_bp[merge_at_once], output_run);
                fclose(output_run);
                printf("Emptying output buffer, writing %d bytes\n", check);
            }
            break;
        }
        int min_rec_run = get_run_number(min_rec, run_bp, merge_at_once);

        // copy the min record popped from the heap into the output run        
        memcpy(run_pointers[merge_at_once], min_rec, m.record_size);        
        run_pointers[merge_at_once] += m.record_size;                 

        // fetch a new record from the run and push into heap,
        // adjusting the run pointer
        if (run_pointers[min_rec_run - 1] < run_ep[min_rec_run - 1])
        {
            heap_push(HEAP, run_pointers[min_rec_run - 1]);            
            push++;
            run_pointers[min_rec_run - 1] += m.record_size;
        }
        else if (run_pointers[min_rec_run - 1] >= run_ep[min_rec_run - 1])
        {                    
            size_t check = fread(run_bp[min_rec_run - 1],
                num_records_in_block * m.record_size, blocks_per_run, runs[min_rec_run - 1]);
            read_from_runs += check * num_records_in_block;

            // reset the run pointer
            if (check > 0)
            {                 
                run_pointers[min_rec_run - 1] = run_bp[min_rec_run - 1];
                run_ep[min_rec_run - 1] = run_bp[min_rec_run - 1] + check * num_records_in_block * m.record_size;               

                // push first record from fresh lot into heap
                heap_push(HEAP, run_pointers[min_rec_run - 1]);                
                push++;
                run_pointers[min_rec_run - 1] += m.record_size;
            }
        }      

        // check for overflow of the output run,
        // write to output run if an overflow is detected
        if (run_pointers[merge_at_once] >= run_bp[merge_at_once] + bytes_per_run)
        {
            FILE *output_run = fopen("run_temp.bin", "a");
            size_t check = fwrite(run_bp[merge_at_once], 
                num_records_in_block * m.record_size, blocks_per_run, output_run);
            fclose(output_run);
            num_recs_read += check * num_records_in_block;
            
            // reset the run pointer
            run_pointers[merge_at_once] = run_bp[merge_at_once];
        }
    }
    printf("Wrote %d records to temp run file. %d pushed. %d read from runs\n", num_recs_read, push, read_from_runs);

    // clean up  
    free(HEAP);  
    free(HEAP->data);
    free(run_ep);
    free(run_bp);
    free(run_pointers);
    for (i = 0; i < merge_at_once; i++)
        fclose(runs[i]);
    free(runs);

    /************************* END MERGE *************************************/

    // clean up
    free(m.attrlist);
    free(buffer);

    // close the input data file
    fclose(in);
}

int compare_int(const void *a, const void *b)
{
    int c = *(int *) a;
    int d = *(int *) b;
    return (c > d) - (c < d);
}

int main(int argc, char **argv)
{
	int attributes[1] = {1};
    sort(argv[1], argv[0], 1, attributes, 1000000);

    // test heap
    // int a[5] = {1, 2, 3, 4, 0};
    // heap *h = (heap *) malloc(sizeof(heap));
    // heap_init(h, compare_int);
    // int i = 0;
    // for (; i < 5; i++)
    //     heap_push(h, &a[i]);
    // int got = *(int *) heap_pop(h);
    // printf("Min: %d\n", got);
    // heap_push(h, &a[2]);
    // printf("Min: %d\n", *(int *) heap_pop(h));
    // heap_push(h, &a[4]);
    // printf("Min: %d\n", *(int *) heap_pop(h));

    return 0;
}
