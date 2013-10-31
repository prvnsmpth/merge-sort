#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "heap.h"

#define DISK_BLOCK_SIZE 4096
#define DEFAULT_BUFFER 100000

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
    printf("Blocks in buffer: %d \n", num_blocks_in_buffer);
    
    // the buffer
    char *buffer = (char *) malloc(bufsize);
    
    // create the initial set of runs
    size_t records_read, records_written;
    i = 0;
    char run_name[20];
    FILE *out;
    unsigned int num_recs_read = 0;
    unsigned int num_recs_written = 0;
    while (1)
    {
        records_read = fread(buffer, m.record_size, 
        		num_records_in_buffer, in);
        if (records_read == 0)
        	break;
        num_recs_read += records_read;

        // sort the chunk
        qsort(buffer, records_read, m.record_size, compare_records);

        // write sorted chunk to a run file
        i++;
        sprintf(run_name, "run_%d.bin", i);
        out = fopen(run_name, "w");
        write_metadata(out, m);
        records_written = fwrite(buffer, m.record_size, 
        	records_read, out);
        num_recs_written += records_written;
        fclose(out);
    }
    printf("Read %d records from input file.\n", num_recs_read);

    // number of generated runs
    int init_num_runs = i;
    printf("Number of initial runs generated: %d\nMerging...\n", i);

    /********************************* MERGE ********************************/

    // number of runs in a pass
    int num_runs = init_num_runs;

    // max no. of runs we can merge at once
    int max_merge_runs = (bufsize / DISK_BLOCK_SIZE) - 1;

    int start = 1;
    int interm_runp = 1;
    char temp_run_name[20];
    int runs_left_this_pass = num_runs;

    // run multiple passes over the runs to merge into one file
    while (1)
    {
        // check if we have to move to the next pass
        if (runs_left_this_pass == 0)
        {
            start = 1;
            interm_runp = 1;
            int merged_at_once = (num_runs < max_merge_runs ? num_runs : max_merge_runs);
            if (num_runs == merged_at_once)
            {
                // the final merged output of all runs is in run_1.bin
                // rename it to 'outputfile'
                rename("run_1.bin", outputfile);
                printf("Done.\nOutput written to: %s\n", outputfile);

                break;
            }
            num_runs = (num_runs / merged_at_once) + 1 * (num_runs % merged_at_once != 0);            
            runs_left_this_pass = num_runs; 
        }        

        // decide how many runs to merge at once
        int merge_at_once = (runs_left_this_pass < max_merge_runs ? runs_left_this_pass : max_merge_runs);       
        sprintf(temp_run_name, "run_temp.bin");  

        // how much to read from one run file
        int blocks_per_run = (bufsize / (merge_at_once + 1)) / DISK_BLOCK_SIZE;
        int records_per_run = blocks_per_run * num_records_in_block;
        int bytes_per_run = blocks_per_run * num_records_in_block * m.record_size;

        // read from runs into buffer
        FILE **runs = (FILE **) malloc(merge_at_once * sizeof(FILE *));
        char **run_pointers = (char **) malloc((merge_at_once + 1) * sizeof(char *));
        char **run_bp = (char **) malloc((merge_at_once + 1) * sizeof(char *));
        char **run_ep = (char **) malloc((merge_at_once + 1) * sizeof(char *));
        heap *HEAP = (heap *) malloc(sizeof(heap));
        heap_init(HEAP, compare_records);
        for (i = 0; i < merge_at_once; i++)
        {    	
        	sprintf(run_name, "run_%d.bin", i + start);
        	runs[i] = fopen(run_name, "r");
            fseek(runs[i], m.header_size, SEEK_SET);
            char *buffer_start_addr = buffer + i * bytes_per_run;
        	fread(buffer_start_addr, num_records_in_block * m.record_size, blocks_per_run, runs[i]);        
            run_bp[i] = buffer_start_addr;
            heap_push(HEAP, run_bp[i]);
            run_pointers[i] = run_bp[i] + m.record_size;
            run_ep[i] = run_bp[i] + bytes_per_run;
        }

        // the output buffer
        run_bp[i] = run_pointers[i] = buffer + i * bytes_per_run;
        
        // open a temporary run file for storing merged contents of all the generated runs
        FILE *temp_out = fopen(temp_run_name, "w");
        write_metadata(temp_out, m);
        fclose(temp_out); 

        // read chunks of data from each run file into a heap
        // and write to a temp run file until all the run files become empty
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
                    FILE *output_run = fopen(temp_run_name, "a");
                    size_t check = fwrite(run_bp[merge_at_once], 
                        1, run_pointers[merge_at_once] - run_bp[merge_at_once], output_run);
                    fclose(output_run);
                    // printf("Emptying output buffer, writing %d bytes\n", check);
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
                    m.record_size, records_per_run, runs[min_rec_run - 1]);
                read_from_runs += check;

                // reset the run pointer
                if (check > 0)
                {                 
                    run_pointers[min_rec_run - 1] = run_bp[min_rec_run - 1];
                    run_ep[min_rec_run - 1] = run_bp[min_rec_run - 1] + check * m.record_size;               

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
                FILE *output_run = fopen(temp_run_name, "a");
                size_t check = fwrite(run_bp[merge_at_once], 
                    m.record_size, records_per_run, output_run);
                fclose(output_run);
                num_recs_read += check;
                
                // reset the run pointer
                run_pointers[merge_at_once] = run_bp[merge_at_once];
            }
        }

        // clean up
        heap_destroy(HEAP);
        free(HEAP);       
        free(run_ep);
        free(run_bp);
        free(run_pointers);
        for (i = 0; i < merge_at_once; i++)
            fclose(runs[i]);
        free(runs);

        // delete run files used to build this run file
        for (i = 0; i < merge_at_once; i++)
        {
            sprintf(run_name, "run_%d.bin", i + start);
            remove(run_name);
        }

        // compute number of runs left in this pass        
        runs_left_this_pass -= merge_at_once;   
        start += merge_at_once;

        // rename temp file to run_<index>.bin
        sprintf(run_name, "run_%d.bin", interm_runp);
        interm_runp++;        
        rename(temp_run_name, run_name);
        // printf("Merged %d - %d into %s\n", start - merge_at_once, start - 1, run_name);

    }

    /************************* END MERGE *************************************/

    // clean up
    free(m.attrlist);
    free(buffer);

    // close the input data file
    fclose(in);
}

void print_usage()
{
    printf("Usage: ./sort [-o output_file] [-b buffer_size] \n\
            [-a attr_list] input_file\n");
}

int main(int argc, char **argv)
{
    int num_comp_attrs = 0;
	int *attributes;
    int buffer_size = 0;
    char *outputfile = NULL, *token;
    char *delim = ",";
    int c, len, i;
    while ((c = getopt(argc, argv, "b:o:a:h")) != -1)
    {
        switch (c)
        {
            case 'b' :
                sscanf(optarg, "%d", &buffer_size);
                break;
            case 'o' :
                outputfile = optarg;
                break;
            case 'a' :
                len = strlen(optarg);
                for (i = 0; i < len; i++)
                    if (optarg[i] == ',')
                        num_comp_attrs++;
                num_comp_attrs++;
                attributes = (int *) malloc(sizeof(int) * num_comp_attrs);
                token = strtok(optarg, delim);
                sscanf(token, "%d", &attributes[0]);            
                for (i = 1; token = strtok(NULL, delim); i++)
                {
                    sscanf(token, "%d", &attributes[i]);
                }
                break;
            case 'h':
                // print usage and exit
                print_usage();
                exit(0);
            case '?':
                break;
            default:
                abort();             
        }        
    }

    if (optind >= argc)
    {
        print_usage();
        exit (EXIT_FAILURE);
    }

    if (buffer_size == 0)
        buffer_size = DEFAULT_BUFFER;
    if (num_comp_attrs == 0)
    {
        num_comp_attrs = 1;
        attributes = (int *) malloc(sizeof(int));
        attributes[0] = 1;
    }
    if (outputfile == NULL)
    {
        outputfile = "output.bin";
    }

    sort(argv[optind], outputfile, num_comp_attrs, attributes, buffer_size);

    free(attributes);
    return 0;
}
