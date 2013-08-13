#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv){
	int num1;
	int count;
	int attributes[25][2];
	double num2;
	char data2[50];
	FILE *ptr_myfile;
    char *inputfile = argv[1];
    int numrecords;
    if (argc > 2)
        sscanf(argv[2], "%d", &numrecords);
    else
        numrecords = 120000;
	//Location of bin file
	ptr_myfile=fopen(inputfile,"rb");
	if (!ptr_myfile)
	{
		printf("Unable to open file!");
		return 1;
	}
	int memloc=0;
	unsigned int noofattr;
	int type,length;
	fread(&noofattr,sizeof(unsigned int),1,ptr_myfile);
	// printf("No of attributes: %d",noofattr);
	// printf("\n\n----------Metadata---------------\n\n");
	for(count=0;count<noofattr;count++){
		fread(&type,sizeof(int),1,ptr_myfile);
		fread(&length,sizeof(int),1,ptr_myfile);
		attributes[count][0]=type;
		attributes[count][1]=length;
		// printf("Attribute %d\ntype: %d length: %d\n\n",count+1,type,length);
	}
	
	int returnval=1;
	while(returnval && numrecords){
	    numrecords--;	
		for(count=0;count<noofattr;count++){
			switch (attributes[count][0]){
			case 1: returnval=fread(&num1,attributes[count][1],1,ptr_myfile);
                    if (returnval == 0) break;
					printf("%d ",num1);
					break;
			case 2: returnval=fread(&num2,attributes[count][1],1,ptr_myfile);
                    if (returnval == 0) break;
					printf("%f ",num2);
					break;
			case 3: returnval=fread(data2,attributes[count][1],1,ptr_myfile);
                    if (returnval == 0) break;
					printf("%s ",data2);
					break;
			}
			
		}
		printf("\n");
	}
	
	
}
