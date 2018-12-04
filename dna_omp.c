#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <limits.h>

// MAX char table (ASCII)
#define MAX 256

// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;

	// pre-processing
	for (j = 0; j < MAX; j++)
		d[j] = m + 1;
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;

	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)
			return k + 1;
		i = i + d[(int) string[i + 1]];
	}

	return -1;
}

FILE *fdatabase, *fquery, *fout;

void openfiles() {

	fdatabase = fopen("dna.in", "r");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

void remove_eol(char *line) {
	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

char *bases;
char *str;

int main(int argv, char* args) {
	int numThreads = (int)args[1];
	omp_set_num_threads(numThreads);
	bases = (char*) malloc(sizeof(char) * 1000001);
	if (bases == NULL) {
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	str = (char*) malloc(sizeof(char) * 1000001);
	if (str == NULL) {
		perror("malloc str");
		exit(EXIT_FAILURE);
	}

	openfiles();

	char desc_dna[100], desc_query[100];
	char line[100];
	int i, result=INT_MAX, found;

	fgets(desc_query, 100, fquery);
	remove_eol(desc_query);
	while (!feof(fquery)) {
		fprintf(fout, "%s\n", desc_query);
		// read query string
		fgets(line, 100, fquery);
		remove_eol(line);
		str[0] = 0;
		i = 0;
		do {
			strcat(str + i, line);
			if (fgets(line, 100, fquery) == NULL)
				break;
			remove_eol(line);
			i += 80;
		} while (line[0] != '>');
		strcpy(desc_query, line);

		// read database and search
		found = 0;
		fseek(fdatabase, 0, SEEK_SET);
		fgets(line, 100, fdatabase);
		remove_eol(line);
		while (!feof(fdatabase)) {
			strcpy(desc_dna, line);
			bases[0] = 0;
			i = 0;
			fgets(line, 100, fdatabase);
			remove_eol(line);
			do {
				strcat(bases + i, line);
				if (fgets(line, 100, fdatabase) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');

			int part_size = strlen(bases)/numThreads; // poderiamos usar omp_get_num_threads na divisão porém isso já temos esse valor como parâmetro
			int resto = strlen(bases)%numThreads;

			#pragma omp parallel reduction(+:found)
			{
				int len, id, offset, result_temp;

				id = omp_get_thread_num();
				len = part_size + strlen(str) - 1;
				if (id < resto) len++;
				offset = id < resto ? id : resto;		

				// char* base_local = (char*) malloc(sizeof(char) * 1000001);
				// strcpy(base_local, &bases[(i * part_size) + offset]);
				// strcat(base_local, '\0');

				/*int j;for(j=(id * part_size) + offset; j<len;j++){printf("%c", bases[j]);}printf("\n");*/
				result_temp = bmhs(&bases[(id * part_size) + offset], len, str, strlen(str));

				
				if (result_temp > -1) {
					result_temp += id * part_size;
					if (id < resto) result_temp += id;
					else result_temp += resto;

					result = result_temp;
					printf("%d - RESULT_TEMP = %d\n", id, result_temp);
					#pragma omp critical
					fprintf(fout, "%s\n%d\n", desc_dna, result);
					found++;
				}
				
			
			}

			//printf("RESULT = %d\n", result);
			/* if (result != INT_MAX) {
				fprintf(fout, "%s\n%d\n", desc_dna, result);
				found++;
			}*/

		}

		if (!found)
			fprintf(fout, "NOT FOUND\n");
	}

	closefiles();

	free(str);
	free(bases);

	return EXIT_SUCCESS;
}
