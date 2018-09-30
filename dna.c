#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
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

	/*for (i = n-m; i < n; i++){
		int match = 0;
		j = i;
		k = 0;
		while (j < n && k < m && string[j] == substr[k]) {
			match++;
			j++;
			k++;
		}
		if (j == n && k == m) return (match * -1);
	}*/

	return -1;
}

int search(char *string, int n, char *substr, int m) {

	int answer = bmhs(string, n, substr, m);

	if (answer > -1) return answer;

	int i;
	/*for (i = 1; i < n; i++) {
		answer = bmhs(string[m-n+i], n-i, substr[i], n-i);
	}*/
	for (i = n-1; i > 0; i--) {
		answer = bmhs(string + m-i, i, substr + n-i, i);
		if (answer > -1) return answer * -1;
	}

	return INT_MIN;
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

int min(int a, int b) {
	if (a < b)
		return a;
	else return b;
}

char *bases;
char *str;

int main(int argc, char** argv) {
	//int const TAG_CARGA = 1;
	int const TAG_SIZE = 2;
	int const TAG_QUERY = 3;
	int const TAG_DNA = 4;
	int const TAG_ANSWER = 5;
	
	int my_rank, np, tag = 0, len;
	int result, resultMin = INT_MAX;
	int more_query, more_bases; // Variável que controla se o master mandará mais carga aos trabalhadores
    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

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
	
	//char dna_split[part_size];
	if (my_rank == 0) {

		openfiles();

		char desc_dna[100], desc_query[100];
		char line[100];
		int i, found;

		fgets(desc_query, 100, fquery);
		remove_eol(desc_query);
		while (!feof(fquery)) {
			more_query = 1;

			MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);
			
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

			// Ponto para chamar MPI_SEND de todos
			len = strlen(str) +1;
			for(i = 1; i< np; i++ ){
				MPI_Send(&len, 1, MPI_INT, i, TAG_SIZE, MPI_COMM_WORLD);
				//printf("QUERY ATUAL: %s\n", str);
				MPI_Send(str, len, MPI_CHAR, i, TAG_QUERY, MPI_COMM_WORLD);
			}
			// read database and search
			found = 0;
			fseek(fdatabase, 0, SEEK_SET);
			fgets(line, 100, fdatabase);
			remove_eol(line);
			while (!feof(fdatabase)) {
				more_bases = 1;

				MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);

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
				// Ponto para chamar MPI_SEND de todos (TODO Scatter)
				/*len = (strlen(bases) + 1) / np;
				len += strlen(str) - 1;
				for (i = 1;i < np; i++) {
					MPI_Send(&len, 1, MPI_INT, i, TAG_SIZE, MPI_COMM_WORLD);
					MPI_Send(&bases[i * len], len, MPI_CHAR, i, TAG_DNA, MPI_COMM_WORLD);
				}*/
				int part_size = strlen(bases)/np;
				int resto = strlen(bases)%np;
				for (i = 1;i < np; i++) {
					len = part_size + strlen(str) - 1;
					if (i < resto) len++;
					int offset = i < resto ? i : resto;
					MPI_Send(&len, 1, MPI_INT, i, TAG_SIZE, MPI_COMM_WORLD);
					MPI_Send(&bases[(i * part_size) + offset], len, MPI_CHAR, i, TAG_DNA, MPI_COMM_WORLD);
				}
				result = bmhs(&bases[0], part_size + (resto>0?1:0), str, strlen(str));
				if (result >= 0)
					resultMin = min(result, resultMin);
				int result_temp;
				// MPI_Recv de todos
				for (i = 1; i < np; i++) {
					MPI_Recv(&result_temp, 1, MPI_INT, i, TAG_ANSWER, MPI_COMM_WORLD, &status);
					printf("%d - processou de %d ate %d, com resultado = %d\n", i, (i * part_size) + (i < resto ? i : resto), (i * part_size) + (i < resto ? i : resto) + part_size + (i < resto ? 1 : 0), result_temp);
					if (result_temp >= 0){
						result_temp += i * part_size;
						if (i < resto) result_temp += i;
						else result_temp += resto;
						resultMin = min(result_temp, resultMin);
					}
				}
				if (resultMin != INT_MAX) {
					fprintf(fout, "%s\n%d\n", desc_dna, resultMin);
					found++;
				}
				resultMin = INT_MAX;
			}
			more_bases = 0;

			MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);


			if (!found)
				fprintf(fout, "NOT FOUND\n");
		}
		more_query = 0;

		MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);

		closefiles();

	}

	else {
		// sempre verifica se o mestre enviará mais carga
		MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);
		
			while(more_query){
				MPI_Recv(&len, 1, MPI_INT, 0, TAG_SIZE, MPI_COMM_WORLD, &status);
				MPI_Recv(str, len, MPI_CHAR, 0, TAG_QUERY, MPI_COMM_WORLD, &status);
				//printf("%d recebeu o query %s \n", my_rank, str);
				//printf("QUERY RECEBIDO: %s\n", str);
				// sempre verifica se o mestre enviará mais carga
				
				MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);
				
				//printf("%d sabe que tem mais carga  de dna\n", my_rank);
					while(more_bases){
						//if (my_rank == 1){
						MPI_Recv(&len, 1, MPI_INT, 0, TAG_SIZE, MPI_COMM_WORLD, &status);
						MPI_Recv(bases, len, MPI_CHAR, 0, TAG_DNA, MPI_COMM_WORLD, &status);
						bases[len] = '\0';
						//printf("%d - Recebeu base de tamanho %d: %s\n", my_rank, len, bases);
						result = bmhs(bases, strlen(bases), str, strlen(str));
					
						MPI_Send(&result, 1, MPI_INT, 0, TAG_ANSWER, MPI_COMM_WORLD);
						//}
						MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);
					}

				MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);
			}
	}

	free(str);
	free(bases);

	MPI_Finalize();
	return EXIT_SUCCESS;
}
