#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

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

int main(int argc, char** argv) {

	int my_rank, np, tag = 0, len;
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

	if (my_rank == 0) {

		openfiles();

		char desc_dna[100], desc_query[100];
		char line[100];
		int i, found, result;

		fgets(desc_query, 100, fquery);
		remove_eol(desc_query);
		while (!feof(fquery)) {
			more_query = 1;
			MPI_Send(&more_query, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
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
			len = strlen(str);
			MPI_Send(&len, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
			MPI_Send(str, strlen(str), MPI_CHAR, 1, 0, MPI_COMM_WORLD);

			// read database and search
			found = 0;
			fseek(fdatabase, 0, SEEK_SET);
			fgets(line, 100, fdatabase);
			remove_eol(line);
			while (!feof(fdatabase)) {
				more_bases = 1;
				MPI_Send(&more_bases, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
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
				// Ponto para chamar MPI_SEND de todos
				len = strlen(bases);
				MPI_Send(&len, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
				MPI_Send(bases, strlen(bases), MPI_CHAR, 1, 0, MPI_COMM_WORLD);
				// MPI_Recv de todos
				MPI_Recv(&result, 1, MPI_INT, 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				if (result > 0) {
					fprintf(fout, "%s\n%d\n", desc_dna, result);
					found++;
				}
			}
			more_bases = 0;
			MPI_Send(&more_bases, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);

			if (!found)
				fprintf(fout, "NOT FOUND\n");
		}
		more_query = 0;
		MPI_Send(&more_query, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);

		closefiles();

	}

	else {
		// sempre verifica se o mestre enviará mais carga
		MPI_Recv(&more_query, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		while(more_query){
			MPI_Recv(&len, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			MPI_Recv(str, len, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			// sempre verifica se o mestre enviará mais carga
			MPI_Recv(&more_bases, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			while(more_bases){
				MPI_Recv(&len, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				MPI_Recv(bases, len, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
				int result = bmhs(bases, strlen(bases), str, strlen(str));
				MPI_Send(&result, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
				MPI_Recv(&more_bases, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			}
			MPI_Recv(&more_query, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		}
	}

	free(str);
	free(bases);

	MPI_Finalize();
	return EXIT_SUCCESS;
}
