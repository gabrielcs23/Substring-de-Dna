#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

 // MAX char table (ASCII)
int const MAX = 256;
int const TAG_FALSE = 0;	
int const TAG_TRUE = 1;
int const TAG_ENVIO_SEQ = 2; 	// Tag para sinalizar envio de sequência de DNA
int const TAG_ENVIO_SIZE_SEQ = 3;
int const TAG_ENVIO_QUERY = 4;	// Tag para sinalizar envio de subsequência de DNA (query)
int const TAG_ENVIO_SIZE_QUERY = 5;	// Tag para sinalizar envio de tamanho de string

// TALVEZ NÃO USAR AS TAGS ABAIXO
int const TAG_ENVIO_RESULT = 6;	// Tag para sinalizar o envio do resultado de um processo para o processo mestre ## TALVEZ NAO ENVIAR RESULTADO
int const TAG_RECEBER_SEQS = 7;	// Tag para sinalizar que ainda há subsequências para receber 
int const TAG_ENVIO_EXTRA = 8;	// Tag para sinalizar que está enviando uma subsequência de buffer para o caso de um query quebrado em dois processos



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
	int part_size, resto; // part_size <= 80  ... Tamanho da sequencia particionada a ser enviada para os processos, resto para o caso de part_size > 80
	int my_rank, np, tag = 0;
	int len_query, len_bases; // Verificar o tamanho do query e da sequencia total a ser enviado para processos
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
		int bufferino;
		fgets(desc_query, 100, fquery);
		remove_eol(desc_query);
		while (!feof(fquery)) {

			fprintf(fout, "%s\n", desc_query);
			// read query string
			fgets(line, 100, fquery);
			remove_eol(line);
			str[0] = 0;
			i = 0;
			do {											//
				strcat(str + i, line);						//
				if (fgets(line, 100, fquery) == NULL)		// 
					break;									//
				remove_eol(line);							//
				i += 80;									// 
			} while (line[0] != '>');						// Armazena em str a query atual
			strcpy(desc_query, line);				

			
			len_query = strlen(str); 

			// Ponto para chamar MPI_SEND de todos
			for( i = 1; i < np; i++){		
				bufferino = TAG_TRUE;													//
				MPI_Send(&bufferino, 1, MPI_INT, np, TAG_RECEBER_SEQS, MPI_COMM_WORLD);			// Indica que ainda há querys para enviar
				MPI_Send(&len_query, 1, MPI_INT, np, TAG_ENVIO_SIZE_QUERY, MPI_COMM_WORLD);		// Para cada processo
				MPI_Send(str, strlen(str), MPI_CHAR, 1, TAG_ENVIO_QUERY, MPI_COMM_WORLD);		// envia a query e seu tamanho
			}
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
				// Ponto para chamar MPI_SEND de todos
				len_bases = strlen(bases);				

				part_size = len_bases/np; 				// 
				if (part_size >80){						// 
					resto = (part_size - 80) * np;		// Calcula o tamanho da particao
					part_size = 80;						// Se maior que o máximo permitido
				}										// Limita pra 80 e poe o restante em resto


				MPI_Send(&len_bases, 1, MPI_INT, 1, TAG_ENVIO_SIZE_SEQ, MPI_COMM_WORLD);
				MPI_Send(bases, len_bases, MPI_CHAR, 1, TAG_ENVIO_SEQ, MPI_COMM_WORLD);
				// MPI_Recv de todos
				MPI_Recv(&result, 1, MPI_INT, 1, TAG_ENVIO_RESULT, MPI_COMM_WORLD, &status);
				if (result > 0) {
					fprintf(fout, "%s\n%d\n", desc_dna, result);
					found++;
				}
				break;
			}

			if (!found)
				fprintf(fout, "NOT FOUND\n");
			break;
		}

		closefiles();

	}

	else {
		// do some worker stuff
		int continuar_recebendo;
		MPI_Recv(&continuar_recebendo, 1, MPI_INT, 0, TAG_RECEBER_SEQS, MPI_COMM_WORLD, &status);
		
		if(continuar_recebendo){
			MPI_Recv(&len_query, 1, MPI_INT, 0, TAG_ENVIO_SIZE_QUERY, MPI_COMM_WORLD, &status);
			MPI_Recv(str, len_query, MPI_CHAR, 0, TAG_ENVIO_QUERY, MPI_COMM_WORLD, &status);
			MPI_Recv(&len_bases, 1, MPI_INT, 0, TAG_ENVIO_SIZE_SEQ, MPI_COMM_WORLD, &status);
			MPI_Recv(bases, len_bases, MPI_CHAR, 0, TAG_ENVIO_SIZE_SEQ, MPI_COMM_WORLD, &status);
			//int result = bmhs(bases, strlen(bases), str, strlen(str));
			int result = 17;
			
			MPI_Send(&result, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
		}
	}

	free(str);
	free(bases);

	MPI_Finalize();
	return EXIT_SUCCESS;
}
