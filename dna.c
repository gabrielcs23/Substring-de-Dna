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

int min(int a, int b) {						//
	if (a < b)								//
		return a;							//
	else return b;							// Função  básica de min comparativa
}											

char *bases;
char *str;

int main(int argc, char** argv){

	int const TAG_SIZE = 2;					//
	int const TAG_QUERY = 3;				//
	int const TAG_DNA = 4;					//
	int const TAG_ANSWER = 5;				// Tags para envios de mensagens
	
	int my_rank, np, tag = 0, len;			
	int result, resultMin = INT_MAX;			// Começa com uma variável de resultado no INT_MAX
	int more_query, more_bases; 				// Variável que controla se o master mandará mais carga aos trabalhadores
    MPI_Status status;							
    MPI_Init(&argc, &argv);						
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    double t1, t2, t3;
    int numBase;

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
	
	// ----------------------------- Escopo das ações dos mestres ---------------------------
	if (my_rank == 0) {						

		openfiles();														

		char desc_dna[100], desc_query[100];
		char line[100];
		int i, found;

		fgets(desc_query, 100, fquery);
		remove_eol(desc_query);
		while (!feof(fquery)) {
			numBase = 0;

			t1 = MPI_Wtime(); // Inicia o cronômetro
			t3 = t1; // Armazena uma cópia para calcular o tempo de cada query


			more_query = 1;												// Envia uma flag sinalizando para os processos
			MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);		// se vão receber mais substrings ou não
			
			fprintf(fout, "%s\n", desc_query);							// Lê e salva a descrição da query atualno arquivo de saída
			fgets(line, 100, fquery);									// Começa a lê a query
			remove_eol(line);											
			str[0] = 0;													
			i = 0;														
			do {														//
				strcat(str + i, line);									//
				if (fgets(line, 100, fquery) == NULL)					//
					break;												//
				remove_eol(line);										//
				i += 80;												//
			} while (line[0] != '>');									// Armazena o query em str e salva a 
			strcpy(desc_query, line);									// descrição do próximo query

			// Ponto para chamar MPI_SEND de todos
			len = strlen(str) +1;											// len + 1 para inclusão do '\0' do final da query
																			// 
			MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);					// Envia o tamanho da query a ser recebida
			MPI_Bcast(str, len, MPI_CHAR, 0, MPI_COMM_WORLD);				// por todos os processos e depois as próprias queries																	//
			
			

			found = 0;															//
			fseek(fdatabase, 0, SEEK_SET);										//
			fgets(line, 100, fdatabase);										// Leitura da base de dados
			remove_eol(line);
			while (!feof(fdatabase)) {
				numBase++;
				more_bases = 1;													// Avisa para todos os outros processos que 
				MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);			// ainda há bases de DNA para receberem

				strcpy(desc_dna, line);
				bases[0] = 0;
				i = 0;
				fgets(line, 100, fdatabase);
				remove_eol(line);
				do {															//
					strcat(bases + i, line);									//
					if (fgets(line, 100, fdatabase) == NULL)					//
						break;													// Lê do arquivo dna.in a base e armazena
					remove_eol(line);											// a sequência em bases, em seguida guarda a
					i += 80;													// descrição da próxima sequência
				} while (line[0] != '>');										//
				
				int part_size = strlen(bases)/np;								// define o tamanho das parcelas de cada processo
				int resto = strlen(bases)%np;									// e salva o resto para balanceamento de carga
				
				for (i = 1;i < np; i++) {										// Para cada processo, o mestre
					len = part_size + strlen(str) - 1;							// verifica se i é cadidato ao balanceamento,
					if (i < resto) len++;										// se for, ele enviará o tamanho da partição 
					int offset = i < resto ? i : resto;							// ajustada para balanceamento e também a partição
					MPI_Send(&len, 1, MPI_INT, i, TAG_SIZE, MPI_COMM_WORLD);	// da sequencia de DNA referente para aquele processo
					MPI_Send(&bases[(i * part_size) + offset], len, MPI_CHAR,	// O offset garante integridade do deslocamento no 
					 i, TAG_DNA, MPI_COMM_WORLD);								// balanceamento de carga
				}

				result = bmhs(&bases[0], part_size + (resto>0?1:0) + strlen(str) - 1, str, strlen(str));	// Chama a função bhms para a partição do mestre
				
				if (result >= 0)												// Se a função retorna uma posição,
					resultMin = min(result, resultMin);							// verifica se ela é menor que um resultado já achado
				int result_temp;												// anteriormente
				

				for (i = 1; i < np; i++) {																// O mestre então recebe de cada processo
					MPI_Recv(&result_temp, 1, MPI_INT, i, TAG_ANSWER, MPI_COMM_WORLD, &status);			// resultado na posição relativa, e então
					if (result_temp >= 0){																// calcula a posição absoluta
						result_temp += i * part_size;													// 
						if (i < resto) result_temp += i;												// 
						else result_temp += resto;														// Ajusta o valor da posição por conta do balanceamento
						resultMin = min(result_temp, resultMin);										// Compara o resultado atual com o menor já achado
					}
				}

				if (resultMin != INT_MAX) {										// Se o resultado no final != INT_MAX, significa
					fprintf(fout, "%s\n%d\n", desc_dna, resultMin);				// achou a posição, então o mestre
					found++;													// salva a resposta no arquivo de saída
					t2 = MPI_Wtime(); // Para o cronômetro para cada base
					printf("Tempo para achar query %s na base %d foi de: %f\n", str, numBase, (t2 - t1));
					t1 = t2;
				}

				resultMin = INT_MAX;											// Reseta o valor do resultado para a próxima base.
			}

			more_bases = 0;														// Quando acaba as bases, envia um sinal para todos
			MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);				// indicando que não há mais para receber


			
			t2 = MPI_Wtime(); // Para o cronômetro para cada query

			if (!found) {
				fprintf(fout, "NOT FOUND\n");
				printf("Não foi possível achar a query %s em nenhuma base. ", str);
			}

			printf("Tempo gasto com a query %s foi de: %f\n", str, (t2 - t3)*1000);

		}
		more_query = 0;															// Quado acabam as queries, avisa para todos
		MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);					// que não há mais queries para receberem

		closefiles();

	}


// ----------------------------- Escopo dos outros processos -----------------------------------
	else {
		
		MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);							// Verifica se o mestre enviará mais query
			
			while(more_query){															// Enquanto houverem queries

				MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);							// Recebe o tamanho da query atual
				MPI_Bcast(str, len, MPI_CHAR, 0, MPI_COMM_WORLD);						// e a query em si

				
				MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);					// Verifica se o mestre enviará mais bases
										
				while(more_bases){														// Enquanto houverem bases

					MPI_Recv(&len, 1, MPI_INT, 0, TAG_SIZE, MPI_COMM_WORLD, &status);	// Recebe o tamanho da base  
					MPI_Recv(bases, len, MPI_CHAR, 0, TAG_DNA, MPI_COMM_WORLD, &status);// e a base em si
					bases[len] = '\0';		
					result = bmhs(bases, strlen(bases), str, strlen(str));				// Calcula a posição relativa da substring para esse processo
				
					MPI_Send(&result, 1, MPI_INT, 0, TAG_ANSWER, MPI_COMM_WORLD);		// Envia o resultado de volta para o mestre
					MPI_Bcast(&more_bases, 1, MPI_INT, 0, MPI_COMM_WORLD);				// Verifica novamente se há mais bases para receber
				}

				MPI_Bcast(&more_query, 1, MPI_INT, 0, MPI_COMM_WORLD);					// Verifica novamente se há mais queries para receber
			}
	}

	free(str);				//
	free(bases);			// Libera memória

	MPI_Finalize();			// Finaliza MPI
	return EXIT_SUCCESS;
}
