#pragma comment(lib, "mpi.lib")
#define _USE_MATH_DEFINES
#include<stdio.h>
#include<mpi.h>
#include<stdlib.h>
#include<math.h>

const double k = 1;
const double h = 5e-4;
const double dt = 2e-9;
const double T = 0.1;
//const int processorsCount = 4;

double EquationStep(double solutionPrev, double solutionCurr, double solutionNext)
{
 	//printf("solutionPrev = %f, solutionCurr = %f, solutionNext = %f \n", solutionPrev, solutionCurr, solutionNext);
	return solutionCurr + ((k * dt) / pow(h, 2.0)) * (solutionNext - 2 * solutionCurr + solutionPrev);
}

double calculTerm(double x, double t, int m)
{
	return exp(-k * pow(M_PI, 2.0) * pow((2 * m + 1), 2.0) * t) * sinf(M_PI * (2 * m + 1) * x) / (2 * m + 1);
}

double calcSeries(double x, double t)
{
	double newTerm = 0;
	double series = 0;
	double eps = 1e-8;
	int m = 0;

	while (1)
	{
		newTerm = calculTerm(x, t, m);
		series += newTerm;
		m++;

		if (fabs(newTerm) < eps)
		{
			return 4.0 * series / M_PI;
		}
	}
}

int main(int argc, char *argv[])
{
	
	int myrank, size;
	int i = 0;
	
	MPI_Status Status;  
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

	double start, end, total;
	start = MPI_Wtime();

	int solutionSize = (int)(1 / h);
	double* solution = calloc(solutionSize, sizeof(double));
	for (i = 0; i < solutionSize; i++)
	{
		solution[i] = 1;
	}
	solution[0] = 0;
	solution[solutionSize - 1] = 0;
	
	int bounds[2] = { 0 };
	int** allBounds = (int**)calloc(size, sizeof(int*));

	for (i = 0; i < size; i++)
	{
		allBounds[i] = (int*)calloc(2, sizeof(int));
	}

	double tempLastElem;
	double tempFirstElem;


	if (myrank != 0)
	{
		MPI_Recv(bounds, 2, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, &Status);
	}
	else
	{
		for (i = 1; i < size; i++)
		{
			bounds[0] = i * solutionSize / size;
			bounds[1] = ((i + 1) * solutionSize / size) - 1;
			
			allBounds[i][0] = bounds[0];
			allBounds[i][1] = bounds[1];
			//printf("%d", i);
			
			MPI_Send(bounds, 2, MPI_DOUBLE, i, 1, MPI_COMM_WORLD);
		}

		bounds[0] = 0;
		bounds[1] = ((myrank + 1) * solutionSize / size) - 1;
		
		allBounds[0][0] = bounds[0];
		allBounds[0][1] = bounds[1];
	}


	double j = 0;
	
	for (j = 0; j < T; j += dt)
	{
		//printf("j = %f, T = %f\n", j, T);
		if (myrank == 0)
		{
			if(size > 1)
			{
				MPI_Recv(&tempLastElem, 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD, &Status);
				MPI_Send(solution + bounds[1], 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD);

				int i = 0;
				if(bounds[0] != bounds[1])							// more that one element in the first array
				{
					for (i = bounds[0] + 1; i < bounds[1]; i++)
					{
						solution[i] = EquationStep(solution[i - 1], solution[i], solution[i + 1]);
					}
					solution[i] = EquationStep(solution[bounds[1] - 1], solution[bounds[1]], tempLastElem);
				}
			}
			else{

				if(bounds[0] != bounds[1])							// more that one element in the first array
				{
					for (i = bounds[0] + 1; i < bounds[1]; i++)
					{
						solution[i] = EquationStep(solution[i - 1], solution[i], solution[i + 1]);
					}
					solution[i] = EquationStep(solution[bounds[1] - 1], solution[bounds[1]], tempLastElem);
				}
			}

		}

		else if (myrank == size - 1)
		{
			MPI_Send(solution + bounds[0], 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD);
			MPI_Recv(&tempFirstElem, 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD, &Status);

			int i = 0;
			if (bounds[0] != bounds[1])
			{
				solution[bounds[0]] = EquationStep(tempFirstElem, solution[bounds[0]], solution[bounds[0] + 1]);
				
				for (i = bounds[0] + 1; i < bounds[1]; i++)
				{
					solution[i] = EquationStep(solution[i -1], solution[i], solution[i + 1]);
				}
			}
		}

		else
		{
			MPI_Sendrecv(solution + bounds[0], 1, MPI_DOUBLE, myrank - 1, 1, &tempLastElem, 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD, &Status);
			MPI_Sendrecv(solution + bounds[1], 1, MPI_DOUBLE, myrank + 1, 1, &tempFirstElem, 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD, &Status);
			
			if (bounds[0] != bounds[1])
			{
				int  i = 0;

				solution[bounds[0]] = EquationStep(tempFirstElem, solution[bounds[0]], solution[bounds[0] + 1]);
				for (i = bounds[0] + 1; i < bounds[1]; i++)
				{
					solution[i] = EquationStep(solution[i - 1], solution[i], solution[i + 1]);
				}
				solution[bounds[1]] = EquationStep(solution[bounds[1] - 1], solution[bounds[1]], tempLastElem);
			}
			else
			{
				solution[bounds[0]] = EquationStep(tempFirstElem, solution[bounds[0]], tempLastElem);
			}
		}
	}
	
	if (myrank != 0)
	{
		MPI_Send(solution + bounds[0], bounds[1] - bounds[0] + 1, MPI_DOUBLE, 0, 2,  MPI_COMM_WORLD);
	}

	else {
		for (i = 1; i < size; i++)
		{
			MPI_Recv(solution + allBounds[i][0], allBounds[i][1] - allBounds[i][0] + 1, MPI_DOUBLE, i, 2, MPI_COMM_WORLD, &Status);			
		}

		end = MPI_Wtime();
		total = end - start;

		printf("time = %f \n", total);

		int p = 0;
		for (p = 0; p < solutionSize; p = p + solutionSize / 10)
		{
			printf("%.4f, ", solution[p]);
		}
		printf("%.4f ", solution[solutionSize - 1]);

		printf("\n");

		printf("exactSolution: \n");

		double * exactSolution = calloc(solutionSize, sizeof(double));
		for (i = 0; i < solutionSize; i = i + solutionSize / 10)
		{
			exactSolution[i] = calcSeries((float)i / solutionSize, T);
			printf("%.4f, ", exactSolution[i]);
		}
		free(exactSolution);
	}



	for (i = 0; i < size; i++)
	{
		free(allBounds[i]);
	}
	free(allBounds);
	free(solution);
	

	MPI_Finalize();

	return 0;
}