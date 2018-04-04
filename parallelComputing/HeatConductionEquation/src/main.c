#pragma comment(lib, "mpi.lib")
#include<stdio.h>
#include<mpi.h>
#include<stdlib.h>

const double k = 1;
const double h = 0.2;
const double dt = 0.02;
const double T = 0.1;
const int p_size = 2;

double EquationStep(double solutionPrev, double solutionCurr, double solutionNext, double tau)
{
	return solutionCurr + (k * tau) * (solutionNext - 2 * solutionCurr + solutionPrev) / (h * h);
}

int main(int argc, char *argv[])
{
	int myrank, size;
	MPI_Status Status;     // òèï äàííûõ mpi

	MPI_Init(&argc, &argv);
	
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

	int solution_size = (int)(1 / h);
	double* solution = calloc(solution_size, sizeof(double));
	//printf("solution array size = %d\n", solution_size);
	int bounds[2];
	double tempLastElem;
	double tempFirstElem;

	double j = 0;
	for (j = 0; j < T; j += dt)
	{
		//printf("j = %f, T = %f\n", j, T);
		if (myrank == 0)
		{
			bounds[0] = 0;
			bounds[1] = ((myrank + 1) * solution_size / p_size) - 1;
			//printf("b0 = %d, b1 = %d\n", bounds[0], bounds[1]);
			MPI_Recv(&tempLastElem, 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD, &Status);
			MPI_Send(solution + bounds[1], 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD);

			int i = 0;
			for (i = bounds[0] + 1; i < bounds[1]; i++)
			{
				EquationStep(solution[i - 1], solution[i], solution[i + 1], dt);
			}
			EquationStep(solution[bounds[1] - 1], solution[bounds[1]], tempLastElem, dt);

		}

		else if (myrank == p_size - 1)
		{
			bounds[0] = myrank * solution_size / p_size;
			bounds[1] = ((myrank + 1) * solution_size / p_size) - 1;
			//printf("b0 = %d, b1 = %d\n", bounds[0], bounds[1]);
			MPI_Send(solution + bounds[0], 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD);
			MPI_Recv(&tempFirstElem, 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD, &Status);

			int i = 0;
			EquationStep(tempFirstElem, solution[bounds[0]], solution[bounds[0] + 1], dt);
			for (i = bounds[0] + 1; i < bounds[1]; i++)
			{
				EquationStep(solution[i], solution[i], solution[i + 1], dt);
			}
		}

		else
		{
			bounds[0] = myrank * solution_size / p_size;
			bounds[1] = ((myrank + 1) * solution_size / p_size) - 1;
			//printf("b0 = %d, b1 = %d\n", bounds[0], bounds[1]);
			MPI_Sendrecv(solution + bounds[0], 1, MPI_DOUBLE, myrank - 1, 1, &tempLastElem, 1, MPI_DOUBLE, myrank + 1, 1, MPI_COMM_WORLD, &Status);
			MPI_Sendrecv(solution + bounds[1], 1, MPI_DOUBLE, myrank + 1, 1, &tempFirstElem, 1, MPI_DOUBLE, myrank - 1, 1, MPI_COMM_WORLD, &Status);
			if (bounds[0] != bounds[1])
			{
				int  i = 0;
				EquationStep(tempFirstElem, solution[bounds[0]], solution[bounds[0] + 1], dt);
				for (i = bounds[0] + 1; i < bounds[1]; i++)
				{
					EquationStep(solution[i - 1], solution[i], solution[i + 1], dt);
				}
				EquationStep(solution[bounds[1] - 1], solution[bounds[1]], tempLastElem, dt);
			}
			else
			{
				EquationStep(tempFirstElem, solution[bounds[0]], tempLastElem, dt);
			}
		}
	}

	int p = 0;
	for (p = 0; p < solution_size; p++)
	{
		printf("%f ", p);
	}

	MPI_Finalize();
	free(solution);

	return 0;
}