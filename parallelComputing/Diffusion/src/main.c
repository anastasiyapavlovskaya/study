#include<stdio.h>
#include<stdlib.h>

#define T 4
#define c 1
#define h 0.1
#define tau 0.09

double LeftHandCorner(double solutionPrev, double solutionCurr)
{
	return solutionCurr - tau * c * (solutionCurr - solutionPrev) / h;
}

double RightHandCorner(double solutionCurr, double solutionNext)
{
	return solutionCurr - (tau * c / h) * (solutionNext - solutionCurr);
}

double g(double x)
{
	return (x >= 0 && x <= 2) ? x * (2 - x) : 0;
}


int main()
{
	int size = 10 / h;
	double* solution = (double*)calloc(size, sizeof(double));

	int i = 0;
	double x = 0;
	while (x <= 2.0)
	{
		solution[i] = g(x);
		x += h;
		i++;
	}

	printf("Initial Solution:\n");
	for (int i = 0; i < size; i++)
	{
		printf("%f, ", solution[i]);
	}
	printf("\n");

	int p = 0;
	double t = 0;
	for (t = 0; t < T; t += tau)
	{
		solution[0] = 0;

		for (i = size - 1; i > 0; i--)
		{
			solution[i] = LeftHandCorner(solution[i - 1], solution[i]);
		}

	}

	printf("Final Solution:\n");
	for (int i = 0; i < size; i++)
	{
		printf("%f, ", solution[i]);
	}
	printf("\n");

	double* exacSolution = (double*)calloc(size, sizeof(double));
	x = 0;
	printf("Exac solution:\n");
	for (i = 0; i < size; i++)
	{ 
		solution[i] = g(x - c * T);
		x += h;
		printf("%f, ", solution[i]);
	}
	printf("\n");

	free(solution);

	return 0;
}