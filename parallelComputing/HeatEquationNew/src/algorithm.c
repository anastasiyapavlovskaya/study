double EquationStep(double solutionPrev, double solutionCurr, double solutionNext)
{
 	//printf("solutionPrev = %f, solutionCurr = %f, solutionNext = %f \n", solutionPrev, solutionCurr, solutionNext);
	return solutionCurr + ((k * dt) / pow(h, 2.0)) * (solutionNext - 2 * solutionCurr + solutionPrev);
}


	int solutionSize = (int)1 / h;
	double* solution = calloc(solutionSize, sizeof(double));
	for (int i = 0; i < solutionSize; i++)
	{
		solution[i] = 1;
	}

	solution[0] = 0;
	solution[solutionSize - 1] = 0;


	for (double t = 0; t < 0.5*T; t+=dt)
	{
		for (int i = 1; i < solutionSize - 1; i++)
		{
			solution[i] = EquationStep(solution[i - 1], solution[i], solution[i + 1]);
			//printf("%f \n", solution[i]);
		}

	}

	for (int i = 0; i < solutionSize; i++)
	{
		printf("%f, ", solution[i]);
	}
