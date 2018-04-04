#include "integration.hpp"

double function(double value)
{
	return log(1 / sin(value));
}

double RegularFunction(double value)
{
	return (value > 0) ? log(1 / sin(value)) + log(value) : 0;
}

double SimpleFunction(double value)
{
	return pow(value, 6.0);
}

double CaluclateIntegral(double lowLimit, double upperLimit)
{
	double nNodeIntegral = 0;
	double halfNNodeIntegral = 1;
	int N = 2;

	double x1 = lowLimit;
	double x2 = upperLimit;
	double step = (upperLimit - lowLimit) / N;

	while (fabs((halfNNodeIntegral - nNodeIntegral) / 15) > 1e-10)
	{
		nNodeIntegral = halfNNodeIntegral;
		halfNNodeIntegral = 0;

		N *= 2;
		step = (upperLimit - lowLimit) / N;

		x1 = lowLimit;
		x2 = lowLimit + step;

		for (int i = 0; i < N; i++)
		{
			halfNNodeIntegral += (step / 2) * (RegularFunction(x1 / 2 + x2 / 2 - x2 / (2 * sqrt(3)) + x1 / (2 * sqrt(3))) + RegularFunction(x1 / 2 + x2 / 2 + x2 / (2 * sqrt(3)) - x1 / (2 * sqrt(3))));
			x1 += step;
			x2 += step;
			//std::cout << "lowLim = " << x1 << " upLim = " << x2 << " res = " << res << std::endl;
		}
		std::cout << "2N = " << 2 * N << " integralValue = " << halfNNodeIntegral << " N = " << N << " integralValue = " << nNodeIntegral << std::endl;
		if (N > 10000)
		{
			std::cout << "too many iterations" << std::endl;
		}
	}
	return nNodeIntegral;
}
