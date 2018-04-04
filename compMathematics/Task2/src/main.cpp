#include<math.h>
#include"spline.hpp"
#include"integration.hpp"

int main()
{
	/*
	int N = 10;
	double pi = 4 * std::atan(1.);
	double step = 2.0f * pi / (N - 1);
	//float step = 2.0f / (N - 1);
	std::vector<double> f(N);
	std::vector<cubicSpline> splinesForF(N);		//N - 1

	for (int i = 0; i < N; i++)					//N - 1
	{
		f.at(i) = std::sin(i * step);
		//f.at(i) = pow(i * step, 2);
		splinesForF.at(i).xi = i * step;
	}

	CalculCoeff(step, &f, &splinesForF);
	
	for (int i = 1; i < N; i++)					//	for (int i = 0; i < N - 1; i++)
	{
		splinesForF.at(i).PrintSpline();
	}

	double maxDeviation = 0;
	for (int i = 1; i < N; i++)
	{
		float iDeviation = splinesForF.at(i).MaxDeviation(step);
		if (iDeviation > maxDeviation)
		{
			maxDeviation = iDeviation;
		}
	}

	std::cout << "N = " << N << " step = " << step << " maxDeviation = " << maxDeviation << std::endl;
	*/
	/*
	double pi = 4 * std::atan(1);
	double lowLimit = 0;
	double upperLimit = pi / 2.0f;
	
	std::cout << "value = " << CaluclateIntegral(lowLimit, upperLimit) + (pi / 2) * (1 - log(pi / 2)) << std::endl;
	*/
	double x = 0;
	double x0 = -9.958;
	double y = 0;
	double dy = 0;

	while (1)
	{
		std::cin >> x;
		for (int i = 0; i < 6; i++)
		{
			y = 1 / pow(x+1, 0.5);
			std::cout << "y = " << y << std::endl;
			x = y;
		}
		std::cout << "y = " << y << std::endl;

		//dy = 0.005 * pow(x, 4.0) + 2.0 * x;
		//std::cout << "dy = " << dy << std::endl;
		//std::cout << "y/dy = " << y / dy << std::endl;
		//std::cout << "x0 - y/dy = " << x0 - (y / dy) << std::endl;
	}
	return 0;
}