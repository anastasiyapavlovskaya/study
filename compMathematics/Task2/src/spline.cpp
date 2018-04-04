#include"spline.hpp"

void CalculCoeff(double h, std::vector<double>* f, std::vector<cubicSpline>* splinesForF)
{
	int N = (int)splinesForF->size();
	double A, B, C, F;

	std::vector<double> P(N + 1);
	P.at(0) = 0;
	std::vector<double> Q(N + 1);
	Q.at(0) = 0;
	
	splinesForF->at(0).c = 0;
	//splinesForF->at(0).a = f->at(0);


	for (int i = 1; i < N - 1; i++)
	{
		F = (6.0 / h) * (f->at(i + 1) - 2.0 * f->at(i) + f->at(i - 1));
		A = h;
		B = 2.0 * (h + h);
		C = h;
		P.at(i + 1) = -  C / (A * P.at(i) + B);
		Q.at(i + 1) = (F - A * Q.at(i)) / (A * P.at(i) + B);
		//std::cout << "F(" << i << ") = " << F << std::endl;
		//std::cout << "P(" << i << ") = " << P.at(i) << " Q(" << i << ") = " << Q.at(i) << std::endl;
	}

	//P.at(N) = -C / (A * P.at(N - 1) + B);
	//Q.at(N) = (F - A * Q.at(N - 1)) / (A * P.at(N - 1) + B);

	splinesForF->at(N - 1).c = P.at(N - 2) * splinesForF->at(N - 2).c + Q.at(N - 2);
	
	for (int i = N - 2; i > 0; --i)
	{
		splinesForF->at(i).c = P.at(i + 1) * splinesForF->at(i + 1).c + Q.at(i + 1);
	}
	//std::cout << "c(1) = " << splinesForF->at(1).c << std::endl;

	for (int i = 1; i < N; i++)
	{
		splinesForF->at(i).a = f->at(i);
		splinesForF->at(i).d = (splinesForF->at(i).c - splinesForF->at(i - 1).c) / h;
		//std::cout << "(" << f->at(i) << " - " << f->at(i - 1) << ") / " << h << std::endl;
		splinesForF->at(i).b = (f->at(i) - f->at(i - 1)) / h + h * (2.0 * splinesForF->at(i).c + splinesForF->at(i - 1).c)/6.0;
	}
}		

double cubicSpline::MaxDeviation(double step)
{
	int N = 20;
	double maxDeviation = 0;
	for (int i = 1; i < N; i++)
	{
		double x = this->xi - step * i / N;
		double Si = this->a + this->b * (x - this->xi) + this->c * pow(x - this->xi, 2) / 2 + this->d * pow(x-this->xi, 3) / 6;
		double deviation = sin(x) - Si;
		if (deviation > maxDeviation)
		{
			maxDeviation = deviation;
		}
	}
	return maxDeviation;
}

void cubicSpline::PrintSpline()
{
	std::cout << this->a << " + " << this->b << " * (x - " << this->xi << ") + " << this->c << " * (1/2) * (x - " << this->xi << ")^2 + " << this->d << " * (1/6) * (x - " << this->xi << ")^3" << std::endl;
}