#include<vector>
#include<iostream>

struct cubicSpline
{
	double a;
	double b;
	double c;
	double d;
	double xi;
	void PrintSpline();
	double MaxDeviation(double step);
};

void CalculCoeff(double h, std::vector<double>* f, std::vector<cubicSpline>* splinesForF);
