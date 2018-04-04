#include "integration.hpp"

float function(float value)
{
	return log(1 / sin(value));
}

float CaluclateIntegral(float lowLimit, float upperLimit)
{
	float res = 0;
	res = (upperLimit - lowLimit) * (function(lowLimit / 2.0f + upperLimit / 2.0f - upperLimit / (2.0f * sqrt(3)) + lowLimit / (2.0f * sqrt(3))) + function(lowLimit / 2.0f + upperLimit / 2.0f + upperLimit / (2.0f * sqrt(3)) - lowLimit / (2.0f * sqrt(3)))) / 2.0f;
	return res;
}
