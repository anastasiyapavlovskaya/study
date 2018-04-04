#pragma once
#include "Vector2f.hpp"


class Coords2f
{
public:
	Coords2f(){};
	Coords2f(Vector2f pos, float ang);
	void SetPos(Vector2f pos);
	void SetXVector(Vector2f xVector);

	Vector2f GetPos();
	Vector2f GetXVector();
	Vector2f GetYVector();
	float GetAng();
	Vector2f GetWorldPoint(Vector2f localPoint);
	Vector2f GetLocalPoint(Vector2f worldPoint);
	Vector2f GetWorldVector(Vector2f localVector);
	Vector2f GetLocalVector(Vector2f worldVector);

private:
	Vector2f pos;
	//float ang;
	Vector2f xVector, yVector;
};