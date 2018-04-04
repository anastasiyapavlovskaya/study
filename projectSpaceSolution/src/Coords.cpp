#include "Coords.hpp"
#include <math.h>
#include <iostream>

Coords2f::Coords2f(Vector2f pos, float ang)
{
	this->pos = pos;
	this->xVector = Vector2f(cosf(ang), -sinf(ang));
	this->yVector = Vector2f(sinf(ang), cosf(ang));
}

Vector2f Coords2f::GetPos(){
	return this->pos;
}

Vector2f Coords2f::GetXVector(){
	return this->xVector;
}

Vector2f Coords2f::GetYVector(){
	return this->yVector;
}

float Coords2f::GetAng()
{
	return atan(xVector.y / xVector.x);
}

void Coords2f::SetPos(Vector2f pos){
	this->pos = pos;
}

void Coords2f::SetXVector(Vector2f xVector)
{
	this->xVector = xVector;
	this->yVector = xVector.GetPerpendicular();
}

Vector2f Coords2f::GetWorldPoint(Vector2f localPoint)
{
	return pos + localPoint.x * xVector + localPoint.y * yVector;
}

Vector2f Coords2f::GetLocalPoint(Vector2f worldPoint)
{
	Vector2f localPoint = worldPoint - pos;
	return Vector2f(localPoint * xVector, localPoint * yVector);
}

Vector2f Coords2f::GetWorldVector(Vector2f localVector)
{
	return localVector.x * xVector + localVector.y * yVector;
}

Vector2f Coords2f::GetLocalVector(Vector2f worldVector)
{
	return Vector2f(worldVector * xVector, worldVector * yVector);
}



