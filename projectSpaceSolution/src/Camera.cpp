#include "Camera.hpp"
#include "Vector2f.hpp"
#include "Game.hpp"
#include <cmath>


extern const float defFov = 400.0f;


Camera::Camera()
{}

Camera::Camera(Game* owner, Vector2f currPos, Vector2f dstPos, float currFov, float dstFov, float currAng, float dstAng)
{
	this->owner = owner;

	this->currCoords = Coords2f(currPos, currAng);
	this->dstCoords = Coords2f(dstPos, dstAng);

	this->currFov = currFov;
	this->dstFov = dstFov;
}

void Camera::Update(float dt)
{
	float alpha = 1000.0f;

	this->InputHandler();

	//Shift
	Vector2f dir = dstCoords.GetPos() - currCoords.GetPos();
	this->currCoords.SetPos(dstCoords.GetPos() - dir * exp(-alpha * dt));

	//Rotate
	Vector2f rotateDir = dstCoords.GetXVector() - currCoords.GetXVector();
	this->currCoords.SetXVector((dstCoords.GetXVector() - rotateDir * exp(-alpha * dt)).GetNorma());

	//Scale
	float diff = dstFov - currFov;
	this->currFov = dstFov - diff * exp(-alpha * dt);
	
}

void Camera::SetDstCoords(Coords2f newCoords)
{
	this->dstCoords = newCoords;
}

void Camera::SetDstPos(Vector2f newPos)
{
	this->dstCoords.SetPos(newPos);
}

void Camera::SetDstFov(float newFov)
{
	this->dstFov = newFov;
}

float Camera::GetCurrFov()
{
	return this->currFov;
}

Vector2f Camera::GetCurrPos()
{
	return this->currCoords.GetPos();
}

Coords2f Camera::GetDstCoords()
{
	return this->dstCoords;
}

Coords2f Camera::GetCurrCoords()
{
	return this->currCoords;
}


Vector2f Camera::ConvertPosToLocal(Vector2f pos)
{
	return currCoords.GetLocalPoint(pos);
}

Vector2f Camera::ConvertVectorToLocal(Vector2f vector)
{
	return currCoords.GetLocalVector(vector);
}

void Camera::InputHandler()
{
	float deltaFov = 5.0f;
	if (owner->IsScalingCameraUp() && currFov > 100.0f)
	{
		dstFov = std::max(dstFov - deltaFov, 0.01f);
	}

	if (owner->IsScalingCameraDown() && currFov < 2500.0f)
	{
		dstFov += deltaFov;
	}
}

