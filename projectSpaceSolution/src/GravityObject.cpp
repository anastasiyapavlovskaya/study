#include "GravityObject.hpp"
#include "Game.hpp"
#include <assert.h>

const float gravityGamma = 6.67f;


// GravityObjectLocation

GravityObjectLocation::GravityObjectLocation(int type, void* ptr)
{
	this->type = type;
	this->ptr = ptr;
}

// GravityObjectLocation



// GravityObject
GravityObject::GravityObject()
: location(-1, NULL)
{
	this->currPos = Vector2f(0.0f, 0.0f);
	this->prevPos = Vector2f(0.0f, 0.0f);
	this->acceleration = Vector2f(0.0f, 0.0f);
	this->mass = 0.0f;
	this->radius = 0.0f;
}

GravityObject::GravityObject(Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, int type, void* ptr)
: location(type, ptr)
{
	this->currPos = currPos;
	this->prevPos = prevPos;
	this->acceleration = acceleration;
	this->mass = mass;
	this->radius = radius;
}

void GravityObject::IntegrateGravityObject(const float dt)
{
	Vector2f pos = this->currPos;
	this->currPos = (this->currPos) * 2.0f - this->prevPos + this->acceleration * dt * dt;
	this->prevPos = pos;
}

Vector2f GravityObject::GetPos()
{
	return this->currPos;
}

float GravityObject::GetRadius()
{
	return this->radius;
}

int GravityObject::GetType()
{
	return this->location.type;
}

void* GravityObject::GetOwner()
{
	return this->location.ptr;
}

//GravityObject




//GravityObjectSystem

GravityObjectSystem::GravityObjectSystem(Functor* onCollisionFunctor)
{
	this->onCollisionFunctor = onCollisionFunctor;
}

GravityObjectSystem::~GravityObjectSystem()
{
	const int gravObjCount = this->gravityObjects.size();
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->gravityObjects[i] != NULL)
		{
			delete this->gravityObjects[i];
		}
	}
}


int GravityObjectSystem::GetGravityObjectsCount()
{
	return this->gravityObjects.size();
}
/*
int GravityObjectSystem::SetTimeScale(float newTimeScale)
{
	float velocityMult = newTimeScale / this->currTimeScale;
	this->dt = this->defaultDt * newTimeScale;

	for (int i = 0; i < count; i++)
	{
		Vector2f delta = p[i].pos - p[i].prevPos;
		p[i].prevPos = p[i].pos 
	}

	return this->gravityObjects.size();
}*/

int GravityObjectSystem::GetGravityObjectIndex(GravityObject* gravityObject)
{
	assert(gravityObject);
	const int gravObjCount = this->gravityObjects.size();
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->gravityObjects[i] == gravityObject)
		{
			return i;
		}
	}

	return -1;
}

void GravityObjectSystem::RemoveGravityObject(GravityObject* gravityObject)
{
	assert(gravityObject);
	const int gravObjCount = this->gravityObjects.size();
	int index = 0;
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->gravityObjects[i] == gravityObject)
		{
			index = i;
			break;
		}
	}

	GravityObject* auxPtr = this->gravityObjects[gravObjCount - 1];
	delete this->gravityObjects[index];
	this->gravityObjects[index] = auxPtr;
	this->gravityObjects.pop_back();
}

void GravityObjectSystem::ForgetGravityObject(GravityObject* gravityObject)
{
	assert(gravityObject);
	int gravObjCount = this->gravityObjects.size();
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->gravityObjects[i] == gravityObject)
		{
			delete this->gravityObjects[i];
			this->gravityObjects[i] = NULL;
			break;
		}
	}
}


void GravityObjectSystem::RemoveAllGravityObjects()
{
	const int gravObjCount = this->gravityObjects.size();
	for (int i = 0; i < gravObjCount; i++)
	{
		delete this->gravityObjects[i];
	}

	this->gravityObjects.clear();
}

GravityObject* GravityObjectSystem::AddGravityObject(GravityObject* gravityObject)
{
	assert(gravityObject);
	this->gravityObjects.push_back(gravityObject);
	return gravityObject;
}

GravityObject* GravityObjectSystem::AddGravityObject(Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, int type, void* ptr)
{
	assert(ptr);
	GravityObject* newbie = new GravityObject(currPos, prevPos, acceleration, mass, radius, type, ptr);
	this->gravityObjects.push_back(newbie);
	return newbie;
}

GravityObject* GravityObjectSystem::GetGravityObject(const int index)
{
	assert(0 <= index && index < (int) this->gravityObjects.size());
	return this->gravityObjects[index];
}


void GravityObjectSystem::CalculateAccelereations()
{
	const int gravObjCount = this->gravityObjects.size();
	Vector2f gravObjAcceleration(0.0f, 0.0f);
	Vector2f delta(0.0f, 0.0f);
	float distance = 0.0f;
	float accelerationAbs = 0.0f;

	for (int i = 0; i < gravObjCount; i++)
	{
		for (int j = 0; j < gravObjCount; j++)
		{
			if (i != j && this->gravityObjects[i] != NULL && this->gravityObjects[j] != NULL)
			{
				delta = this->gravityObjects[j]->currPos - this->gravityObjects[i]->currPos;
				distance = sqrt(delta.x * delta.x + delta.y * delta.y);
				accelerationAbs = gravityGamma * (this->gravityObjects[j]->mass) / (distance * distance);
				gravObjAcceleration += delta * (accelerationAbs / distance);
			}
		}

		if (this->gravityObjects[i] != NULL)
		{
			this->gravityObjects[i]->acceleration = gravObjAcceleration;
		}
		gravObjAcceleration = Vector2f(0.0f, 0.0f);
	}
}

void GravityObjectSystem::IntegrateSystem(const float dt)
{
	const int gravObjCount = this->gravityObjects.size();
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->gravityObjects[i] != NULL)
		{
			this->gravityObjects[i]->IntegrateGravityObject(dt);
		}
	}

	for (int i = 0; i < gravObjCount; i++)
	{
		for (int j = i; j < gravObjCount; j++)
		{
			if (i != j && this->gravityObjects[i] != NULL && this->gravityObjects[j] != NULL)
			{
				if (IfCollision(this->gravityObjects[i], this->gravityObjects[j]) && this->onCollisionFunctor != NULL)
				{
					this->onCollisionFunctor->OnCollision(this->gravityObjects[i], this->gravityObjects[j]);
				}
			}
		}
	}
}


bool GravityObjectSystem::IfCollision(GravityObject* gravityObject1, GravityObject* gravityObject2)
{
	assert(gravityObject1);
	assert(gravityObject2);

	Vector2f delta = gravityObject2->GetPos() - gravityObject1->GetPos();
	const float minDistance = gravityObject2->GetRadius() + gravityObject1->GetRadius();
	if (delta.GetLength() < minDistance)
	{
		return true;
	}
	else
	{
		return false;
	}
}

// GravityObjectSystem

