#pragma once
#include "Vector2f.hpp"
#include <vector>

class Functor;




class GravityObjectLocation
{
	friend class GravityObject;

public:
	GravityObjectLocation(int type, void* ptr);

private:
	int type;
	void* ptr;
};


class GravityObject
{
	friend class GravityObjectSystem;
	friend class Ship;

public:
	GravityObject();
	GravityObject(Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, int type, void* ptr);
	Vector2f GetPos();
	float GetRadius();
	void IntegrateGravityObject(const float dt);
	int GetType();
	void* GetOwner();

private:
	Vector2f currPos;
	Vector2f prevPos;
	Vector2f acceleration;
	float mass;
	float radius;
	GravityObjectLocation location;
	bool isExist;
};


class GravityObjectSystem
{
public:
	GravityObjectSystem(Functor* onCollisionFunctor);
	~GravityObjectSystem();
	int GetGravityObjectsCount();
	int GetGravityObjectIndex(GravityObject* gravityObject);
	GravityObject* AddGravityObject(GravityObject* gravityObject);
	GravityObject* AddGravityObject(Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, int type, void* ptr);
	GravityObject* GetGravityObject(const int index);
	void RemoveGravityObject(GravityObject* gravityObject);
	void RemoveAllGravityObjects();
	void ForgetGravityObject(GravityObject* gravityObject);
	bool IfCollision(GravityObject* gravityObject1, GravityObject* gravityObject2);
	void CalculateAccelereations();
	void IntegrateSystem(const float dt);
	
private:
	std::vector <GravityObject*> gravityObjects;
	Functor* onCollisionFunctor;
};

