#pragma once
#include "GravityObject.hpp"
#include "Vector2f.hpp"
#include "Game.hpp"
#include "Sprite.hpp"
#include "Flame.hpp"


class Ship : public GameObject
{
public:
	enum Status
	{
		Alive    = 0,
		Crashed  = 1,
		InPortal = 2
	};

	Ship(Game* owner, float angle, float fuelMass, Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, std::string textureFilename);
	virtual ~Ship();
	float GetFuelFraction();
	float GetAngle();
	void Rotate(const float dAngle);
	void SwitchEngine(bool isMovingAhead, bool isMovingBack);
	void CatchByPortal(Vector2f portalPos, const float leftTime, const float dt);
	
	virtual void Update(const float dt);
	virtual void Draw();
	virtual void SwitchOff();
	virtual void Drop();
	virtual bool IsExist();

private:
	float angle;
	float fuelMass;
	float initialFuelMass;
	float initialRadius;
	bool isEngineWork;
	Status status;
	Vector2f direction;
	GravityObject* gravityObject;
	Game* owner;
	Sprite sprite;
	Flame flameEffect;
};

