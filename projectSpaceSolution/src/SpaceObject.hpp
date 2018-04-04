#pragma once
#include "GravityObject.hpp"
#include "BlackHole.hpp"
#include "Vector2f.hpp"
#include "Game.hpp"
#include "Sprite.hpp"
#include <string>


class SpaceObject : public GameObject
{
public:
	enum Type
	{
		Planet = 0,
		Star   = 1,
		Portal = 2
	};

	SpaceObject(Game* owner, Type type, Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, std::string textureFilename);
	virtual ~SpaceObject();
	Type GetType();
	Vector2f GetPos();

	virtual void Update(const float dt);
	virtual void Draw();
	virtual void SwitchOff();
	virtual void Drop();
	virtual bool IsExist();

private:
	GravityObject* gravityObject;
	Game* owner;
	Type type;
	bool isExist;
	Sprite sprite;
	BlackHoleEffect* blackHoleEffect;
};

