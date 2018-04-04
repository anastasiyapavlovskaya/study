#include "SpaceObject.hpp"
#include <assert.h>


SpaceObject::SpaceObject(Game* owner, Type type, Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, std::string textureFilename)
{
	assert(owner);
	this->owner = owner;
	this->type = type;
	this->isExist = true;
	if (type == Type::Portal)
	{
		this->gravityObject = owner->GetGravitySystem()->AddGravityObject(currPos, prevPos, acceleration, mass, radius, (int)GameObject::Type::PortalType, this);
	}
	else
	{
		this->gravityObject = owner->GetGravitySystem()->AddGravityObject(currPos, prevPos, acceleration, mass, radius, (int)GameObject::Type::SpaceObjectType, this);
	}

	this->sprite = Sprite(textureFilename);
	
	if (type == Type::Portal)
	{
		this->blackHoleEffect = new BlackHoleEffect();
	}
}

SpaceObject::~SpaceObject()
{
	//delete this->gravityObject;
}

SpaceObject::Type SpaceObject::GetType()
{
	return this->type;
}

void SpaceObject::SwitchOff()
{
	this->isExist = false;
}

void SpaceObject::Drop()
{
	this->owner->GetGravitySystem()->RemoveGravityObject(this->gravityObject);
	this->gravityObject = NULL;
}

bool SpaceObject::IsExist()
{
	return this->isExist;
}

Vector2f SpaceObject::GetPos()
{
	return this->gravityObject->GetPos();
}


void SpaceObject::Update(const float dt)
{
	if (type == Type::Portal)
	{
		this->blackHoleEffect->Update(dt);
	}
}

void SpaceObject::Draw()
{
	Vector2f size = Vector2f(gravityObject->GetRadius() * 2.0f, gravityObject->GetRadius() * 2.0f);
	switch (this->type)
	{
		case Type::Planet:
			size *= 1.01f;
			sprite.Draw(this->owner->GetWindow(), this->gravityObject->GetPos(), 0.0f, size, this->owner->GetCamera());
		break;

		case Type::Star: 
			size *= 3.0f;
			sprite.Draw(this->owner->GetWindow(), this->gravityObject->GetPos(), 0.0f, size, this->owner->GetCamera());
		break;

		case Type::Portal:
			size *= 3.10f;
			this->blackHoleEffect->Render(this->owner->GetWindow(),this->owner->GetCamera(), 0.0f, this->gravityObject->GetPos(), size);
		break;
	}
	
}

