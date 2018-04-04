#include "Explosion.hpp"
#include <assert.h>

const float existingTime = 1.5f; //how many seconds explosion exists


Explosion::Explosion(Game* owner, Vector2f pos, float explosionSize)
{
	assert(owner);
	this->owner = owner;
	this->pos = pos;
	this->explosionSize = explosionSize;
	this->elapsedTime = 0.0f;

	Sprite newbie("Data/Explosion/1.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/2.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/3.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/4.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/5.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/6.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/7.png");
	sprite.push_back(newbie);
	newbie = Sprite("Data/Explosion/8.png");
	sprite.push_back(newbie);
}

Explosion::~Explosion()
{
	// nothing have to do
}


void Explosion::Update(const float dt)
{
	this->elapsedTime += dt;
}

void Explosion::Draw()
{
	Vector2f size(this->explosionSize * this->elapsedTime * 7.0f, this->explosionSize * this->elapsedTime * 7.0f);
	int spriteIndex = (int)(elapsedTime / 0.15f);
	if (spriteIndex < (int)sprite.size())
	{
		sprite[spriteIndex].Draw(owner->GetWindow(), pos, 0.0f, size, owner->GetCamera());
	}	
}

void Explosion::SwitchOff()
{
	this->elapsedTime = existingTime; 
}

void Explosion::Drop()
{
	// nothing have to do
}

bool Explosion::IsExist()
{
	if (this->elapsedTime < existingTime)
	{
		return true;
	}
	else
	{
		return false;
	}
}