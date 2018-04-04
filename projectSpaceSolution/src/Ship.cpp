#include "Ship.hpp"
#include <assert.h>

//extern const float gameStatusTimer;
extern const float gameStatusTimer = 2.0f;
const float fuelVelocity = 500.0f;



Ship::Ship(Game* owner, float angle, float fuelMass, Vector2f currPos, Vector2f prevPos, Vector2f acceleration, float mass, float radius, std::string textureFilename)
{
	assert(owner);
	this->owner = owner;
	this->gravityObject = owner->GetGravitySystem()->AddGravityObject(currPos, prevPos, acceleration, mass, radius, (int) GameObject::Type::ShipType, this);
	this->angle = angle;
	this->initialFuelMass = fuelMass;
	this->fuelMass = fuelMass;
	this->initialRadius = radius;
	this->isEngineWork = false;
	this->direction = Vector2f(0.0f, 0.0f);
	this->status = Status::Alive;
	this->sprite = Sprite(textureFilename);

	owner->AddShip(this);
}

Ship::~Ship()
{
	this->owner->RemoveShip();
	//delete this->gravityObject;
}

float Ship::GetAngle()
{
	return this->angle;
}

float Ship::GetFuelFraction()
{
	return (this->fuelMass) / (this->initialFuelMass);
}

void Ship::Rotate(const float dAngle)
{
	const float fuelLoss = 0.01f;
	const float pi = 3.14159265f;

	if (this->fuelMass > fuelLoss)
	{
		this->fuelMass -= fuelLoss;
		this->angle += dAngle;
	}
	else
	{
		this->angle += dAngle * this->fuelMass / fuelLoss;
		this->fuelMass = 0.0f;
	}

	this->angle = fmod(this->angle, 2 * pi);
}

void Ship::Update(const float dt)
{
	//owner->AddObject(new Rocket(owner, ...));
	//std::cout << "ang = " << angle * 180.0f / 3.14159265f << "  fuelMass = " << fuelMass << std::endl;
	
	if (this->status == Status::InPortal)
	{
		this->gravityObject->IntegrateGravityObject(dt);
		this->gravityObject->radius = this->initialRadius * (1.0f - 1.1f * (::gameStatusTimer - this->owner->GetGameStatusTimer()) / ::gameStatusTimer);
		if (this->gravityObject->radius <= 0.0f)
		{
			this->gravityObject->radius = 0.0f;
		}
	}
	else
	{
		if (this->isEngineWork && fuelMass > 0.0f)
		{
			const float fuelMassLoss = 10.0f;

			if (fuelMass > fuelMassLoss * dt)
			{
				this->gravityObject->acceleration += this->direction * (fuelMassLoss * fuelVelocity / (this->gravityObject->mass + this->fuelMass));
				this->fuelMass -= fuelMassLoss * dt;
			}
			else
			{
				this->gravityObject->acceleration += this->direction * (fuelMass * fuelVelocity / (this->gravityObject->mass + this->fuelMass));
				this->fuelMass = 0.0f;
			}
		}
	}
	this->flameEffect.Update(dt);
	this->owner->GetCamera()->SetDstPos(this->gravityObject->GetPos());
}


void Ship::SwitchEngine(bool isMovingAhead, bool isMovingBack)
{
	this->isEngineWork = isMovingAhead ^ isMovingBack;
	if (isMovingAhead && !isMovingBack)
	{
		direction = Vector2f(cos(this->angle), -sin(this->angle));
	}
	if (!isMovingAhead && isMovingBack)
	{
		direction = Vector2f(-cos(this->angle), sin(this->angle));
	}
}

void Ship::CatchByPortal(Vector2f portalPos, const float leftTime, const float dt)
{
	assert(leftTime > 0.0f);
	this->gravityObject->prevPos = (this->gravityObject->currPos - portalPos) * (dt / leftTime) + this->gravityObject->currPos;
	this->gravityObject->acceleration = Vector2f(0.0f, 0.0f);
	this->status = Status::InPortal;
}


void Ship::SwitchOff()
{
	this->status = Status::Crashed;
}

void Ship::Drop()
{
	this->owner->GetGravitySystem()->RemoveGravityObject(this->gravityObject);
	this->gravityObject = NULL;
	this->owner->RemoveShip();
}

bool Ship::IsExist()
{
	if (this->status == Status::Alive || this->status == Status::InPortal)
	{
		return true;
	}
	else
	{
		return false;
	}
}


void Ship::Draw()
{
	Vector2f size = Vector2f(gravityObject->GetRadius() * 2.0f, gravityObject->GetRadius() * 2.0f);
	Camera* camera = this->owner->GetCamera();
	Vector2f pos = this->gravityObject->GetPos();
	
	Vector2f flameSize;
	if (this->isEngineWork && this->fuelMass > 0.0f)
	{
		flameSize = size * 0.7f * std::max(this->GetFuelFraction(), 0.4f);
	}
	else
	{
		flameSize = Vector2f(0.0f, 0.0f);
	}
	this->flameEffect.Render(this->owner->GetWindow(), this->owner->GetCamera(), this->angle, pos - Vector2f(cosf(angle) * size.x * 0.5, -sinf(angle) * size.y * 0.5f), flameSize);
	sprite.Draw(this->owner->GetWindow(), pos, angle, size, camera);
}














