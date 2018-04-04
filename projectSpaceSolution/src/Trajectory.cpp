#include "Trajectory.hpp"
#include "SpaceObject.hpp"
#include "Ship.hpp"
#include <assert.h>


const int futurePosCount = 500;



GravityCollisionHandler::GravityCollisionHandler(TrajectoryComputer* owner)
{
	assert(owner);
	this->owner = owner;
}

void GravityCollisionHandler::OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2)
{
	assert(gravityObject1);
	assert(gravityObject2);

	switch ((GameObject::Type) gravityObject1->GetType())
	{
		case GameObject::Type::ShipType:
			switch ((GameObject::Type) gravityObject2->GetType())
			{
				case GameObject::Type::ShipType:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
				break;

				case GameObject::Type::SpaceObjectType:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
				break;

				case GameObject::Type::PortalType:
					this->owner->SetGreenIndex(gravityObject1);
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
				break;

				default: assert(0);
			}
		break;

		case GameObject::Type::SpaceObjectType:
			switch ((GameObject::Type) gravityObject2->GetType())
			{
				case GameObject::Type::ShipType:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
				break;

				case GameObject::Type::PortalType:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
				break;

				case GameObject::Type::SpaceObjectType:
					switch (((SpaceObject*)gravityObject1->GetOwner())->GetType())
					{
						case SpaceObject::Type::Planet:
							switch (((SpaceObject*)gravityObject2->GetOwner())->GetType())
							{
								case SpaceObject::Type::Planet:
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
								break;

								case SpaceObject::Type::Star:
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
								break;

								default: assert(0);
							}
						break;

						case SpaceObject::Type::Star:
							switch (((SpaceObject*)gravityObject2->GetOwner())->GetType())
							{
								case SpaceObject::Type::Planet:
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
								break;

								case SpaceObject::Type::Star:
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
									this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
								break;

								default: assert(0);
							}
						break;

						default: assert(0);
					}
				break;

				default: assert(0);
			}
		break;

		case GameObject::Type::PortalType:
			switch ((GameObject::Type) gravityObject2->GetType())
			{
				case GameObject::Type::ShipType:
					this->owner->SetGreenIndex(gravityObject2);
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
				break;

				case GameObject::Type::PortalType:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject2);
				break;

				default:
					this->owner->GetGravitySystem()->ForgetGravityObject(gravityObject1);
			}
		break;

		default: assert(0);
	}
}




TrajectoryComputer::TrajectoryComputer(Game* owner)
: computeSys(&this->gravityCollisionHandler)
, gravityCollisionHandler(this)
{
	Sprite newbie("Data/Trajectory/blueLine.png");
	sprites.push_back(newbie);
	newbie = Sprite("Data/Trajectory/redLine.png");
	sprites.push_back(newbie);
	newbie = Sprite("Data/Trajectory/greenLine.png");
	sprites.push_back(newbie);

	assert(owner);
	this->owner = owner;
	this->isExist = true;
	this->dt = 0.0f;
	this->greenIndex = -1;
}

TrajectoryComputer::~TrajectoryComputer()
{
	// nothing
}

void TrajectoryComputer::SwitchOff()
{
	this->isExist = false;
}

void TrajectoryComputer::Drop()
{
	// nothing
}

bool TrajectoryComputer::IsExist()
{
	return this->isExist;
}

void TrajectoryComputer::Update(const float dt)
{
	this->dt = dt;
}


void TrajectoryComputer::Clone()
{
	GravityObjectSystem* sourceSys = this->owner->GetGravitySystem();
	assert(sourceSys);
	this->computeSys.RemoveAllGravityObjects();
	this->greenIndex = -1;
	const int gravObjCount = sourceSys->GetGravityObjectsCount();

	for (int i = 0; i < gravObjCount; i++)
	{
		GravityObject* newbie = new GravityObject;
		*newbie = *(sourceSys->GetGravityObject(i));
		this->computeSys.AddGravityObject(newbie);
	}
}


GravityObjectSystem* TrajectoryComputer::GetGravitySystem()
{
	assert(&(this->computeSys));
	return &(this->computeSys);
}


void TrajectoryComputer::InitializeColors()
{
	this->colors.clear();
	const int gravObjCount = this->computeSys.GetGravityObjectsCount();
	for (int i = 0; i < gravObjCount; i++)
	{
		this->colors.push_back(Color::Green);
	}
}

void TrajectoryComputer::SetGreenIndex(GravityObject* gravityObject)
{
	assert(gravityObject);
	this->greenIndex = this->computeSys.GetGravityObjectIndex(gravityObject);
}


void TrajectoryComputer::CalculateTrajectories()
{
	Clone();
	InitializeColors();

	for (int i = 0; i < futurePosCount; i++)
	{
		this->computeSys.CalculateAccelereations();
		this->computeSys.IntegrateSystem(this->dt);
	}

	const int gravObjCount = this->computeSys.GetGravityObjectsCount();
	assert((int) this->colors.size() == gravObjCount);
	for (int i = 0; i < gravObjCount; i++)
	{
		if (this->computeSys.GetGravityObject(i) == NULL)
		{
			this->colors[i] = Color::Red;
		}
		else
		{
			this->colors[i] = Color::White;
		}
	}

	if (0 <= this->greenIndex)
	{
		assert(this->greenIndex < gravObjCount);
		this->colors[greenIndex] = Color::Green;
	}
}


void TrajectoryComputer::Draw()
{
	CalculateTrajectories();
	Clone();

	const float minLenght = 1.0f;
	std::vector <Vector2f> prevPoint;
	for (int i = 0; i < this->computeSys.GetGravityObjectsCount(); i++)
	{
		prevPoint.push_back(this->computeSys.GetGravityObject(i)->GetPos());
	}

	for (int i = 1; i <= futurePosCount; i++)
	{
		if (this->owner->IsShowTrajectory())
		{
			for (int j = 0; j < this->computeSys.GetGravityObjectsCount(); j++)
			{
				if (this->computeSys.GetGravityObject(j) != NULL)
				{
					if ((this->computeSys.GetGravityObject(j)->GetPos() - prevPoint[j]).GetLength() >= minLenght)
					{
						float objectRadius = this->computeSys.GetGravityObject(j)->GetRadius();
						Vector2f objectPos = this->computeSys.GetGravityObject(j)->GetPos();
						Vector2f firstPos = prevPoint[j];
						Vector2f secondPos = objectPos;
						prevPoint[j] = objectPos;
						this->sprites[this->colors[j]].DrawTrajectory(this->owner->GetWindow(), firstPos, secondPos, this->owner->GetCamera());
					}
				}
			}
		}

		this->computeSys.CalculateAccelereations();
		this->computeSys.IntegrateSystem(this->dt);
	}
}


