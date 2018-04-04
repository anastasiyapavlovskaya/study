#include "Game.hpp"
#include "SpaceObject.hpp"
#include "Ship.hpp"
#include "Explosion.hpp"
#include "Trajectory.hpp"
#include "Camera.hpp"
#include "Vector2f.hpp"
#include <assert.h>

const sf::Time timePerFrame = sf::seconds(1.0f / 60.0f);
const float minTimeWarp = 0.25f;
const float maxTimeWarp = 2.00f;
const float dTimeWarp = 0.05f;
const float dAngle = 0.05f;
extern const float defFov;
extern const float gameStatusTimer;

// GameObject
GameObject::~GameObject()
{
	//std::cout << "destructor gameobject" << std::endl;
}
// GameObject


void Functor::OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2)
{
	// nothing have to do
}

CollisionHandler::CollisionHandler(Game* owner)
{
	this->owner = owner;
}

void CollisionHandler::OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2)
{
	assert(gravityObject1);
	assert(gravityObject2);
	Vector2f collisionPoint = gravityObject1->GetPos() + (gravityObject2->GetPos() - gravityObject1->GetPos()).GetNorma() * (gravityObject1->GetRadius());

	switch ((GameObject::Type) gravityObject1->GetType())
	{
		case GameObject::Type::ShipType:
			switch ((GameObject::Type) gravityObject2->GetType())
			{
				case GameObject::Type::ShipType:
					this->owner->CollapseGameObjects(&collisionPoint, gravityObject1, gravityObject2);
				break;

				case GameObject::Type::SpaceObjectType:
					this->owner->CollapseGameObject(&collisionPoint, gravityObject1);
				break;

				case GameObject::Type::PortalType:
					this->owner->InitFinishPortal((SpaceObject*)gravityObject2->GetOwner());
					this->owner->ChangeGameStatus(Game::Status::FlyToPortalStatus);
				break;

				default: assert(0);
			}
		break;

		case GameObject::Type::SpaceObjectType:
			switch ((GameObject::Type) gravityObject2->GetType())
			{
				case GameObject::Type::ShipType:
					this->owner->CollapseGameObject(&collisionPoint, gravityObject2);
				break;

				case GameObject::Type::PortalType:
					this->owner->CollapseGameObject(&collisionPoint, gravityObject2);
				break;

				case GameObject::Type::SpaceObjectType:
					switch (((SpaceObject*)gravityObject1->GetOwner())->GetType())
					{
						case SpaceObject::Type::Planet:
							switch (((SpaceObject*)gravityObject2->GetOwner())->GetType())
							{
								case SpaceObject::Type::Planet:
									this->owner->CollapseGameObjects(&collisionPoint, gravityObject1, gravityObject2);
								break;

								case SpaceObject::Type::Star:
									this->owner->CollapseGameObject(&collisionPoint, gravityObject1);
								break;

								default: assert(0);
							}
						break;

						case SpaceObject::Type::Star:
							switch (((SpaceObject*)gravityObject2->GetOwner())->GetType())
							{
								case SpaceObject::Type::Planet:
									this->owner->CollapseGameObject(&collisionPoint, gravityObject2);
								break;

								case SpaceObject::Type::Star:
									this->owner->CollapseGameObjects(&collisionPoint, gravityObject1, gravityObject2);
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
					this->owner->InitFinishPortal((SpaceObject*)gravityObject1->GetOwner());
					this->owner->ChangeGameStatus(Game::Status::FlyToPortalStatus);
				break;

				case GameObject::Type::PortalType:
					this->owner->CollapseGameObjects(&collisionPoint, gravityObject1, gravityObject2);
				break;

				default:
					this->owner->CollapseGameObject(&collisionPoint, gravityObject1);
			}
		break;

		default: assert(0);
	}
}


// Game

Game::Game()
: window(sf::VideoMode(1920, 1080), "Hello :)", sf::Style::Fullscreen)
, camera(this, Vector2f(0.0f, 0.0f), Vector2f(0.0f, 0.0f), 1000.0f, 1000.0f, 0.0f, 0.0f)
, background(this)
, sys(&this->collisionHandler)
, collisionHandler(this)
, sfGUI()
, mainMenu(this)
, levelsMenu(this)
, pauseMenu(this)
, winMenu(this)
, loseMenu(this)
, gameInterface(this)
{
	this->window.resetGLStates();
	this->gameStatusTimer = ::gameStatusTimer;
	this->currentLevel = 0;
	this->gameStatus = Status::MainMenuStatus;
	this->ship = NULL;
	this->finishPortal = NULL;

	ResetControl();
	this->isShowTrajectory = false;
}


Game::~Game()
{
	this->ship = NULL;
	const int gameObjCount = this->gameObjects.size();
	for (int i = 0; i < gameObjCount; i++)
	{
		assert(this->gameObjects[i]);
		delete this->gameObjects[i];
	}
}


void Game::Run()
{
	sf::Clock clock;
	sf::Time timeSinceLastUpdate = sf::Time::Zero;

	while (window.isOpen())
	{
		timeSinceLastUpdate += clock.restart();
		while (timeSinceLastUpdate > timePerFrame)
		{
			timeSinceLastUpdate -= timePerFrame;
			//gravitySystem->SetTimeScale(timeScale);
			ProcessEvents();
			Update(timePerFrame.asSeconds());
		}

		Render();
	}

	//sf::Time time(sf::seconds(0.1f));
	//sf::sleep(time);
}


void Game::ProcessEvents()
{
	sf::Event evt;
	while (this->window.pollEvent(evt))
	{
		if (evt.type == sf::Event::Closed || this->gameStatus == Status::ExitStatus)
		{
			this->window.close();
			break;
		}

		if (this->gameStatus == Status::MainMenuStatus)
		{
			this->mainMenu.GetGuiWindow()->HandleEvent(evt);
		}
		if (this->gameStatus == Status::LevelsMenuStatus)
		{
			this->levelsMenu.GetGuiWindow()->HandleEvent(evt);
		}
		if (this->gameStatus == Status::PauseMenuStatus)
		{
			this->pauseMenu.GetGuiWindow()->HandleEvent(evt);
		}
		if (this->gameStatus == Status::WinMenuStatus)
		{
			this->winMenu.GetGuiWindow()->HandleEvent(evt);
		}
		if (this->gameStatus == Status::LoseMenuStatus)
		{
			this->loseMenu.GetGuiWindow()->HandleEvent(evt);
		}

		if (this->gameStatus == Status::ActionStatus)
		{
			switch (evt.type)
			{
				case sf::Event::KeyPressed:
					HandleInput(evt.key.code, true);
				break;

				case sf::Event::KeyReleased:
					HandleInput(evt.key.code, false);
				break;

				default:;
			}
		}
	}
}


void Game::Update(const float dt)
{
	RemoveGameObjects();
	UpdateGameStatus(dt);
	UpdateMenu(dt);

	if (this->gameStatus == Status::ActionStatus || this->gameStatus == Status::ExplosionStatus || this->gameStatus == Status::FlyToPortalStatus)
	{
		if (this->gameStatus == Status::ActionStatus)
		{
			this->sys.CalculateAccelereations();

			if (this->isMovingLeft && this->ship != NULL)
			{
				this->ship->Rotate(dAngle);
			}
			if (this->isMovingRight && this->ship != NULL)
			{
				this->ship->Rotate(-dAngle);
			}
			if (this->ship != NULL)
			{
				this->ship->SwitchEngine(this->isMovingUp, this->isMovingDown);
			}
		}

		const int gameObjCount = this->gameObjects.size();
		for (int i = 0; i < gameObjCount; i++)
		{
			this->gameObjects[i]->Update(dt);
		}

		if (this->gameStatus == Status::ActionStatus)
		{
			this->camera.Update(dt);
			this->background.Update(dt);
			this->gameInterface.Update(dt);
			this->sys.IntegrateSystem(dt);
		}
	}
}

void Game::UpdateGameStatus(const float dt)
{
	assert(dt >= 0.0f);
	switch (this->gameStatus)
	{
		case Status::ExplosionStatus:
			this->gameStatusTimer -= dt;
			if (this->gameStatusTimer <= 0.0f)
			{
				ChangeGameStatus(Status::LoseMenuStatus);
				this->gameStatusTimer = ::gameStatusTimer;
			}
		break;

		case Status::FlyToPortalStatus:
			this->gameStatusTimer -= dt;
			if (this->gameStatusTimer <= 0.0f)
			{
				ChangeGameStatus(Status::WinMenuStatus);
				this->gameStatusTimer = ::gameStatusTimer;
			}
		break;

		default:;
	}
}

void Game::UpdateMenu(const float dt)
{
	assert(dt >= 0.0f);

	switch (this->gameStatus)
	{
		case Status::MainMenuStatus:
			this->mainMenu.Update(dt);
		break;

		case Status::LevelsMenuStatus:
			this->levelsMenu.Update(dt);
		break;

		case Status::PauseMenuStatus:
			this->pauseMenu.Update(dt);
		break;

		case Status::WinMenuStatus:
			this->winMenu.Update(dt);
		break;

		case Status::LoseMenuStatus:
			this->loseMenu.Update(dt);
		break;

		default:;
	}
}


void Game::Render()
{
	this->window.clear();
	this->background.Draw();

	if (this->gameStatus == Status::ActionStatus || this->gameStatus == Status::ExplosionStatus || this->gameStatus == Status::FlyToPortalStatus || this->gameStatus == Status::PauseMenuStatus)
	{
		const int gameObjCount = this->gameObjects.size();
		for (int i = 0; i < gameObjCount; i++)
		{
			this->gameObjects[i]->Draw();
		}
		this->gameInterface.Draw();
	}

	if (this->gameStatus == Status::MainMenuStatus)
	{
		this->mainMenu.Render();
	}
	if (this->gameStatus == Status::LevelsMenuStatus)
	{
		this->levelsMenu.Render();
	}
	if (this->gameStatus == Status::PauseMenuStatus)
	{
		this->pauseMenu.Render();
	}
	if (this->gameStatus == Status::WinMenuStatus)
	{
		this->winMenu.Render();
	}
	if (this->gameStatus == Status::LoseMenuStatus)
	{
		this->loseMenu.Render();
	}

	this->window.display();
}



void Game::HandleInput(sf::Keyboard::Key key, bool isPressed)
{
	switch (key)
	{
		case sf::Keyboard::Up:
			this->isMovingUp = isPressed;
		break;

		/*case sf::Keyboard::Down:
			this->isMovingDown = isPressed;
		break;*/

		case sf::Keyboard::Left:
			this->isMovingLeft = isPressed;
		break;

		case sf::Keyboard::Right:
			this->isMovingRight = isPressed;
		break;

		case sf::Keyboard::W:
			this->isScalingCameraUp = isPressed;
		break;

		case sf::Keyboard::S:
			this->isScalingCameraDown = isPressed;
		break;

		case sf::Keyboard::T:
			if (isPressed)
			{
				this->isShowTrajectory = this->isShowTrajectory ^ true;
			}
		break;

		case sf::Keyboard::Escape:
			this->gameStatus = Status::PauseMenuStatus;
		break;

		default:;
	}
}


void Game::Reset()
{
	SwitchOffAllGameObjects();
	RemoveGameObjects();

	this->gameStatusTimer = ::gameStatusTimer;
	this->ship = NULL;
	this->finishPortal = NULL;
	this->camera.SetDstFov(defFov);

	this->isShowTrajectory = false;
	ResetControl();
}

void Game::ResetControl()
{
	this->isMovingUp = false;
	this->isMovingDown = false;
	this->isMovingLeft = false;
	this->isMovingRight = false;
	this->isScalingCameraUp = false;
	this->isScalingCameraDown = false;
}


void Game::AddGameObject(GameObject* gameObject)
{
	assert(gameObject);
	this->gameObjects.push_back(gameObject);
}

void Game::SwitchOffGameObject(GameObject* gameObject)
{
	assert(gameObject);
	gameObject->SwitchOff();
}

void Game::SwitchOffAllGameObjects()
{
	const int gameObjCount = this->gameObjects.size();
	for (int i = 0; i < gameObjCount; i++)
	{
		assert(this->gameObjects[i]);
		this->gameObjects[i]->SwitchOff();
	}
}

void Game::RemoveGameObjects()
{
	unsigned int i = 0;
	while (i < this->gameObjects.size())
	{
		if (this->gameObjects[i]->IsExist())
		{
			i++;
		}
		else
		{
			this->gameObjects[i]->Drop();
			GameObject* auxPtr = this->gameObjects[this->gameObjects.size() - 1];
			delete this->gameObjects[i];
			this->gameObjects[i] = auxPtr;
			this->gameObjects.pop_back();
		}
	}
}

void Game::CollapseGameObjects(Vector2f* collisionPoint, GravityObject* gravityObject1, GravityObject* gravityObject2)
{
	assert(collisionPoint);
	assert(gravityObject1 && gravityObject2);
	AddGameObject(new Explosion(this, *collisionPoint, std::max(gravityObject1->GetRadius(), gravityObject2->GetRadius())));
	SwitchOffGameObject((GameObject*)gravityObject1->GetOwner());
	SwitchOffGameObject((GameObject*)gravityObject2->GetOwner());

	if (gravityObject1->GetType() == (int)GameObject::Type::ShipType || gravityObject2->GetType() == (int)GameObject::Type::ShipType)
	{
		this->gameStatus = Status::ExplosionStatus;
	}
}

void Game::CollapseGameObject(Vector2f* collisionPoint, GravityObject* gravityObject)
{
	assert(collisionPoint);
	assert(gravityObject);
	AddGameObject(new Explosion(this, *collisionPoint, gravityObject->GetRadius()));
	SwitchOffGameObject((GameObject*)gravityObject->GetOwner());

	if (gravityObject->GetType() == (int)GameObject::Type::ShipType)
	{
		this->gameStatus = Status::ExplosionStatus;
	}
}

void Game::AddShip(Ship* ship)
{
	assert(ship);
	this->ship = ship;
}

void Game::RemoveShip()
{
	this->ship = NULL;
}

void Game::ChangeLevelNumber(int levelNumber)
{
	assert(levelNumber >= 0);
	this->currentLevel = levelNumber;
}

int Game::GetLevelNumber()
{
	return this->currentLevel;
}

void Game::ChangeGameStatus(Status newStatus)
{
	assert(0 <= newStatus && newStatus <= Status::ExplosionStatus);

	if (newStatus == Status::MainMenuStatus || newStatus == Status::ExitStatus || newStatus == Status::WinMenuStatus || newStatus == Status::LoseMenuStatus)
	{
		Reset();
		this->gameInterface.GetGuiWindow()->Show(false);
	}
	if (newStatus == Status::ActionStatus || newStatus == Status::ExplosionStatus || newStatus == Status::FlyToPortalStatus)
	{
		ResetControl();
		if (this->gameStatus != Status::PauseMenuStatus)
		{
			this->isShowTrajectory = false;
		}
	}
	if (newStatus == Status::FlyToPortalStatus)
	{
		assert(this->ship);
		this->ship->CatchByPortal(this->finishPortal->GetPos(), gameStatusTimer, timePerFrame.asSeconds());
	}

	this->gameStatus = newStatus;
}

void Game::InitFinishPortal(SpaceObject* finishPortal)
{
	assert(finishPortal);
	this->finishPortal = finishPortal;
}

bool Game::IsShowTrajectory()
{
	return this->isShowTrajectory;
}

bool Game::IsScalingCameraUp()
{
	return this->isScalingCameraUp;
}

bool Game::IsScalingCameraDown()
{
	return this->isScalingCameraDown;
}

float Game::GetGameStatusTimer()
{
	return this->gameStatusTimer;
}

float Game::GetFuelFraction()
{
	if (this->ship)
	{
		return this->ship->GetFuelFraction();
	}
	else
	{
		return 0.0f;
	}
}

Game::Status Game::GetGameStatus()
{
	return this->gameStatus;
}

sf::RenderWindow* Game::GetWindow()
{
	return &(this->window);
}

Camera* Game::GetCamera()
{
	return &(this->camera);
}

LevelsMenu* Game::GetLevelsMenu()
{
	return &(this->levelsMenu);
}

sfg::SFGUI* Game::GetSFGUI()
{
	return &(this->sfGUI);
}

GravityObjectSystem* Game::GetGravitySystem()
{
	return &(this->sys);
}

//Game

