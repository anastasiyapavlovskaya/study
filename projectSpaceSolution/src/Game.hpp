#pragma once
#include <SFML/Graphics.hpp>
#include <SFGUI/SFGUI.hpp>
#include <SFGUI/Widgets.hpp>
#include <SFGUI/Renderers.hpp>
#include <vector>
#include "GravityObject.hpp"
#include "Camera.hpp"
#include "Background.hpp"
#include "Menu.hpp"
#include "GameInterface.hpp"

class Ship;
class SpaceObject;
class Game;
class TrajectoryComputer;



class GameObject
{
public:
	enum Type
	{
		SpaceObjectType = 0,
		ShipType = 1,
		PortalType = 2,
		ExplosionType = 3
	};

	virtual void Update(const float dt) = 0;
	virtual void Draw() = 0;
	virtual void SwitchOff() = 0;
	virtual void Drop() = 0;
	virtual bool IsExist() = 0;
	virtual ~GameObject();
};


class Functor
{
public:
	virtual void OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2);
};

class CollisionHandler : public Functor
{
public:
	CollisionHandler(Game* owner);
	virtual void OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2);

private:
	Game* owner;
};

class GravityCollisionHandler : public Functor
{
public:
	GravityCollisionHandler(TrajectoryComputer* owner);
	virtual void OnCollision(GravityObject* gravityObject1, GravityObject* gravityObject2);

private:
	TrajectoryComputer* owner;
};




class Game
{
public:
	enum Status
	{
		ExitStatus        = 0,
		MainMenuStatus    = 1,
		LevelsMenuStatus  = 2,
		PauseMenuStatus   = 3,
		WinMenuStatus     = 4,
		LoseMenuStatus    = 5,
		ActionStatus      = 6,
		FlyToPortalStatus = 7,
		ExplosionStatus   = 8,
	};

	Game();
	~Game();
	void Run();
	void ProcessEvents();
	void Update(const float dt);
	void UpdateGameStatus(const float dt);
	void UpdateMenu(const float dt);
	void Render();
	void HandleInput(sf::Keyboard::Key key, bool isPressed);

	void Reset();
	void ResetControl();
	void AddGameObject(GameObject* gameObject);
	void SwitchOffGameObject(GameObject* gameObject);
	void SwitchOffAllGameObjects();
	void RemoveGameObjects();
	void CollapseGameObjects(Vector2f* collisionPoint, GravityObject* gravityObject1, GravityObject* gravityObject2);
	void CollapseGameObject(Vector2f* collisionPoint, GravityObject* gravityObject);
	void AddShip(Ship* ship);
	void RemoveShip();
	void InitFinishPortal(SpaceObject* finishPortal);
	void ChangeGameStatus(Status newStatus);
	void ChangeLevelNumber(int levelNumber);
	float GetGameStatusTimer();
	float GetFuelFraction();
	int GetLevelNumber();
	Status GetGameStatus();

	bool IsShowTrajectory();
	bool IsScalingCameraUp();
	bool IsScalingCameraDown();
	GravityObjectSystem* GetGravitySystem();
	sf::RenderWindow* GetWindow();
	Camera* GetCamera();
	LevelsMenu* GetLevelsMenu();
	sfg::SFGUI* GetSFGUI();

private:
	sf::RenderWindow window;
	Camera camera;
	Background background;

	std::vector <GameObject*> gameObjects;
	GravityObjectSystem sys;
	Ship* ship;
	SpaceObject* finishPortal;
	CollisionHandler collisionHandler;

	float gameStatusTimer;
	int currentLevel; // if there is no active levels, this variable will be 0
	Status gameStatus;
	sfg::SFGUI sfGUI;
	MainMenu mainMenu;
	LevelsMenu levelsMenu;
	PauseMenu pauseMenu;
	WinMenu winMenu;
	LoseMenu loseMenu;
	GameInterface gameInterface;

	bool isMovingUp;
	bool isMovingDown;
	bool isMovingLeft;
	bool isMovingRight;
	bool isShowTrajectory;
	bool isScalingCameraUp;
	bool isScalingCameraDown;
};



