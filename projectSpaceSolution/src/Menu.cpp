#include <SFML/Graphics.hpp>
#include <SFGUI/SFGUI.hpp>
#include <SFGUI/Widgets.hpp>
#include <SFGUI/Renderers.hpp>
#include <assert.h>
#include <fstream>
#include <cstring>
#include <json/json.h>
#include "Menu.hpp"
#include "Game.hpp"
#include "SpaceObject.hpp"
#include "Ship.hpp"
#include "Trajectory.hpp"


// Menu

Menu::Menu(Game* owner)
{
	this->owner = owner;
	this->guiWindow = sfg::Window::Create();
}

Menu::~Menu()
{
	// nothing
}

sfg::Window::Ptr Menu::GetGuiWindow()
{
	return this->guiWindow;
}

void Menu::Update(const float dt)
{
	this->guiWindow->Update(dt);
}

void Menu::Render()
{
	this->guiWindow->Show(true);
	this->owner->GetSFGUI()->Display(*this->owner->GetWindow());
}
// Menu


// MainMenu

MainMenu::MainMenu(Game* owner) : Menu(owner)
{
	assert(owner);
	assert(this->guiWindow);
	this->guiWindow->SetStyle('b');
	SetProperties();

	sfg::Button::Ptr buttonPlay = sfg::Button::Create("Play");
	sfg::Button::Ptr buttonExit = sfg::Button::Create("Exit");

	sfg::Label::Ptr title = sfg::Label::Create();
	title->SetText("MAIN MENU");
	
	buttonPlay->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&MainMenu::Play, this));
	buttonExit->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&MainMenu::Exit, this));
	
	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::VERTICAL, 10.0f);
	box->Pack(title, false, false);
	box->Pack(title, false, false);
	box->Pack(buttonPlay, false, false);
	box->Pack(buttonExit, false, false);

	this->guiWindow->Add(box);
	
	sf::Vector2f menuSize = sf::Vector2f(box->GetAllocation().width, box->GetAllocation().height);
	this->guiWindow->SetPosition((const sf::Vector2f)this->owner->GetWindow()->getSize() * 0.5f - menuSize * 0.5f);
}

MainMenu::~MainMenu()
{
	//nothing have to do
}


void MainMenu::SetProperties()
{
	sfg::Context::Get().GetEngine().SetProperty("Window", "BackgroundColor", sf::Color(12, 43, 54));
	sfg::Context::Get().GetEngine().SetProperty("Window", "BorderWidth", 0.0f);

	sfg::Context::Get().GetEngine().SetProperty("Button", "BackgroundColor", sf::Color(52, 73, 94));
	sfg::Context::Get().GetEngine().SetProperty("Button", "BorderWidth", 0.0f);
	sfg::Context::Get().GetEngine().SetProperty("Button", "FontName", "Data/Lato-Medium.ttf");
	sfg::Context::Get().GetEngine().SetProperty("Button", "FontSize", 20.0f);
	sfg::Context::Get().GetEngine().SetProperty("Button:PRELIGHT", "BackgroundColor", sf::Color(22, 160, 133));
	sfg::Context::Get().GetEngine().SetProperty("Button:ACTIVE", "BackgroundColor", sf::Color(22, 160, 133));

	sfg::Context::Get().GetEngine().SetProperty("Label", "FontName", "Data/Lato-Medium.ttf");
	sfg::Context::Get().GetEngine().SetProperty("Label", "FontSize", 40.0f);
}

void MainMenu::Play()
{
	this->owner->ChangeGameStatus(Game::Status::LevelsMenuStatus);
	this->guiWindow->Show(false);
}

void MainMenu::Exit()
{
	this->owner->ChangeGameStatus(Game::Status::ExitStatus);
	this->guiWindow->Show(false);
}

// MainMenu



// LevelsMenu

LevelsMenu::LevelsMenu(Game* owner) : Menu(owner)
{
	assert(owner);
	assert(this->guiWindow);
	this->guiWindow->SetStyle('b');

	sfg::Label::Ptr title = sfg::Label::Create();
	title->SetText("CHOOSE LEVEL");

	sfg::Button::Ptr buttonLevel1 = sfg::Button::Create("Level 1");
	sfg::Button::Ptr buttonLevel2 = sfg::Button::Create("Level 2");
	sfg::Button::Ptr buttonLevel3 = sfg::Button::Create("Level 3");
	sfg::Button::Ptr buttonBack = sfg::Button::Create("Back");

	buttonLevel1->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LevelsMenu::CreateLevel1, this));
	buttonLevel2->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LevelsMenu::CreateLevel2, this));
	buttonLevel3->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LevelsMenu::CreateLevel3, this));
	buttonBack->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LevelsMenu::BackToMainMenu, this));

	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::VERTICAL, 10.0f);
	box->Pack(title, false, false);
	box->Pack(buttonLevel1, true, true);
	box->Pack(buttonLevel2, true, true);
	box->Pack(buttonLevel3, true, true);
	box->Pack(buttonBack, true, true);

	this->guiWindow->Add(box);

	sf::Vector2f menuSize = sf::Vector2f(box->GetAllocation().width, box->GetAllocation().height);
	this->guiWindow->SetPosition((const sf::Vector2f)this->owner->GetWindow()->getSize() * 0.5f - menuSize * 0.5f);
}

LevelsMenu::~LevelsMenu()
{
	//nothing have to do
}

void LevelsMenu::CreateLevel1()
{
	CreateLevel(1);
}

void LevelsMenu::CreateLevel2()
{
	CreateLevel(2);
}

void LevelsMenu::CreateLevel3()
{
	CreateLevel(3);
}

void LevelsMenu::CreateLevel(int levelNumber)
{
	this->owner->ChangeGameStatus(Game::Status::ActionStatus);
	this->owner->AddGameObject(new TrajectoryComputer(this->owner));
	
	std::ifstream levelFile("Data/levels/level" + std::to_string(levelNumber) + ".json");
	if (!levelFile.is_open())
	{
		std::cout << "Cannot open file " << "Data/levels/level" + std::to_string(levelNumber) + ".json" << std::endl;
	}

	Json::Reader jsonReader;
	Json::Value root;
	if (!jsonReader.parse(levelFile, root))
	{
		std::cout << "Parsing error: " << jsonReader.getFormattedErrorMessages() << std::endl;
	}

	const int objectsCount = root["objects"].size();
	for (int i = 0; i < objectsCount; i++)
	{
		Json::Value item = root["objects"][i];
		Vector2f pos(item["pos"][0].asFloat(), item["pos"][1].asFloat());
		Vector2f prevPos(item["prevPos"][0].asFloat(), item["prevPos"][1].asFloat());
		Vector2f acceleration(item["acceleration"][0].asFloat(), item["acceleration"][1].asFloat());
		float mass = item["mass"].asFloat();
		float radius = item["radius"].asFloat();
		std::string path(item["path"].asCString());

		if (!strcmp(item["type"].asCString(), "ship"))
		{
			this->owner->AddGameObject(new Ship(this->owner, item["angle"].asFloat(), item["fuel"].asFloat(), pos, prevPos, acceleration, mass, radius, path));
		}
		if (!strcmp(item["type"].asCString(), "star"))
		{
			this->owner->AddGameObject(new SpaceObject(this->owner, SpaceObject::Star, pos, prevPos, acceleration, mass, radius, path));
		}
		if (!strcmp(item["type"].asCString(), "planet"))
		{
			this->owner->AddGameObject(new SpaceObject(this->owner, SpaceObject::Planet, pos, prevPos, acceleration, mass, radius, path));
		}
		if (!strcmp(item["type"].asCString(), "portal"))
		{
			this->owner->AddGameObject(new SpaceObject(this->owner, SpaceObject::Portal, pos, prevPos, acceleration, mass, radius, path));
		}
	}

	levelFile.close();
	
	//this->owner->AddGameObject(new TrajectoryComputer(this->owner));
	//this->owner->AddGameObject(new SpaceObject(this->owner, SpaceObject::Portal, Vector2f(600.1f, 400.1f), Vector2f(600.5f, 400.5f), Vector2f(0.0f, 0.0f), 1.0f, 20.0f, "Data/sun.png"));
	//this->owner->AddGameObject(new Ship(this->owner, 0.0f, 100.0f, Vector2f(250.0f, 150.0f), Vector2f(250.0f, 150.0f), Vector2f(0.0f, 0.0f), 1.0f, 5.0f, "Data/player1.png"));

	this->owner->ChangeLevelNumber(levelNumber);
	this->guiWindow->Show(false);
}


void LevelsMenu::BackToMainMenu()
{
	this->owner->ChangeGameStatus(Game::Status::MainMenuStatus);
	this->guiWindow->Show(false);
}

// LevelsMenu


 
// PauseMenu

PauseMenu::PauseMenu(Game* owner) : Menu(owner)
{
	assert(owner);
	assert(this->guiWindow);
	this->guiWindow->SetStyle('b');

	sfg::Label::Ptr title = sfg::Label::Create();
	title->SetText("PAUSE");

	sfg::Button::Ptr buttonResume = sfg::Button::Create("Resume");
	sfg::Button::Ptr buttonRestartLevel = sfg::Button::Create("Restart level");
	sfg::Button::Ptr buttonMainMenu = sfg::Button::Create("Main menu");

	buttonResume->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&PauseMenu::Resume, this));
	buttonRestartLevel->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&PauseMenu::RestartLevel, this));
	buttonMainMenu->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&PauseMenu::BackToMainMenu, this));

	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::VERTICAL, 10.0f);
	box->Pack(title, false, false);
	box->Pack(buttonResume, true, true);
	box->Pack(buttonRestartLevel, true, true);
	box->Pack(buttonMainMenu, true, true);

	this->guiWindow->Add(box);

	sf::Vector2f menuSize = sf::Vector2f(box->GetAllocation().width, box->GetAllocation().height);
	this->guiWindow->SetPosition((const sf::Vector2f)this->owner->GetWindow()->getSize() * 0.5f - menuSize * 0.5f);
}

PauseMenu::~PauseMenu()
{
	//nothing have to do
}

void PauseMenu::Resume()
{
	this->owner->ChangeGameStatus(Game::Status::ActionStatus);
	this->guiWindow->Show(false);
}

void PauseMenu::RestartLevel()
{
	this->owner->Reset();
	this->owner->ChangeGameStatus(Game::Status::ActionStatus);
	this->owner->GetLevelsMenu()->CreateLevel(this->owner->GetLevelNumber());
	this->guiWindow->Show(false);
}

void PauseMenu::BackToMainMenu()
{
	this->owner->ChangeGameStatus(Game::Status::MainMenuStatus);
	this->owner->ChangeLevelNumber(0);
	this->guiWindow->Show(false);
}

// PauseMenu


// WinMenu

WinMenu::WinMenu(Game* owner) : Menu(owner)
{
	assert(owner);
	assert(this->guiWindow);
	this->guiWindow->SetStyle('b');
	sfg::Label::Ptr title = sfg::Label::Create();
	title->SetText("You win!");
	sfg::Context::Get().GetEngine().SetProperty("Label", "FontSize", 40.0f);
	
	sfg::Button::Ptr buttonMainMenu = sfg::Button::Create("Main menu");

	buttonMainMenu->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&WinMenu::BackToMainMenu, this));

	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::VERTICAL, 10.0f);
	box->Pack(title, false, false);
	box->Pack(buttonMainMenu, true, true);

	this->guiWindow->Add(box);

	sf::Vector2f menuSize = sf::Vector2f(box->GetAllocation().width, box->GetAllocation().height);
	this->guiWindow->SetPosition((const sf::Vector2f)this->owner->GetWindow()->getSize() * 0.5f - menuSize * 0.5f);
}

WinMenu::~WinMenu()
{
	// nothing
}

void WinMenu::BackToMainMenu()
{
	this->owner->ChangeGameStatus(Game::Status::MainMenuStatus);
	this->owner->ChangeLevelNumber(0);
	this->guiWindow->Show(false);
}

// WinMenu


// LoseMenu

LoseMenu::LoseMenu(Game* owner) : Menu(owner)
{
	assert(owner);
	assert(this->guiWindow);
	this->guiWindow->SetStyle('b');

	sfg::Label::Ptr title = sfg::Label::Create();
	title->SetText("You lose!");

	sfg::Button::Ptr buttonRestartLevel = sfg::Button::Create("Restart level");
	sfg::Button::Ptr buttonMainMenu = sfg::Button::Create("Main menu");

	buttonRestartLevel->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LoseMenu::RestartLevel, this));
	buttonMainMenu->GetSignal(sfg::Button::OnLeftClick).Connect(std::bind(&LoseMenu::BackToMainMenu, this));

	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::VERTICAL, 10.0f);
	box->Pack(title, false, false);
	box->Pack(buttonRestartLevel, true, true);
	box->Pack(buttonMainMenu, true, true);

	this->guiWindow->Add(box);

	sf::Vector2f menuSize = sf::Vector2f(box->GetAllocation().width, box->GetAllocation().height);
	this->guiWindow->SetPosition((const sf::Vector2f)this->owner->GetWindow()->getSize() * 0.5f - menuSize * 0.5f);
}

LoseMenu::~LoseMenu()
{
	// nothing
}

void LoseMenu::RestartLevel()
{
	this->owner->Reset();
	this->owner->ChangeGameStatus(Game::Status::ActionStatus);
	this->owner->GetLevelsMenu()->CreateLevel(this->owner->GetLevelNumber());
	this->guiWindow->Show(false);
}

void LoseMenu::BackToMainMenu()
{
	this->owner->ChangeGameStatus(Game::Status::MainMenuStatus);
	this->owner->ChangeLevelNumber(0);
	this->guiWindow->Show(false);
}

// LoseMenu