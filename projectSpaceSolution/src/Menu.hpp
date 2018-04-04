#pragma once
#include <SFGUI/SFGUI.hpp>
#include <SFGUI/Widgets.hpp>
#include <SFGUI/Renderers.hpp>

class Game;


class Menu
{
public:
	Menu(Game* owner);
	virtual ~Menu();

	sfg::Window::Ptr GetGuiWindow();
	void Update(const float dt);
	void Render();

protected:
	Game* owner;
	sfg::Window::Ptr guiWindow;
};


class MainMenu : public Menu
{
public:
	MainMenu(Game* owner);
	virtual ~MainMenu();
	
	void SetProperties();
	void Play();
	void Exit();
};


class LevelsMenu : public Menu
{
public:
	LevelsMenu(Game* owner);
	virtual ~LevelsMenu();

	void CreateLevel(int levelNumber);
	void CreateLevel1();
	void CreateLevel2();
	void CreateLevel3();
	void BackToMainMenu();
};


class PauseMenu : public Menu
{
public:
	PauseMenu(Game* owner);
	virtual ~PauseMenu();

	void Resume();
	void RestartLevel();
	void BackToMainMenu();
};


class WinMenu : public Menu
{
public:
	WinMenu(Game* owner);
	virtual ~WinMenu();

	void BackToMainMenu();
};


class LoseMenu : public Menu
{
public:
	LoseMenu(Game* owner);
	virtual ~LoseMenu();

	void RestartLevel();
	void BackToMainMenu();
};