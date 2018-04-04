#pragma once
#include "Vector2f.hpp"
#include "Sprite.hpp"
#include <SFGUI/SFGUI.hpp>
#include <SFGUI/Widgets.hpp>
#include <SFGUI/Renderers.hpp>


class Game;

class GameInterface
{
public:
	GameInterface(Game* owner);
	void Draw();
	void Update(float dt);
	void SetProperties();

	sfg::Window::Ptr GetGuiWindow();
	
private:
	Game* owner;
	sfg::Window::Ptr guiWindow;
	sfg::ProgressBar::Ptr progressBar;
	sf::Image fuel;
	sf::Text timeSpeed;
	
	float currTimeSpeed;
};