#pragma once
#include <SFML/Graphics.hpp> 
#include "Sprite.hpp"
//#include "Game.hpp"

class Game;

class Background
{
public:
	Background(Game* owner);
	void Update(float dt);
	void Draw();

private:
	struct Layer{
		Sprite sprite;
		float offset;
		float worldSize;
		Vector2f texCoords[4];
	};
	std::vector<Layer> layers;
	Sprite menuBackground;
	Game* owner;

};
