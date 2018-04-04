#pragma once
#include "Vector2f.hpp"
#include <SFML/Graphics.hpp>
#include <string>
#include <map>
#include "Camera.hpp"


class Sprite
{
public:
	Sprite(){}
	Sprite(std::string filename);
	void Draw(sf::RenderWindow *window, Vector2f pos, float ang, Vector2f size, Camera* camera);
	void DrawTrajectory(sf::RenderWindow *window, Vector2f firstPos, Vector2f secondPos, Camera* camera);
	void DrawBackground(sf::RenderWindow * window, Vector2f * texCoords);

	sf::Texture* GetTexture();
	void SetRepeating();
private:
	static std::map<std::string, sf::Texture*> textures;
	sf::Texture* tex;
};