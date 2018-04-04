#pragma once
#include"Camera.hpp"
#include <SFML/Graphics.hpp>

void RenderQuad(sf::RenderWindow *window, sf::Vector2f center, float ang, sf::Vector2f size, sf::Shader *shader, Camera* camera);