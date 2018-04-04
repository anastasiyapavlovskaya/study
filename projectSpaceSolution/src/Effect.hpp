#pragma once
#include <SFML/Graphics.hpp>
#include "Vector2f.hpp"
#include "Camera.hpp"

struct Effect
{
  virtual void Render(sf::RenderWindow *window, Camera* camera, float ang, Vector2f pos, Vector2f size) = 0;
  virtual void Affect(){};
  virtual void Update(float dt) = 0;
  virtual void Reset(){};
};

