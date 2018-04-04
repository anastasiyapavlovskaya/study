#pragma once
#include "Effect.hpp"
#include "Utils.hpp"

struct BlackHoleEffect : public Effect
{
  BlackHoleEffect();
  virtual void Render(sf::RenderWindow *window, Camera* camera, float ang, Vector2f pos, Vector2f size);
  virtual void Affect();
  virtual void Reset();
  virtual void Update(float dt);
private:
  sf::Shader shader;
  sf::Texture backgroundTex;
  sf::Texture fireballTex;
  float isExploding;
  float currTime;
  float startTime;
};