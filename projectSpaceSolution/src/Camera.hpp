#pragma once
#include "Vector2f.hpp"
#include "Coords.hpp"
#include <SFML/Graphics.hpp>


class Game;

class Camera
{
public:
	Camera();
	Camera(Game* owner, Vector2f currPos, Vector2f dstPos, float currFov, float dstFov, float currAng, float dstAng);

	void Update(float dt);
	void InputHandler();

	Vector2f ConvertPosToLocal(Vector2f pos);
	Vector2f ConvertVectorToLocal(Vector2f vector);

	void SetDstCoords(Coords2f newCoords);
	void SetDstPos(Vector2f newPos);
	void SetDstFov(float newFov);
	float GetCurrFov();
	Vector2f GetCurrPos();
	Coords2f GetDstCoords();
	Coords2f GetCurrCoords();

private:
	Game* owner;
	Coords2f currCoords;
	float currFov;
	Coords2f dstCoords;
	float dstFov;
};
