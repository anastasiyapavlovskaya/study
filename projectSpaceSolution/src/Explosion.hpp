#include "Game.hpp"
#include "Vector2f.hpp"
#include "Sprite.hpp"


class Explosion : public GameObject
{
public:
	Explosion(Game* owner, Vector2f pos, float explosionSize);
	virtual ~Explosion();

	virtual void Update(const float dt);
	virtual void Draw();
	virtual void SwitchOff();
	virtual void Drop();
	virtual bool IsExist();

private:
	Game* owner;
	Vector2f pos;
	float explosionSize;
	float elapsedTime;
	std::vector <Sprite> sprite;
};