#include "Game.hpp"
#include "Sprite.hpp"
#include <map>


class TrajectoryComputer : public GameObject
{
public:
	enum Color
	{
		White = 0,
		Red = 1,
		Green = 2
	};

	TrajectoryComputer(Game* owner);
	virtual ~TrajectoryComputer();
	void Clone();
	void CalculateTrajectories();
	void InitializeColors();
	void SetGreenIndex(GravityObject* gravityObject);
	GravityObjectSystem* GetGravitySystem();

	virtual void Update(const float dt);
	virtual void Draw();
	virtual void SwitchOff();
	virtual void Drop();
	virtual bool IsExist();

private:
	Game* owner;
	GravityObjectSystem computeSys;
	bool isExist;

	GravityCollisionHandler gravityCollisionHandler;
	float dt;

	std::vector <Color> colors;
	std::vector <Sprite> sprites;
	int greenIndex;  // it is index of the ship if collision with portal will occure
};
