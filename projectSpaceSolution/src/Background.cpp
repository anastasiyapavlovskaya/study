#include "Background.hpp"
#include "Camera.hpp"
#include "Game.hpp"


Background::Background(Game* owner)
{
	this->owner = owner;

	Layer newLayer;

	newLayer.sprite = Sprite("Data/background/background.png");
	newLayer.offset = 0.2f;
	newLayer.worldSize = 2048.0f;
	layers.push_back(newLayer);
	
	newLayer.sprite = Sprite("Data/background/stars4.png");
	newLayer.offset = 0.4f;
	newLayer.worldSize = 2048.0f;
	layers.push_back(newLayer);
	
	newLayer.sprite = Sprite("Data/background/stars3.png");
	newLayer.offset = 0.6f;
	newLayer.worldSize = 2048.0f;
	layers.push_back(newLayer);

	newLayer.sprite = Sprite("Data/background/stars2.png");
	newLayer.offset = 0.8f;
	newLayer.worldSize = 2048.0f;
	layers.push_back(newLayer);

	newLayer.sprite = Sprite("Data/background/stars1.png");
	newLayer.offset = 1.0f;
	newLayer.worldSize = 2048.0f;
	layers.push_back(newLayer);
	
	for (size_t i = 0; i < layers.size(); i++)
	{
		this->layers[i].sprite.SetRepeating();
	}

	this->menuBackground = Sprite("Data/background/menuBackground.png");
}

void Background::Update(float dt)
{
	Camera * camera = owner->GetCamera();
	Vector2f cameraFov;
	cameraFov.x = camera->GetCurrFov();
	cameraFov.y = cameraFov.x * ((float)owner->GetWindow()->getSize().y / owner->GetWindow()->getSize().x);

	Vector2f dir = camera->GetDstCoords().GetPos() - camera->GetCurrCoords().GetPos();
	float alpha = 10.0f;

	for (size_t i = 0; i < layers.size(); i++)
	{
		Vector2f worldSize;
		worldSize.x = layers[i].worldSize;
		worldSize.y = worldSize.x * ((float)owner->GetWindow()->getSize().y / owner->GetWindow()->getSize().x);

		Vector2f texSize = Vector2f(0.0f, 0.0f);
		texSize.x = (float)layers[i].sprite.GetTexture()->getSize().x;
		texSize.y = texSize.x * ((float)owner->GetWindow()->getSize().y / owner->GetWindow()->getSize().x);

		Vector2f center;
		center.x = camera->GetCurrCoords().GetPos().x * layers[i].offset;
		center.y = camera->GetCurrCoords().GetPos().y * layers[i].offset;

//		std::cout << "backgroundCenter = " << center.x << ";" << center.y << std::endl;
		Vector2f xVector = camera->GetCurrCoords().GetXVector();
		Vector2f yVector = xVector.GetPerpendicular();

		texSize.x = texSize.x * 1.0f / (1.0f / (cameraFov.x / worldSize.x) + 1.0f / layers[i].offset) * 5;
		texSize.y = texSize.y * 1.0f / (1.0f / (cameraFov.y / worldSize.y) + 1.0f / layers[i].offset) * 5;

		layers[i].texCoords[0] = center - xVector * texSize.x * 0.5f - yVector * texSize.y * 0.5f;
		layers[i].texCoords[1] = center + xVector * texSize.x * 0.5f - yVector * texSize.y * 0.5f;
		layers[i].texCoords[2] = center + xVector * texSize.x * 0.5f + yVector * texSize.y * 0.5f;
		layers[i].texCoords[3] = center - xVector * texSize.x * 0.5f + yVector * texSize.y * 0.5f;
	
	/*
		layers[i].texCoords[0] = Vector2f(0.0f, 0.0f);
		layers[i].texCoords[1] = Vector2f(10000.0f, 0.0f);
		layers[i].texCoords[2] = Vector2f(10000.0f, 10000.0f);
		layers[i].texCoords[3] = Vector2f(0.0f, 10000.0f);
	*/

	}
}

void Background::Draw()
{
	if (this->owner->GetGameStatus() == this->owner->MainMenuStatus || this->owner->GetGameStatus() == this->owner->LevelsMenuStatus)
	{
		float xTexSize = (float)menuBackground.GetTexture()->getSize().x;
		float yTexSize = (float)menuBackground.GetTexture()->getSize().y;

		Vector2f texCoords[4];
		texCoords[0] = Vector2f(0.0f, 0.0f);
		texCoords[1] = Vector2f(xTexSize, 0.0f);
		texCoords[2] = Vector2f(xTexSize, yTexSize);
		texCoords[3] = Vector2f(0.0f, yTexSize);
		
		menuBackground.DrawBackground(this->owner->GetWindow(), texCoords);
	}

	else{
		for (size_t i = 0; i < layers.size(); i++)
		{
			layers[i].sprite.DrawBackground(owner->GetWindow(), layers[i].texCoords);
		}
	}
}