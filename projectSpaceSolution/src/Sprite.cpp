#include"Sprite.hpp"

std::map<std::string, sf::Texture*> Sprite::textures;

Sprite::Sprite(std::string filename)
{
	if (Sprite::textures.find(filename) == textures.end())
	{
		Sprite::textures[filename] = new sf::Texture;
		Sprite::textures[filename]->loadFromFile(filename);
	}
	this->tex = textures[filename];
	//this->tex = new sf::Texture;
	//this->tex->loadFromFile(filename);
}

void Sprite::Draw(sf::RenderWindow *window, Vector2f pos, float ang, Vector2f size, Camera* camera)
{
	sf::Vertex vertices[4];
	Vector2f vertexPos;

	Vector2f xVector = camera->ConvertVectorToLocal(Vector2f(cosf(ang), -sinf(ang))).GetNorma();
	Vector2f yVector = xVector.GetPerpendicular();

	Vector2f windowSize = Vector2f((float)window->getSize().x, (float)window->getSize().y);

	Vector2f fov = Vector2f(0.0f, 0.0f);
	fov.x = camera->GetCurrFov();
	fov.y = fov.x * (windowSize.y / windowSize.x);

	Vector2f center = windowSize * 0.5f;

//	printf("pos %f %f camera %f %f\n", pos.x, pos.y, camera->GetCurrPos().x, camera->GetCurrPos().y);
	pos = camera->ConvertPosToLocal(pos);
	Vector2f screenPos;
	screenPos.x = (pos.x / fov.x) * windowSize.x;
	screenPos.y = (pos.y / fov.y) * windowSize.y;

	Vector2f screenSize;
	screenSize.x = (size.x * 0.5f / fov.x) * windowSize.x;
	screenSize.y = (size.y * 0.5f / fov.y) * windowSize.y;

//	std::cout << pos.x << " " << pos.y << std::endl;

	vertexPos = center + screenPos - xVector * screenSize.x - yVector * screenSize.y;
	vertices[0].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenPos + xVector * screenSize.x - yVector * screenSize.y;
	vertices[1].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenPos + xVector * screenSize.x + yVector * screenSize.y;
	vertices[2].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenPos - xVector * screenSize.x + yVector * screenSize.y;
	vertices[3].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	float xTexSize = (float)tex->getSize().x;
	float yTexSize = (float)tex->getSize().y;

	vertices[0].texCoords = sf::Vector2f(0.0f, 0.0f);
	vertices[1].texCoords = sf::Vector2f(xTexSize, 0.0f);
	vertices[2].texCoords = sf::Vector2f(xTexSize, yTexSize);
	vertices[3].texCoords = sf::Vector2f(0.0f, yTexSize);

	for (int i = 0; i < 4; i++)
	{
		vertices[i].color = sf::Color::White;
	}

	window->draw(vertices, 4, sf::Quads, this->tex);

}

void Sprite::DrawTrajectory(sf::RenderWindow *window, Vector2f firstPos, Vector2f secondPos, Camera* camera)
{
	sf::Vertex vertices[4];

	Vector2f xVector = camera->ConvertVectorToLocal(secondPos - firstPos).GetNorma();
	Vector2f yVector = xVector.GetPerpendicular();

	Vector2f windowSize = Vector2f((float)window->getSize().x, (float)window->getSize().y);

	Vector2f fov = Vector2f(0.0f, 0.0f);
	fov.x = camera->GetCurrFov();
	fov.y = fov.x * (windowSize.y / windowSize.x);

	Vector2f center = windowSize * 0.5f;

	firstPos = camera->ConvertPosToLocal(firstPos);
	secondPos = camera->ConvertPosToLocal(secondPos);

	Vector2f screenFirstPos;
	screenFirstPos.x = (firstPos.x / fov.x) * windowSize.x;
	screenFirstPos.y = (firstPos.y / fov.y) * windowSize.y;

	Vector2f screenSecondPos;
	screenSecondPos.x = (secondPos.x / fov.x) * windowSize.x;
	screenSecondPos.y = (secondPos.y / fov.y) * windowSize.y;

	float lineWidth = 2.0f;

	Vector2f vertexPos;
	vertexPos = center + screenFirstPos + lineWidth * yVector;
	vertices[0].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenFirstPos - lineWidth * yVector;
	vertices[1].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenSecondPos - lineWidth * yVector;
	vertices[2].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	vertexPos = center + screenSecondPos + lineWidth * yVector;
	vertices[3].position = sf::Vector2f(vertexPos.x, vertexPos.y);

	float xTexSize = (float)tex->getSize().x;
	float yTexSize = (float)tex->getSize().y;

	vertices[0].texCoords = sf::Vector2f(0.0f, 0.0f);
	vertices[1].texCoords = sf::Vector2f(xTexSize, 0.0f);
	vertices[2].texCoords = sf::Vector2f(xTexSize, yTexSize);
	vertices[3].texCoords = sf::Vector2f(0.0f, yTexSize);

	for (int i = 0; i < 4; i++)
	{
		vertices[i].color = sf::Color::White;
	}

	window->draw(vertices, 4, sf::Quads, this->tex);
}

void Sprite::DrawBackground(sf::RenderWindow * window, Vector2f * texCoords)
{
	sf::Vertex vertices[4];
	
	for (int i = 0; i < 4; i++)
	{
		vertices[i].color = sf::Color::White;
	}

	Vector2f windowSize = Vector2f((float)window->getSize().x, (float)window->getSize().y);

	vertices[0].position = sf::Vector2f(0.0f, 0.0f);
	vertices[1].position = sf::Vector2f(windowSize.x, 0.0f);
	vertices[2].position = sf::Vector2f(windowSize.x, windowSize.y);
	vertices[3].position = sf::Vector2f(0.0f, windowSize.y);

	for (int i = 0; i < 4; i++)
	{
		vertices[i].texCoords = sf::Vector2f(texCoords[i].x, texCoords[i].y);
	}

	window->draw(vertices, 4, sf::Quads, this->tex);

}

sf::Texture* Sprite::GetTexture()
{
	return this->tex;
}

void Sprite::SetRepeating()
{
	this->tex->setRepeated(true);
}