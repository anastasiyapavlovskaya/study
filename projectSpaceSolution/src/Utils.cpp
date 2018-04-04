#include "Utils.hpp"

void RenderQuad(sf::RenderWindow *window, sf::Vector2f center, float ang, sf::Vector2f size, sf::Shader *shader, Camera* camera)
{
  sf::Vertex vertices[4];

  Vector2f xVector = camera->ConvertVectorToLocal(Vector2f(cosf(ang), -sinf(ang))).GetNorma();
  Vector2f yVector = xVector.GetPerpendicular();

  Vector2f centerInCamera = camera->ConvertPosToLocal(Vector2f(center.x, center.y));
  Vector2f windowSize = Vector2f((float)window->getSize().x, (float)window->getSize().y);
  Vector2f windowCenter = windowSize * 0.5f;

  Vector2f fov = Vector2f(0.0f, 0.0f);
  fov.x = camera->GetCurrFov();
  fov.y = fov.x * (windowSize.y / windowSize.x);

  Vector2f centerInScreen;
  centerInScreen.x = (centerInCamera.x / fov.x) * windowSize.x;
  centerInScreen.y = (centerInCamera.y / fov.y) * windowSize.y;

  Vector2f screenSize;
  screenSize.x = (size.x * 0.5f / fov.x) * windowSize.x;
  screenSize.y = (size.y * 0.5f / fov.y) * windowSize.y;

  vertices[0].texCoords.x = 0.0f;
  vertices[0].texCoords.y = 0.0f;
  Vector2f vertexPos = windowCenter + centerInScreen - xVector * screenSize.x - yVector * screenSize.y;
  vertices[0].position = sf::Vector2f(vertexPos.x, vertexPos.y);

  vertices[1].texCoords.x = 1.0f;
  vertices[1].texCoords.y = 0.0f;
  //vertices[1].position.x = windowCenter.x + centerInScreen.x + screenSize.x;
  //vertices[1].position.y = windowCenter.y + centerInScreen.y - screenSize.y;
  vertexPos = windowCenter + centerInScreen + xVector * screenSize.x - yVector * screenSize.y;
  vertices[1].position = sf::Vector2f(vertexPos.x, vertexPos.y);

  vertices[2].texCoords.x = 1.0f;
  vertices[2].texCoords.y = 1.0f;
  //vertices[2].position.x = windowCenter.x + centerInScreen.x + screenSize.x;
  //vertices[2].position.y = windowCenter.y + centerInScreen.y + screenSize.y;
  vertexPos = windowCenter + centerInScreen + xVector * screenSize.x + yVector * screenSize.y;
  vertices[2].position = sf::Vector2f(vertexPos.x, vertexPos.y);

  vertices[3].texCoords.x = 0.0f;
  vertices[3].texCoords.y = 1.0f;
  //vertices[3].position.x = windowCenter.x + centerInScreen.x - screenSize.x;
  //vertices[3].position.y = windowCenter.y + centerInScreen.y + screenSize.y;
  vertexPos = windowCenter + centerInScreen - xVector * screenSize.x + yVector * screenSize.y;
  vertices[3].position = sf::Vector2f(vertexPos.x, vertexPos.y);

  window->draw(vertices, 4, sf::Quads, shader);
}
