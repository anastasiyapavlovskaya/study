#include "Vector2f.hpp"


Vector2f::Vector2f()
{
  this->x = 0;
  this->y = 0;
}

Vector2f::Vector2f(float x, float y)
{
  this->x = x;
  this->y = y;
}

Vector2f::Vector2f(float ang)
{
  this->x = cosf(ang);
  this->y = sinf(ang);
}

Vector2f Vector2f::operator *=(float d)
{
  this->x *= d;
  this->y *= d;
  return *this;
}

Vector2f Vector2f::operator +=(Vector2f right)
{
  this->x += right.x;
  this->y += right.y;
  return *this;
}

Vector2f Vector2f::operator -=(Vector2f right)
{
  this->x -= right.x;
  this->y -= right.y;
  return *this;
}

float Vector2f::SquareLength()
{
  return x * x + y * y;
}

float Vector2f::GetLength()
{
  return std::sqrt(SquareLength());
}

Vector2f Vector2f::GetPerpendicular()
{
  return Vector2f(-y, x);
}

void Vector2f::Print()
{
  std::cout << "x: " << this->x << "y: " << this->y;
}

Vector2f operator +(Vector2f left, Vector2f right)
{
  Vector2f result;

  result.x = left.x + right.x;
  result.y = left.y + right.y;

  return result;
}

Vector2f operator -(Vector2f left, Vector2f right)
{
  Vector2f result;

  result.x = left.x - right.x;
  result.y = left.y - right.y;

  return result;
}

Vector2f operator *(Vector2f v, float d)
{
  Vector2f result;

  result.x = v.x * d;
  result.y = v.y * d;

  return result;
}

Vector2f operator *(float d, Vector2f v)
{
  return v * d;
}

float operator ^(Vector2f v0, Vector2f v1)
{
  return v0.x * v1.y - v0.y * v1.x;
}


float operator *(Vector2f v1, Vector2f v2)
{
  return v1.x * v2.x + v1.y * v2.y;
}

Vector2f Vector2f::GetNorma()
{
  return (*this) * (1.0f / GetLength());
}
