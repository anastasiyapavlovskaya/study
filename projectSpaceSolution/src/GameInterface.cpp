#include "GameInterface.hpp"
#include "Game.hpp"
#include <string>


GameInterface::GameInterface(Game* owner)
{
	this->owner = owner;
	this->guiWindow = sfg::Window::Create();
	this->guiWindow->SetStyle('b');
	this->SetProperties();
	this->progressBar = sfg::ProgressBar::Create();
	
	float xWindowSize = owner->GetWindow()->getSize().x;
	this->progressBar->SetRequisition(sf::Vector2f(xWindowSize * 0.2f, 25.0f));
	this->progressBar->SetPosition(sf::Vector2f(xWindowSize * 0.5, xWindowSize * 0.5));
	
	this->fuel.loadFromFile("Data/fuel.png");
	
	sfg::Image::Ptr image = sfg::Image::Create();
	image->SetImage(fuel);
//	image->Set

	sfg::Box::Ptr box = sfg::Box::Create(sfg::Box::Orientation::HORIZONTAL, 5.0f);
	box->Pack(image);
	box->Pack(progressBar);

	guiWindow->Add(box);
	this->guiWindow->SetPosition(sf::Vector2f(xWindowSize * 0.74f, 10.0f));

/*	sf::Font* font = new sf::Font;
	font->loadFromFile("Data/Lato-Medium.ttf");
	this->timeSpeed.setColor(sf::Color(241, 196, 15));

	this->timeSpeed.setFont(*font);

	currTimeSpeed = 0;	
*/
}

void GameInterface::SetProperties()
{
	sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BarBorderWidth", 0.0f);
	sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BorderWidth", 0.0f);

	sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BarColor", sf::Color(39, 174, 96));
	sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BackgroundColor", sf::Color(44, 62, 80));
}

void GameInterface::Draw()
{
	/*
	int characterSize = 32;
	
	std::stringstream stream;
	stream << std::fixed << std::setprecision(2) << currTimeSpeed;
	std::string stringTimeSpeed = stream.str();

	this->timeSpeed.setString(stringTimeSpeed);
	this->timeSpeed.setCharacterSize(characterSize);

	Vector2f textSize = Vector2f(this->timeSpeed.getLocalBounds().width, this->timeSpeed.getLocalBounds().height);
	Vector2f timeTextPos = Vector2f(this->owner->GetWindow()->getSize().x - textSize.x - 10.0f, textSize.y - 20.0f);
	this->timeSpeed.setPosition(sf::Vector2f(timeTextPos.x, timeTextPos.y));
	
	this->owner->GetWindow()->draw(timeSpeed);
	*/

	this->guiWindow->Show(true);
	this->owner->GetSFGUI()->Display(*this->owner->GetWindow());
}

void GameInterface::Update(float dt)
{
	this->guiWindow->Update(dt);
	float currFuelFraction = this->owner->GetFuelFraction();

	if (currFuelFraction <= 0.2f)
	{
		sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BarColor", sf::Color(192, 57, 43));
	}
	if (0.2f < currFuelFraction && currFuelFraction < 0.4f)
	{
		sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BarColor", sf::Color(243, 156, 18));
	}
	if (0.4f <= currFuelFraction)
	{
		sfg::Context::Get().GetEngine().SetProperty("ProgressBar", "BarColor", sf::Color(39, 174, 96));
	}

	this->progressBar->SetFraction(currFuelFraction);
//	this->currTimeSpeed = std::trunc(this->owner->GetTimeWarp() * 100.0f) / 100.0f;	
}

sfg::Window::Ptr GameInterface::GetGuiWindow()
{
	return this->guiWindow;
}
