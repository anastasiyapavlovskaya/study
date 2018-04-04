#include <iostream>
#include <cmath>
#include <algorithm>
#include "tinyXML2.h"
#include <stdio.h>


const int N = 1000;
const int maxNodeLenght = 20;

char nodes[N][maxNodeLenght];
int nodesCount = 0;
bool used[N];
int f[N][N];
int c[N][N];
int e[N];
int h[N];

int InitCapacity()
{
	for (int v = 0; v < N; v++)
	{
		for (int u = 0; u < v; u++)
		{
			c[u][v] = 0;
			c[v][u] = 0;
		}
	}

/*	c[0][1] = 16;
	c[0][2] = 13;
	c[1][3] = 12;
	c[2][1] = 4;
	c[2][4] = 14;
	c[3][2] = 9;
	c[3][5] = 20;
	c[4][3] = 7;
	c[4][5] = 4;

	c[0][1] = 3;
	c[0][2] = 3;
	c[1][3] = 3;
	c[1][2] = 2;
	c[2][4] = 2;
	c[3][4] = 4;
	c[3][5] = 2;
	c[4][5] = 3;
*/
	c[0][1] = 16;
	c[0][2] = 13;
	c[2][1] = 4;
	c[1][2] = 10;
	c[1][3] = 12;
	c[3][2] = 9;
	c[2][4] = 14;
	c[4][3] = 7;
	c[3][5] = 20;
	c[4][5] = 4;
	nodesCount = 6;
	return 0;
}

int Load(const char* pFilename)
{
	tinyxml2::XMLDocument doc;
	int res = doc.LoadFile(pFilename);
	if (tinyxml2::XML_NO_ERROR != doc.LoadFile(pFilename)) return 2;
	
	tinyxml2::XMLElement* pElem;
	tinyxml2::XMLElement* pRoot = doc.FirstChildElement("network");


	pElem = pRoot->FirstChildElement("node");
	for (pElem; pElem; pElem = pElem->NextSiblingElement("node"))
	{
		const char *pName = pElem->Attribute("id");
		if (pName) strcpy_s(nodes[nodesCount], maxNodeLenght, pName);
		nodesCount++;

	}

	pElem = pRoot->FirstChildElement("edge");
	for (pElem; pElem; pElem = pElem->NextSiblingElement("edge"))
	{
		const char *pNameV = pElem->Attribute("src_node");
		const char *pNameU = pElem->Attribute("dst_node");

		if (!pNameV || !pNameU)
			return 2;

		int v = 0;
		int u = 0;

		for (v = 0; v < nodesCount; v++)
		{
			if (0 == strcmp(nodes[v], pNameV))
				break;
		}
			

		for (u = 0; u < nodesCount; u++)
		{
			if (0 == strcmp(nodes[u], pNameU))
				break;
		}

		pElem->QueryIntAttribute("U", &c[v][u]);
		if (c[v][u] < 0)
			return 2;
	}
	return 0;
}

int Save(const char* pFilename, int maxFlow)
{
	tinyxml2::XMLDocument doc;
	
	tinyxml2::XMLDeclaration* decl = doc.NewDeclaration(NULL);
	doc.InsertEndChild(decl);

	tinyxml2::XMLElement* root = doc.NewElement("network");
	doc.InsertEndChild(root);

	//tinyxml2::XMLElement * root = doc.NewElement("result");
	//root->SetAttribute("maxFlow", maxFlow);

	for (int v = 0; v < nodesCount; v++)
	{
		for (int u = 0; u < nodesCount; u++)
		{
			if (f[v][u] > 0)
			{	
			tinyxml2::XMLElement* pElem = doc.NewElement("edge");
			pElem->SetAttribute("src_node", v);
			pElem->SetAttribute("dst_node", u);
			pElem->SetAttribute("flow", f[v][u]);
			root->InsertEndChild(pElem);
			}
		}
	}
	//doc.InsertEndChild(root);
	
	doc.SaveFile(pFilename);
	return 0;
}

void DFS(int v)
{
	used[v] = true;
	
	for (int i = 0; i < nodesCount; ++i) 
	{
		if (c[v][i] != 0)
		{
			int to = i;
			if (!used[to])
				DFS(to);
		}
	}

}

int checkInputGraph()
{
	for (int i = 0; i < nodesCount; i++)
	{
		used[i] = false;
	}

	DFS(0);
	if (used[nodesCount - 1] == false)
	{
		return 1;
	}
	return 0;
}

void InitializePreflow()
{
	for (int v = 0; v < nodesCount; v++)
	{
		h[v] = 0;
		e[v] = 0;
	}

	for (int v = 0; v < nodesCount; v++)
	{
		for (int u = 0; u < v; u++)
		{
			f[v][u] = 0;
			f[u][v] = 0;
		}
	}

	for (int v = 0; v < nodesCount; v++)
	{
		f[0][v] = c[0][v];
		f[v][0] = -c[0][v];
		e[v] = c[0][v];
		e[0] -= c[0][v];
	}

	h[0] = nodesCount;
}

void PrintGraf()
{
	for (int v = 0; v < nodesCount; v++)
	{
		for (int u = 0; u < nodesCount; u++)
		{
			std::cout << f[v][u] << " ";
		}
		std::cout << std::endl;
	}
}

void PrintHeight()
{
	std::cout << "Height Function:" << std::endl;
	for (int v = 0; v < nodesCount; v++)
	{
		std::cout << h[v] << " ";
	}
	std::cout << std::endl;
}

void PrintExcessFlow()
{
	std::cout << "Overflow Function:" << std::endl;
	for (int v = 0; v < nodesCount; v++)
	{
		std::cout << e[v] << " ";
	}
	std::cout << std::endl;
}

void PrintCapacityConstraints()
{
	std::cout << "Capacity Constraints: " << std::endl;
	for (int v = 0; v < nodesCount; v++)
	{
		for (int u = 0; u < nodesCount; u++)
		{
			std::cout << c[v][u] << "\t";
		}
		std::cout << std::endl;
	}
}

void Push(int u, int v)
{
	int x = std::min(c[u][v] - f[u][v], e[u]);
	f[u][v] += x;
	f[v][u] = -f[u][v];

	e[u] -= x;
	e[v] += x;
}

void Relabel(int u)
{
	int inf = INT_MAX;
	int min = inf;
	for (int v = 0; v < nodesCount; v++)
	{
		if (c[u][v] - f[u][v] > 0 && h[v] < min)
		{
			min = h[v];
		}
	}
	if (min == inf)
	{
		return;
	}

	h[u] = min + 1;
}

int calcMaxFlow()
{
	InitializePreflow();

	while (1)
	{
		int u = 0;
		for (u = 0; u < nodesCount; u++)
		{
			if (e[u] > 0)
				break;
		}
		if (u == nodesCount - 1)
			break;

		int v = 0;
		for (v = 0; v < nodesCount; v++)
		{
			if (c[u][v] - f[u][v] > 0 && h[u] > h[v])
				break;
		}

		if (v < nodesCount)
			Push(u, v);
		else
			Relabel(u);
	}

	int maxFlow = 0;
	for (int v = 0; v < nodesCount; v++)
	{
		if (c[0][v] > 0)
			maxFlow += f[0][v];
	}

	return maxFlow;
}

int main(int argc, char** argv)
{
	//InitCapacity();
	
	if (argc != 3)
	{
		//std::cout << "Error! Not enought arguments" << std::endl << "Usage: MaxFlowAlgorithm inputFile.xml outputFile.xml";
		return 2;
	}
	

	//char inputFilename[100];
	//std::cin >> inputFilename;
	//char outputFilename[100];
	//std::cin >> outputFilename;

	
	if (0 != Load(argv[1]))
	{
		//std::cout << "Error! Incorrect inputFile" << std::endl;
		return 2;
	}
	
	/*
	if (0 != Load(inputFilename))
	{
		//std::cout << "Error! Incorrect inputFile" << std::endl;
		return 2;
	}
	*/
	if (0 != checkInputGraph())
	{
		std::cout << "No flow exists" << std::endl;
		return 1;
	}

	//PrintCapacityConstraints();
	int maxFlow = calcMaxFlow();
	std::cout << "maxFlow = " << maxFlow << std::endl;

	Save(argv[2], maxFlow);
	//Save(outputFilename, maxFlow);
	return 0;
}