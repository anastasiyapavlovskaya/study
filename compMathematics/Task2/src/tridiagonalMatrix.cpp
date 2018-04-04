#include"tridiagonalMatrix.hpp"

const float pi = 3.1415936f;

void computeCoeffP(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* p)
{
	p->at(0) = 0;
	for(int k = 0; k < p->size() - 1; k++)
	{
		p->at(k + 1) = -c->at(k) / (a->at(k) * p->at(k) + b->at(k));
	}
}

void computeCoeffQ(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* q, std::vector<float>* p, std::vector<float>* f)
{
	q->at(0) = 0;
	for (int k = 0; k < q->size() - 1; k++)
	{
		q->at(k + 1) = (f->at(k) - a->at(k) * q->at(k)) / (a->at(k) * p->at(k) + b->at(k));
	}
}

void tridiagonalMatrixAlgorithm(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* x, std::vector<float>* f)
{
	int equationCount = x->size();
	std::vector<float> p(equationCount + 1);
	std::vector<float> q(equationCount + 1);
	p[0] = 0;
	q[0] = 0;

	computeCoeffP(a, b, c, &p);
	computeCoeffQ(a, b, c, &q, &p, f);

	x->at(equationCount - 1) = q[equationCount];
	for (int k = equationCount - 1; k > 0; k--)
	{
		x->at(k - 1) = p[k] * x->at(k) + q[k];
	}

}

void solvingSistem(std::vector<float>* x)
{
	int equationCount = x->size();
	float h = pi / (float)x->size();

	std::vector<float> a(equationCount);
	std::vector<float> b(equationCount);
	std::vector<float> c(equationCount);
	std::vector<float> f(equationCount);
	
	a[0] = 0.0f;
	b[0] = 1.0f;
	c[0] = 0.0f;
	f[0] = 0.0f;

	a[equationCount - 1] = 0.0f;
	b[equationCount - 1] = 1.0f;
	c[equationCount - 1] = 0.0f;
	f[equationCount - 1] = 0.0f;

	for (int k = 1; k < equationCount - 1; k++)
	{
		a[k] = -1.0f;
		b[k] = 2.0f + pow(h, 2.0f);
		c[k] = -1.0f;
		f[k] = 2.0f * pow(h, 2.0f) * sin(k * h);
	}

	tridiagonalMatrixAlgorithm(&a, &b, &c, x, &f);

}