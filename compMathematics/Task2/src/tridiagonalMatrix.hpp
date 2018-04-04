#include<math.h>
#include<vector>

void computeCoeffP(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* p);
void computeCoeffQ(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* q, std::vector<float>* p, std::vector<float>* f);
void tridiagonalMatrixAlgorithm(std::vector<float>* a, std::vector<float>* b, std::vector<float>* c, std::vector<float>* x, std::vector<float>* f);
void solvingSistem(std::vector<float>* x);
