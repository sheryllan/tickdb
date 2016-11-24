#include <cassert>
#include <numeric>
#include <algorithm>
#include <Mixer.h>

std::tuple<std::vector<size_t>, std::vector<size_t>, Mixer::time_vector> 
Mixer::mix(const std::vector<time_vector *>& time)
{
	using namespace std;

	size_t N = accumulate(begin(time),end(time),0,[](int x,time_vector* t){return x+t->array().size();});
	vector<size_t> uidi(N);
	vector<size_t> idxi(N);
	time_vector T(N);
	
	for(size_t I=0,n=0; n<time.size(); n++)
		for(size_t t=0; t<time[n]->size(); t++)
		{
			idxi[I] = t;
			uidi[I]=n;
			T[I++]=time[n]->array()[t];
		}

	vector<size_t> idx(N);
	iota(begin(idx),end(idx),0);
	sort(begin(idx),end(idx),[&T](size_t i1, size_t i2){return T[i1]<T[i2];});

	vector<size_t> ret_uidi(N), ret_idxi(N);
	time_vector ret_T(N);
	for(size_t i=0; i<N; i++)
	{
		ret_uidi[i] = uidi[idx[i]];
		ret_idxi[i] = idxi[idx[i]];
		ret_T[i] = T[idx[i]];
	}

	return std::make_tuple(ret_uidi,ret_idxi,ret_T);
}
