#include <Rcpp.h>
#include <iostream>
#include <string>

using namespace Rcpp;
using namespace std;

// [[Rcpp::export]]
CharacterVector cpaste(const CharacterMatrix& body)
{
	CharacterVector v(body.nrow());

	for(int i=0; i<body.nrow(); i++)
	{
		string res;
		res.reserve(256);

		for(int j=0; j<body.ncol(); j++)
		{
			if(!CharacterMatrix::is_na(body(i,j)))
			{
				if(!res.empty())
					res.append(",");
				res.append(body(i,j));
			}
		}
		if(!res.empty())
			v[i] = res;
		else
			v[i] = NA_STRING;
	}

	return v;
}

// Coding on a friday night:

// [[Rcpp::export]]
CharacterVector v2paste(const CharacterVector& v1, const CharacterVector& v2, const string& sep)
{
	CharacterVector v(v1.size());

	for(int i=0; i<v1.size(); i++)
	{
		if( !CharacterVector::is_na(v1[i]) and
			!CharacterVector::is_na(v2[i]))
		{
			string res;
			res.reserve(v1[i].size()+v2[i].size()+1);
			res.append(v1[i]);
			res.append(sep);
			res.append(v2[i]);
			v[i] = res;
		}
		else
			v[i] = NA_STRING;
	}

	return v;
}

// [[Rcpp::export]]
CharacterVector v3paste(const CharacterVector& v1, const CharacterVector& v2, const CharacterVector& v3,
		const string& sep)
{
	CharacterVector v(v1.size());

	for(int i=0; i<v1.size(); i++)
	{
		if( !CharacterVector::is_na(v1[i]) and
			!CharacterVector::is_na(v2[i]) and
			!CharacterVector::is_na(v3[i]))
		{
			string res;
			res.reserve(v1[i].size()+v2[i].size()+v3[i].size()+2);
			res.append(v1[i]);
			res.append(sep);
			res.append(v2[i]);
			res.append(sep);
			res.append(v3[i]);
			v[i] = res;
		}
		else
			v[i] = NA_STRING;
	}

	return v;
}

// [[Rcpp::export]]
CharacterVector fullpaste(const string& header, const CharacterVector& v1, const string& sep)
{
	CharacterVector v(v1.size());

	for(int i=0; i<v1.size(); i++)
	{
		if( !CharacterVector::is_na(v1[i]))
		{
			string res;
			res.reserve(header.size()+v1[i].size()+1);
			res.append(header);
			res.append(sep);
			res.append(v1[i]);
			v[i] = res;
		}
		else
			v[i] = NA_STRING;
	}

	return v;
}
