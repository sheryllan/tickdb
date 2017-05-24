$0~/^[A-Za-z]/ {printf("\n%s",$0); next}

{
	if(NF>0)
	{
		if(NF>3) # concat when too many spaces in line
		{
			val=""
			for(i=3; i<=NF; i++)
				val +=$i
		}
		else 
			val=$3

		# remove brackets
		gsub("^\\[","",val)
		gsub("\\]$","",val)

		# Interpret value if needed
		switch($2)
		{
			case "(byteVector)":
				split(val,a,"|");
				val=sprintf("%d","0x" a[2]);
				break
			case "(decimal)":
				gsub("[{}]","",val);
				split(val,a,"|");
				val=a[2]*(10**a[1]);
				break
		}
		printf("%s:%s:",$1,val)
	}
}
