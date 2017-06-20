.onLoad <- function(lib,pkg)
{
	options(readr.show_progress=F)
	doMC::registerDoMC(24)
}
