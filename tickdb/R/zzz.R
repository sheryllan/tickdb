.onLoad <- function(lib,pkg)
{
	options(readr.show_progress=F)
	doParallel::registerDoParallel(parallel::detectCores())
}
