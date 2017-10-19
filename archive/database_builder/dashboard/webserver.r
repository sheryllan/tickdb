#!/usr/bin/env Rscript

suppressMessages(library(shiny))
suppressMessages(library(jsonlite))

load_json_config <- function(config_file)
{
	if(file.exists(config_file))
	{
		file <- file(config_file,'rb')
		if(isOpen(file))
		{
			config <- fromJSON(file)
			close(file)
			return(config)
		}
	}

	return(NULL)
}

if(!interactive())
{
	args <- commandArgs(trailingOnly=T)
	if(length(args) == 1)
	{
		config <- load_json_config(args[1])
		global_cfg_fname <<- args[1] # little hack to pass this value to runApp ;-)
		runApp(config$shinyappdir,port=8002,host="0.0.0.0",launch.browser=F)
		quit(status=0)
	}
	else {
		printf("Usage: webserver.r <json config>\n")
		quit(status=1)
	}
}
