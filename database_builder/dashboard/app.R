#!/usr/bin/env Rscript

suppressMessages(library(shiny))
suppressMessages(library(DT))
suppressMessages(library(plyr))
suppressMessages(library(data.table))
suppressMessages(library(readr))
suppressMessages(library(jsonlite))
suppressMessages(library(stringr))

dailydb <- data.frame()
last_update_time <- Sys.time()
config_filename <- global_cfg_fname

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

read_daily_db <- function(provider)
{
	# read json config
	config <- load_json_config(config_filename)

	# read daily db
	x <- read.csv(config[[provider]]$daily_db)

	# decompose filenames into products, types and dates
	p = ldply(str_split(basename(x$files),"[-.]"),
			  function(r) c(prod=r[[1]],type=r[[2]],exp=r[[3]],date=r[[4]]))

	# Put results in global variables
	dailydb <<- cbind(p,x)
	last_update_time <<- Sys.time()
}

select_table <- function(d,input)
{
	# select date range
	days = as.Date(d$date,"%Y%m%d")
	d <- d[ days>=input$status.date.range[1] & days<=input$status.date.range[2], ]

	# select type of products
	d <- d[ d$type %in% input$liquid_product_type , ]

	# select product
	if(!is.null(input$prod.list)) # specific products have been selected
	{
		d <- d[ d$prod %in% input$prod.list , ]
	}

	# select instrument
	if(!is.null(input$inst.list))
	{
		# transform instr list back to its components :-)
		inst = adply(str_split(input$inst.list,"[.]"), 1, function(l)
					 {data.frame(prod=l[3],type=l[2],exp=l[4]) }, .id=NULL)
		d <- d[d$prod %in% inst$prod &
			   d$type %in% inst$type &
			   d$exp  %in% inst$exp, ]
	}

	return(d)
}

server <- function(input, output, session)
{
	# Update the database every 30 minutes
	output$last_update <- renderText({
		read_daily_db(input$provider)
		invalidateLater(1800000,session)
		HTML(paste("<font color=\"#3090C7\">","Last DB update at ", last_update_time,"</font>"))
	})

	# Update product list widget
	output$status.prod <- renderUI({
		sumdb = ddply(dailydb,.(prod,type,exp), summarize, nb=length(prod))

		# restrict to type of products
		sumdb <- sumdb[ sumdb$type %in% input$liquid_product_type, ]

		# generate product list
		p = as.list(unique(sumdb$prod))
		selectInput("prod.list",label="Products",
					choices=p,
					multiple=T,selectize=F)
	})

	# Update instrument list widget
	output$status.inst <- renderUI({
		sumdb = ddply(dailydb,.(prod,type,exp), summarize, nb=length(prod))

		# restrict to type of products
		sumdb <- sumdb[ sumdb$type %in% input$liquid_product_type, ]

		# restrict instruments to selected products
		if(!is.null(input$prod.list))
			sumdb <- sumdb[ sumdb$prod %in% input$prod.list, ]

		# Render instruments list in Reactor's style
		inst = str_c("PROD.",sumdb$type,".",sumdb$prod,".",sumdb$exp)
		inst = as.list(unique(inst))
		selectInput("inst.list",label="Instruments",
					choices=inst,
					multiple=T,selectize=F)
	})

	output$graph.type.container <- renderUI({
		# remove _ from fields' names to make them pretty
		tags=grep("^has_|^is_|^file|^prod|^type|^exp|^date",names(dailydb),value=T,invert=T)
		names(tags)=as.list(str_replace_all(tags,"_"," "))

		# create selection list
		selectInput("graph.type",label="Graph Type",
			choices=tags, multiple=T,selectize=F,selected="nb_trades")
	})

	# Update summary text at the top
	output$status.summary <- renderText({
		# summarize DB
		sumdb = ddply(dailydb,.(prod,type,exp), summarize, nb=length(prod))
		sp = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
		paste(br(),
			  sp,sp,"Nb files in database:",nrow(dailydb),br(),
			  sp,sp,"Nb instruments:",nrow(sumdb),br(),
			  sp,sp,"Avg instruments per day:",mean(sumdb$nb),br(),
			  br(),br(),br())
	})

	# Update summary table in the middle
	output$status.details <- renderDataTable({
		# just reorganize columns for the presentation
		checknames = grep("^has_|^is_",names(dailydb),value=T)
		d <- dailydb[,c("prod","type","exp","date",checknames)]

		# do the selection of results
		d <- select_table(d,input)
		datatable(d,colnames=str_replace_all(names(d),"_"," "),filter="none")
	})

	output$stats.details <- renderDataTable({
		# just reorganize columns for the presentation
		statsnames = grep("^has_|^is_|^file|^prod|^type|^exp|^date",names(dailydb),value=T,invert=T)
		d <- dailydb[,c("prod","type","exp","date",statsnames)]

		# do the selection of results
		d <- select_table(d,input)
		datatable(d,colnames=str_replace_all(names(d),"_"," "))
	})

	output$stats.graph <- renderPlot({
		# just reorganize columns for the presentation
		statsnames = grep("^has_|^is_|^file|^prod|^type|^exp|^date",names(dailydb),value=T,invert=T)
		d <- dailydb[,c("prod","type","exp","date",statsnames)]

		# do the selection of results
		d <- select_table(d,input)
		mins <- sapply(d[,5:ncol(d)],function(x) min(x,na.rm=T)) # used to calibrate graphs
		maxs <- sapply(d[,5:ncol(d)],function(x) max(x,na.rm=T))
		d$date <- as.Date(d$date,"%Y%m%d")

		# Get list of graph types
		plot(1,type='n',axes=F,xlab='dates',ylab='', xlim=c(min(d$date), max(d$date)),yaxt='n')
		axis.Date(1, d$date)
		legd = data.frame(text=character(0),lty=integer(0),col=integer(0))
		if(!is.null(input$graph.type))
		{
			# Plot each type of graph successively
			color <- 2
			yaxis <- -1
			ltype <- 1
			for(typ in input$graph.type)
			{
				par(new=T)
				# draw frame for the graph
				plot(1,type='n',axes=F,xlab='',ylab='',
					 xlim=c(min(d$date),max(d$date)),ylim=c(mins[typ],maxs[typ]))
				axis(at=pretty(d[,typ]),side=2,col=color,line=yaxis)
				# extract data and plot for each instrument
				d_ply(d, .(prod,type,exp),
					function(x)
					{
						lines(x$date, x[,typ], col=color, lty=ltype)
						ltype<<-ltype+1
						legd <<- rbind.fill(legd,
							data.frame(text=str_c(str_replace_all(typ,"_"," "),
									  " ","PROD.",x$type[1],".",x$prod[1],".",x$exp[1]),
									   lty=ltype, col=color))
					})
				color <- color + 1
				yaxis <- yaxis+1
			}
			par(new=T)
			plot(1,type='n',axes=F,xlab='',ylab='',xlim=c(0,1),ylim=c(0,1))
			legend(0,1,legd$text, lty=legd$lty, col=legd$col,bty='n')
		}
	})
}

ui <- fluidPage(
		theme="sandstone.bootstrap.min.css",
		sidebarLayout
		(
			sidebarPanel
			(
				width="3",
			 	# Logo
				img(src="liquid-logo.png",width="75%"),
				br(),
				HTML("<strong><h3 align=\"center\">Tick Database Explorer</h3></strong>"),
				h5("version 0.1",align="center"),
				h6(htmlOutput("last_update",align="center")),
				br(),br(),
			 	# These buttons are always visible
			 	radioButtons("provider","Providers",
					list("Liquid"="liquid_capture","QTG"="qtg"),
					selected="liquid_capture"),
				sliderInput("status.date.range","Date Range",
					min=as.Date("2014-01-01"),max=as.Date("2016-04-12"),
					value=c(as.Date("2014-01-01"),as.Date("2016-04-12")),
					step=1),
				checkboxGroupInput("liquid_product_type","Type of Product",
					list("Options"="O","Futures"="F"),selected=c("F","O")),
				uiOutput("status.prod"),
				uiOutput("status.inst"),
				conditionalPanel(
					condition="input.tabpanel==\"Statistics\"",
					uiOutput("graph.type.container"))
			),
			mainPanel
			(
				tabsetPanel
				(
				 	id="tabpanel",
					# Status of the database (valid days, daily check, ...)
					tabPanel("Status",
						htmlOutput("status.summary"),
						dataTableOutput("status.details")),
					# Statistics on the (valid) data for research purpose
					tabPanel("Statistics",
						plotOutput("stats.graph"),
						dataTableOutput("stats.details"))
					# Time series and other graphs of products
#					tabPanel("Graphs",
#						dataTableOutput("dist.plot"))
				)
			)
		)
	)

shinyApp(ui=ui,server=server)
