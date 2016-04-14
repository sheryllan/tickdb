library(shiny)

server <- function(input, output)
{
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
					h4("Tick Database Viewer",align="center"),
					br(),
				 	# These buttons are always visible
				 	radioButtons("provider","Providers",
						list("Liquid"="liquid_capture","QTG"="qtg"),
						selected="Liquid"),
					# Display parameters UI depending on the selected tab
					conditionalPanel(
						condition="input.tabpanel == 'Status'",
						sliderInput("status.date.range","Date Range",
								min=as.Date("2014-01-01"),max=as.Date("2016-04-12"),
								value=c(as.Date("2014-01-01"),as.Date("2016-04-12")),
								step=1),
						selectInput("status.market",label="Markets",
								choices=list(1,2,3,4,5),
								selected=1,
								multiple=T,selectize=F),
						selectInput("status.products",label="Products",
								choices=list(1,2),
								selected=1,
								multiple=T,selectize=F),
						selectInput("status.instruments",label="Instruments",
								choices=list(1,2),
								selected=1,
								multiple=T,selectize=F)
						),
					conditionalPanel(
						condition="input.tabpanel == 'Statistics'",
						h1("stats")),
					conditionalPanel(
						condition="input.tabpanel == 'Graphs'",
						h1("graphs"))
				),
				mainPanel
				(
					tabsetPanel
					(
					 	id="tabpanel",
						# Status of the database (valid days, daily check, ...)
						tabPanel("Status",
							textOutput("status.summary"),
							tableOutput("status.details")),
						# Statistics on the (valid) data for research purpose
						tabPanel("Statistics",tableOutput("stats")),
						# Time series and other graphs of products
						tabPanel("Graphs", tableOutput("dist.plot"))
					)
				)
			)
		)

shinyApp(ui=ui,server=server)
