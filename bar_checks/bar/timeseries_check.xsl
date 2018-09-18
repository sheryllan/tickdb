<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" indent="yes"/>
	<xsl:key use="@id" match="bar" name ="bartype"/>
	<xsl:template match="report">
		<html>
			<head>
				<style media="screen" type="text/css">
					.flex-container { display: inline-flex; flex-direction: column; align-items: center; }
					table { 
					text-align:center; border-collapse: collapse; margin-bottom: 25px;
					border-left-style: hidden; border-right-style: hidden;
					font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif}
					
					table th, td { color: #000000; font-size: small; text-align:center; padding: 0; border: 2px solid #f2f2f2;}
					table caption { text-align: left; }
					
					table.content { border-collapse: collapse; border-style: hidden; margin: 0; }
					table.content td { padding: 1px 5px; }
					table.content th { padding: 1px 5px; background-color: #d9d9d9; }   
					
					th.lv1 { background-color: #888888; padding:3px 7px; }
					th.lv2 { background-color: #d9d9d9; padding:2px 5px; }
					th.lv3 { padding: 1px 5px}
					
					tr:nth-child(odd) { background-color: #DDDDFF; }
					tr:nth-child(even) { background-color: #FFFFFF; }
				</style>
			</head>
			
			<body>
				<div class="flex-container">
					<h3>Timeseries Integrity Check Report</h3>
					<xsl:apply-templates select="record" mode="record" />
				</div>
			</body>
			
		</html>

	</xsl:template>
	
	
	<xsl:template match="record" mode="record">
		<table> 
			<caption>
				Date(<xsl:value-of select="@timezone"/>): <xsl:value-of select="@date"/>
			</caption>
			<thead>
				<xsl:apply-templates select="child::*[1]" mode="header" />
			</thead>
			<tbody>
				<xsl:apply-templates select="." mode="bars" />
			</tbody>
			
		</table>
	</xsl:template>
	
	<xsl:template match="record/child::*" mode="header">
		<tr>
			<th id="Bar" class="span lv1" colspan="{count(.//bar[1]/@*[name()!='id'])}" scope="colgroup">Bar</th>
			<th id="Error" class="span lv1" colspan="2" scope="colgroup">Error</th>
		</tr>
		<tr>
			<xsl:for-each select=".//bar[1]/@*[name()!='id']">
				<th id="{local-name(.)}" class="lv2" scope="col">
					<xsl:value-of select="local-name(.)"/>
				</th>
			</xsl:for-each>
			
			<th id="type" class="lv2" scope="col">type</th>
			<th id="detail" class="lv2" scope="col">detail</th>
		</tr>
	</xsl:template>
	
	<xsl:template match="record" mode="bars">
		<xsl:for-each select=".//bar[generate-id()=generate-id(key('bartype', @id)[1])]">
			<xsl:apply-templates select="." mode="bar" />
		</xsl:for-each>
		<!--<xsl:for-each-group select=".//bar" group-by="@id">
			<xsl:apply-templates select="." mode="bar">
				<xsl:with-param name="bargroup" select="current-group()" />
			</xsl:apply-templates>
		</xsl:for-each-group>-->
	</xsl:template>
	
	<xsl:template match="bar" mode="bar">
		<xsl:variable name="barid" select="@id"/>
		<xsl:variable name="bargroup" select="key('bartype', @id)" />
		<tr>
			<xsl:for-each select="@*[name()!='id']">
				<th headers="Bar {local-name(.)}" class="lv3" rowspan="{count($bargroup)}" scope="rowgroup">
					<xsl:value-of select="."/>
				</th>
			</xsl:for-each>
			<xsl:apply-templates select="." mode="error" />
		</tr>
		<!--<xsl:for-each select="$bargroup[position() > 1]">-->
		<xsl:for-each select="$bargroup[position() > 1]">
			<tr>
				<xsl:apply-templates select="." mode="error" />
			</tr>
		</xsl:for-each>
	</xsl:template>
	
	<xsl:template match="bar" mode="error">
		<th headers="Error type" class="lv3" scope="row">
			<xsl:value-of select="local-name(parent::*)"/>
		</th>
		<td headers="Error detail">
			<xsl:apply-templates select="." mode="details" />
		</td>
	</xsl:template>
	
	<xsl:template match="bar" mode="details">
		<table class="content">
			<tr>
				<xsl:for-each select="detail[1]/@*">
					<th class="content">
						<xsl:value-of select="local-name(.)"/>
					</th>
				</xsl:for-each>
			</tr>
			<xsl:for-each select="detail">
				<tr>
					<xsl:for-each select="@*">
						<td>
							<xsl:value-of select="."/>
						</td>
					</xsl:for-each>
				</tr>
			</xsl:for-each>
		</table>		
	</xsl:template>
		
</xsl:stylesheet>
