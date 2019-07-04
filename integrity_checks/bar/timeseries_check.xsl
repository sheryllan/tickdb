<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" indent="yes"/>
	<xsl:key use="concat(generate-id(parent::bar), '|', local-name(.))" match="bar/child::*" name ="error_type"/>
	<xsl:variable name="vLower" select="'abcdefghijklmnopqrstuvwxyz'"/>
	<xsl:variable name="vUpper" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'"/>
	
	<xsl:template match="report">
		<html>
			<xsl:attribute name="status">
				<xsl:if test="*">0</xsl:if>
				<xsl:if test="not(*)">1</xsl:if>
			</xsl:attribute>
			<head>
				<style media="screen" type="text/css">
					div.container { margin: 5px; }
					table { 
					text-align:center; border-collapse: collapse; margin: 25px 0;
					border-left-style: hidden; border-right-style: hidden; width: 100%; 
					font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif; }
					
					table th, td { color: #000000; font-size: small; text-align:center; padding: 0; border: 2px solid #f2f2f2;}
					table caption { text-align: left; white-space: nowrap; overflow: hidden; }   
					
					th.lv1 { background-color: #888888; padding:3px 7px; }
					th.lv2 { background-color: #d9d9d9; padding:3px 5px; }
					th.lv3 { padding: 2px 5px; }
					
					table.content { border-collapse: collapse; border-style: hidden; margin: 0; width:100%; }
					table.content td { padding: 2px 5px; }
					
					tr:nth-child(odd) { background-color: #DDDDFF; }
					tr:nth-child(even) { background-color: #FFFFFF; }	
					
					.stressed { font-weight:bold; }		
				</style>
			</head>
			
			<body>
				<div class="container">
					<div>
						<h3>Time Series Integrity Check</h3>
						<xsl:for-each select="@*">
							<div><xsl:value-of select="concat(translate(substring(local-name(.), 1, 1), $vLower, $vUpper), substring(local-name(.), 2))" />: <xsl:value-of select="." /></div>
						</xsl:for-each>
					</div>
					<xsl:apply-templates select="invalid_files" mode="invalid_files" />
					<xsl:apply-templates select="record" mode="record" />
				</div>
			</body>			
		</html>

	</xsl:template>
	
	
	<xsl:template match="invalid_files" mode="invalid_files">
		<table>
			<caption>Invalid Files</caption>
			<thead>
				<tr>
					<xsl:for-each select="*[1]">
						<xsl:for-each select="@*">
							<th class="lv1">
								<xsl:value-of select="concat(translate(substring(local-name(.), 1, 1), $vLower, $vUpper), substring(local-name(.), 2))" />
							</th>
						</xsl:for-each>
					</xsl:for-each>
				</tr>
			</thead>
			<tbody>
				<xsl:for-each select="*">
					<tr>
						<xsl:for-each select="@*">
							<td><xsl:value-of select="."/></td>				
						</xsl:for-each>
					</tr>
				</xsl:for-each>
			</tbody>
		</table>
	</xsl:template>
	
	
	<xsl:template match="record" mode="record">
		<table> 
			<caption>
				Date(<xsl:value-of select="@timezone"/>): <xsl:value-of select="@date"/>
			</caption>
			<thead>
				<xsl:apply-templates select="bar[1]" mode="header" />
			</thead>
			<tbody>
				<xsl:apply-templates select="bar" mode="bar" />
			</tbody>
		</table>
	</xsl:template>
	
	
	<xsl:template match="bar" mode="header">
		<tr>
			<th id="Bar" class="span lv1" colspan="{count(@*[name()!='id'])}" scope="colgroup">Bar</th>
			<th id="Error" class="span lv1" colspan="2" scope="colgroup">Error</th>
		</tr>
		<tr>
			<xsl:for-each select="@*[name()!='id']">
				<th id="{local-name(.)}" class="lv2" scope="col">
					<xsl:value-of select="local-name(.)"/>
				</th>
			</xsl:for-each>
			
			<th id="type" class="lv2" scope="col">type</th>
			<th id="detail" class="lv2" scope="col">detail</th>
		</tr>
	</xsl:template>
	
	
	<xsl:template match="bar" mode="bar">
		<xsl:variable name="error_types" select="*[generate-id()=generate-id(key('error_type', concat(generate-id(current()), '|', local-name(.)))[1])]" />
		<tr>
			<xsl:for-each select="@*[name()!='id']">
				<th headers="Bar {local-name(.)}" class="lv3" rowspan="{count($error_types)}" scope="rowgroup">
					<xsl:value-of select="."/>
				</th>
			</xsl:for-each>
			<xsl:apply-templates select="$error_types[1]" mode="error" />
		</tr>
		<xsl:for-each select="$error_types[position() > 1]">
			<tr> <xsl:apply-templates select="." mode="error" /> </tr>
		</xsl:for-each>	
	</xsl:template>
	
	
	<xsl:template match="bar/child::*" mode="error">
		<th headers="Error type" class="lv3" > 
			<xsl:value-of select="local-name(.)"/> 
		</th>
		
		<td>
			<table class="content">
				<xsl:for-each select="key('error_type', concat(generate-id(parent::bar), '|', local-name(.)))">					
					<tr>
						<xsl:choose>
							<xsl:when test="*">
								<xsl:for-each select="*">
									<td headers="Error detail">
										<div><span class="stressed"><xsl:value-of select="local-name(.)"/></span></div>
										<xsl:for-each select="@*">
											<div><span class="stressed"><xsl:value-of select="local-name(.)"/>:</span>&#160;<xsl:value-of select="."/></div>
										</xsl:for-each>
									</td>
								</xsl:for-each>	
							</xsl:when>
							<xsl:otherwise>
								<xsl:for-each select="@*">
									<td headers="Error detail">
										<span class="stressed"><xsl:value-of select="local-name(.)"/>:</span>&#160;<xsl:value-of select="."/>
									</td>
								</xsl:for-each>	
							</xsl:otherwise>
						</xsl:choose>
					</tr>
				</xsl:for-each>
			</table>
		</td>
	</xsl:template>
</xsl:stylesheet>