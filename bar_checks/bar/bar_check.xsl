<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" indent="yes"/>
	<xsl:template match="report">
		<html>
			<head>
				<style media="screen" type="text/css">
					dl dt { font-weight: bold; padding: 7px 0;}
					dl dd { display:inline-block; }
					dl dd:before { content: '\00a0\2022\00a0\00a0'; color:#999;	color:rgba(0,0,0,0.5); font-size:11px; }
					
					table { border-collapse:collapse; text-align:center; font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif}
					table th, td { padding:3px 7px 2px; font-size: small; border: 2px solid #f2f2f2; }
					table caption { text-align: left; }
					
					th.lv1 { color: #000000; background-color: #888888; text-align:center; }
					th.lv2 { color: #000000; background-color: #d9d9d9; text-align:left; }
					
					th.lv1 { background-color: #888888; padding:3px 7px; }
					th.lv2 { background-color: #d9d9d9; padding:2px 5px; }
					
					td.good { color: #00AA00; font-weight: bold; font-size: small; }
					td.bad { color: #AA0000; font-weight: bold; font-size: small; }
					td.warning { color: #ff790d; font-weight: bold; font-size: small; }
				</style>
			</head>
			
			<body>
				<xsl:apply-templates select="missing_products" />
				<table> 
					<caption>
						Time: <xsl:value-of select="@start" /> to <xsl:value-of select="@end"/>
					</caption>
					<thead>
						<tr>
							<xsl:apply-templates select="bar[1]/record[1]" mode="header" />
						</tr>
					</thead>
					<tbody>
						<xsl:apply-templates select="bar" mode="bar">
							<xsl:with-param name="colspan" select="count(bar[1]/record[1]/@*)"/>
						</xsl:apply-templates>
					</tbody>
				</table>
			</body>
			
		</html>
	</xsl:template>
	
	<xsl:template match="missing_products">
		<dl>
			<dt>Missing Products</dt>
			<xsl:for-each select="product">
				<dd>
					<xsl:value-of select="text()" />
				</dd>
			</xsl:for-each>
		</dl>
	</xsl:template>
			
	<xsl:template match="record" mode="record">
		<xsl:variable name="barid" select="generate-id(parent::*)" />
		<td headers="{$barid} {local-name(.)}">
			<xsl:value-of select="@time"/>
		</td>
		<xsl:for-each select="@*">
			<xsl:if test="name() != 'time'">
				<xsl:choose>
					<xsl:when test=".=''">
						<td class="good" headers="{$barid} {local-name(.)}">
							Passed
						</td>
					</xsl:when>
					<xsl:otherwise>
						<td class="bad" headers="{$barid} {local-name(.)}">
							<details>
								<summary>Failed</summary>
								<pre>
									<xsl:value-of select="." />
								</pre>
							</details>
						</td>
					</xsl:otherwise>
				</xsl:choose>
			</xsl:if>
		</xsl:for-each>
		
	</xsl:template>
	
	<xsl:template match="bar" mode="bar">
		<xsl:param name="colspan" />  
		<tr>
			<th id="{generate-id(.)}" class="span lv2" colspan="{$colspan}" scope="colgroup">
				<xsl:variable name="bartype">
					<xsl:for-each select="@*[name()!='id']">
						<xsl:if test="position() > 1">
							<xsl:text>, </xsl:text>
						</xsl:if>
						<xsl:value-of select="concat(local-name(.), '=', .)" />
					</xsl:for-each>
				</xsl:variable>
				Bar: <xsl:value-of select="$bartype"/>
			</th>
		</tr>
		<xsl:for-each select="record">
			<tr>
				<xsl:apply-templates select="." mode="record"/>
			</tr>
		</xsl:for-each>
	</xsl:template>
	
	<xsl:template match="record" mode="header">
		<xsl:for-each select="@*">            
			 <th class="lv1" id="{local-name(.)}" scope="col">
				 <xsl:value-of select="local-name(.)"/>
			</th>
		</xsl:for-each>
	</xsl:template>
		
</xsl:stylesheet>
