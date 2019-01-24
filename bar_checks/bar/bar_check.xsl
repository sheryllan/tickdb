<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" indent="yes"/>
	<xsl:variable name="vLower" select="'abcdefghijklmnopqrstuvwxyz'"/>
	<xsl:variable name="vUpper" select="'ABCDEFGHIJKLMNOPQRSTUVWXYZ'"/>
	
	<xsl:template match="report">
		<html>
			<head>
				<style media="screen" type="text/css">
					dl dt { font-weight: bold; padding: 7px 0;}
					dl dd { display:inline-block; }
					dl dd:before { content: '\00a0\2022\00a0\00a0'; color:#999;	color:rgba(0,0,0,0.5); font-size:11px; }
					
					table { border-collapse:collapse; margin: 25px 0; text-align:center; 
					font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif}
					table th, td { padding:3px 7px 2px; font-size: small; border: 2px solid #f2f2f2; }
					table caption { text-align: left; white-space: nowrap; overflow: hidden; }
					
					th.lv1 { color: #000000; background-color: #888888; text-align:center; }
					th.lv2 { color: #000000; background-color: #d9d9d9; text-align:left; }
					
					th.lv1 { background-color: #888888; padding:3px 7px; }
					th.lv2 { background-color: #d9d9d9; padding:2px 5px; }				
					
					td.good { color: #00AA00; font-weight: bold; font-size: small; }
					td.bad { color: #AA0000; font-weight: bold; font-size: small; }
					
					pre.detail { font-weight: normal; }
					pre.warning { color: #cccc00; font-weight: normal; }
				</style>
			</head>
			
			<body>
				<h3>Bar Integrity Check</h3>
				<xsl:for-each select="@*">
					<div><xsl:value-of select="concat(translate(substring(local-name(.), 1, 1), $vLower, $vUpper), substring(local-name(.), 2))" />: <xsl:value-of select="." /></div>
				</xsl:for-each>
				<table> 
					<thead>
						<tr>
							<xsl:apply-templates select="bar[1]/record[1]" mode="header" />
						</tr>
					</thead>
					<tbody>
						<xsl:apply-templates select="bar" mode="bar">
							<xsl:with-param name="colspan" select="count(bar[1]/record[1]/*) + 1"/>
						</xsl:apply-templates>
					</tbody>
				</table>
			</body>
			
		</html>
	</xsl:template>	
	
	<xsl:template match="record" mode="record">
		<td>
			<xsl:value-of select="@time"/>
		</td>
		<xsl:for-each select="./*">
			<xsl:apply-templates select="." mode="record_child" />
		</xsl:for-each>
	</xsl:template>
	
	
	<xsl:template match="record/*" mode="record_child">
		<xsl:choose>
			<xsl:when test="@summary='passed'">
				<td class="good">
					<xsl:choose>
						<xsl:when test="@warning">
							<details>
								<summary>Passed</summary>
								<pre class="warning">
									<xsl:value-of select="@warning" />
								</pre>
							</details>
						</xsl:when>
						<xsl:otherwise>
							<xsl:text>Passed</xsl:text>
						</xsl:otherwise>
					</xsl:choose>
				</td>
			</xsl:when>
			<xsl:otherwise>
				<td class="bad">
					<details>
						<summary>Failed</summary>
						<pre class="detail">
							<xsl:value-of select="@detail" />
						</pre>
						<xsl:if test="@warning">
							<pre class="warning">
								<xsl:value-of select="@warning" />
							</pre>
						</xsl:if>
					</details>
				</td>
			</xsl:otherwise>
		</xsl:choose>
	</xsl:template>
	
	
	<xsl:template match="bar" mode="bar">
		<xsl:param name="colspan" />  
		<tr>
			<th id="{@id}" class="span lv2" colspan="{$colspan}" scope="colgroup">
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
		<th class="lv1" id="{local-name(@time)}" scope="col">
			 <xsl:value-of select="local-name(@time)"/>
		</th>
		<xsl:for-each select="./*">            
			 <th class="lv1" id="{local-name(.)}" scope="col">
				 <xsl:value-of select="local-name(.)"/>
			</th>
		</xsl:for-each>
	</xsl:template>
		
		
</xsl:stylesheet>
