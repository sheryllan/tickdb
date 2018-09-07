<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html" indent="yes"/>
    <xsl:template match="report">
		<head>
            <style media='screen' type='text/css'>
                table { border-collapse:collapse; text-align:center; font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif}

                table th, td { border:1px padding:3px 7px 2px; font-size: small; }

                th { color: #000000;
                background-color: #888888; text-align:center; }

                tr:nth-child(odd) { background-color: #DDDDFF; }
                tr:nth-child(even) { background-color: #FFFFFF; }

                td.good { color: #00AA00; font-weight: bold; font-size: small; }
                td.bad { color: #AA0000; font-weight: bold; font-size: small; }
                td.warning { color: #ff790d; font-weight: bold; font-size: small; }
            </style>
        </head>
        <h2>Time: <xsl:value-of select="@start_date" /> to <xsl:value-of select="@end_date"/></h2>
        <table> 
			<caption>Bar Integrity Check Report</caption>
			<thead>
				<tr>
					<xsl:apply-templates select="bar/record[1]" mode="header" />
				</tr>
			</thead>
			<tbody>
				<tr>
					<xsl:apply-templates select="bar" mode="bar"/>	
				</tr>
				<tr>
					<th headers="{generate-id(bar)}" id="{generate-id(.)}">
						<xsl:value-of select="@time"/>
					</th>
				</tr>
				
				
			</tbody>
        
        </table>
		
	</xsl:template>
	
	<!--<xsl:template match="bar" mode="bar_th_id">
		<xsl:copy>
			<xsl:attribute name="id">
				<xsl:value-of select="generate-id(.)"/>
			</xsl:attribute>
		</xsl:>
	</xsl:template>-->
	
	<xsl:template match="bar" mode="bar">
		<th id="{generate-id(.)}" class="span" colspan="0" scope="colgroup">
			Bar: <xsl:for-each select="@*"><xsl:value-of select="local-name(.)"/> : <xsl:value-of select="."/></xsl:for-each>
		</th>
	</xsl:template>
	
	<xsl:template match="record" mode="header">
        <xsl:for-each select="@*">            
             <th>
				 <xsl:attribute name="id">
					 <xsl:value-of select="local-name(.)"/>
				 </xsl:attribute>
				 <xsl:value-of select="local-name(.)"/>
			</th>
        </xsl:for-each>
    </xsl:template>
		
</xsl:stylesheet>
