from lxml import etree
import pandas as pd


def df_to_xmletree(root_ele, mem_ele, df, index_name=None):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    xml = etree.Element(str(root_ele))
    for i, row in df.iterrows():
        fields = {k: str(v) for k, v in row.items()}
        if index_name is not None:
            fields.update({index_name: str(i)})
        subelement = etree.Element(mem_ele, **fields)
        xml.append(subelement)

    return xml


def to_xslstyle_xml(xml, xsl, stream):
    header = '<?xml version="1.0" encoding="utf-8"?>'
    xsl_ref = '<?xml-stylesheet type="text/xsl" href="{}"?>'.format(xsl)
    stream.write(header)
    stream.write(xsl_ref)
    stream.write(etree.tostring(xml).decode('ascii'))
    # etree.ElementTree(xml).write(stream)

