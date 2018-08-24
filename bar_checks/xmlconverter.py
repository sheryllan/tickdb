from lxml import etree
import pandas as pd
from collections import Iterable, Mapping


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


def dicts_to_xmletree(data, root_name, element_name):

    def rcsv_addto_etree(value, root):
        if isinstance(value, Mapping):
            for fk, fv in value.items():
                if isinstance(fv, Iterable):
                    subelement = etree.Element(fk)
                    root.append(rcsv_addto_etree(fv, subelement))
                else:
                    root.set(fk, fv)

        elif isinstance(value, Iterable):
            for val in value:
                root.append(rcsv_addto_etree(val, root))
        else:
            root.text = str(value)

        return root

    xml = etree.Element(root_name)
    for element in data:
        xml.append(rcsv_addto_etree(element, etree.Element(element_name)))

    return xml


def to_xslstyle_xml(xml, xsl, stream):
    header = '<?xml version="1.0" encoding="utf-8"?>'
    xsl_ref = '<?xml-stylesheet type="text/xsl" href="{}"?>'.format(xsl)
    stream.write(header)
    stream.write(xsl_ref)
    stream.write(etree.tostring(xml).decode('ascii'))
    # etree.ElementTree(xml).write(stream)

